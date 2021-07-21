# Copyright 2020 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import AuthorizationException, RequestError
import pandas as pd
from datetime import datetime, timedelta
import re
import json

from spot.crawler.commons import sizeof_fmt, num_elements
from spot.utils.config import SpotConfig
from spot.utils.auth import auth_config
import spot.utils.setup_logger

logger = logging.getLogger(__name__)
REQUEST_TIMEOUT = 30


class Elastic:

    def __init__(self, conf):
        self._conf = conf
        connection = self._conf.elasticsearch_url
        logger.debug(f'Setting Elasticsearch: {connection}')
        http_auth = auth_config(self._conf)

        self._es = elasticsearch.Elasticsearch([connection],
                                               sniff_on_start=False,
                                               # Sniffing may change host,
                                               # in some cases new host address may not respond
                                               sniff_on_connection_fail=False,
                                               timeout=REQUEST_TIMEOUT,
                                               retry_on_timeout=True,
                                               http_auth=http_auth,
                                               connection_class=elasticsearch.RequestsHttpConnection)
        # Spark indexes
        self._raw_index = self._conf.elastic_raw_index
        self._agg_index = self._conf.elastic_agg_index
        self._err_index = self._conf.elastic_err_index

        # YARN indexes
        self._yarn_clust_index = self._conf.yarn_clust_index
        self._yarn_apps_index = self._conf.yarn_apps_index
        self._yarn_scheduler_index = self._conf.yarn_scheduler_index

        self._limit_of_fields_increment = self._conf.elasticsearch_limit_of_fields_increment

        logger.debug("Initializing elasticsearch, checking indexes")
        self.log_indexes_stats()

    def __do_request(self, request_func, *args, **kwargs):
        '''
        Passes an Elasticsearch function to be called, catching AuthorizationException and refreshing the
        '''
        try:
            return request_func(*args, **kwargs)
        except AuthorizationException as ae:
            logger.debug("AuthorizationException: {0}".format(ae))
            if (ae.status_code == 403) and (self._conf.auth_type == 'cognito'):
                logger.debug("Status code of {0} returned, token refresh required".format(ae.status_code))
                self._es.transport.connection_pool.connections[0].session.auth = auth_config(self._conf)
                logger.debug("Auth token refreshed")
            return request_func(*args, **kwargs)

    def _insert_item(self, index, uid, item):
        try:
            if uid:
                res = self.__do_request(self._es.index,
                                        index=index,
                                        op_type='index', # overwrites docs with existing ids
                                        id=uid,
                                        body=item,
                                        ignore=[],
                                        request_timeout=REQUEST_TIMEOUT)
            else:
                res = self.__do_request(self._es.index,
                                        index=index,
                                        op_type='create',
                                        body=item,
                                        ignore=[],
                                        request_timeout=REQUEST_TIMEOUT)

            op_result = res.get('result')
            doc_version = res.get('_version')
            logger.debug(f'uid: {uid} {op_result} as version {doc_version} in index {index}')
        except RequestError as req_err:
            err_type = req_err.info['error']['type']
            err_msg = req_err.info['error']['reason']
            # if error is due to "Limit of total fields"
            # increase the limit and retry
            if err_type == 'illegal_argument_exception' and err_msg.startswith('Limit of total fields'):
                item_fields = num_elements(item)
                substr = re.search("^(Limit of total fields \[)\d+(\] in index)", err_msg).group()
                current_limit_of_fileds = int(re.search("\d+",substr).group())
                new_limit_of_fields = max(item_fields, current_limit_of_fileds + self._limit_of_fields_increment)

                logger.warning(f'{err_msg}. Increasing the limit to: {new_limit_of_fields}')
                self.increase_limit_of_fields(index, new_limit_of_fields)
                self._insert_item(index, uid, item)
            else:  # unknown RequestError
                raise req_err

    def increase_limit_of_fields(self, index, new_limit):
        body = {"index.mapping.total_fields.limit": new_limit}
        res = self.__do_request(self._es.indices.put_settings,
                                index=index,
                                body=body,
                                preserve_existing=False,
                                request_timeout=REQUEST_TIMEOUT)

    def save_app(self, app):
        uid = app.get('id')
        self._insert_item(self._raw_index, uid, app)

    def save_agg(self, agg):
        app_id = agg.get('id')
        attempt_id = agg.get('attempt').get('attemptId', 0)
        uid = f'{app_id}-{attempt_id}'
        self._insert_item(self._agg_index, uid, agg)

    def save_err(self, app):
        self._insert_item(self._err_index, None, app)

    def get_latests_time_ids(self):
        id_set = set()
        if not self.__do_request(self._es.indices.exists, index=self._raw_index):
            return None, id_set

        # get max end_time
        body_max_end_time = {
            'size': 0,
            'aggs': {
                'max_endTime': {'max': {'field': 'attempts.endTime'}}
            }
        }

        res_time = self.__do_request(self._es.search,
                                    index = self._raw_index,
                                    body = body_max_end_time)
        # es uses epoch_millis internally
        timestamp = res_time['aggregations']['max_endTime']['value']
        if timestamp is None:
            return None, id_set

        # elastic search does not understand it's own internal time format in queries,
        # therefore using string
        str_max_end_time = res_time['aggregations']['max_endTime']['value_as_string']
        # get list of ids fot the same date
        body_id_list = {
            "stored_fields": [],
            "query": {
                "match": {
                    "attempts.endTime": str_max_end_time
                }
            }
        }

        res_ids = self.__do_request(self._es.search,
                                    index = self._raw_index,
                                    body = body_id_list)

        for hit in res_ids['hits']['hits']:
            id_set.add(hit['_id'])

        max_end_time = datetime.utcfromtimestamp(timestamp/1000.0)
        return max_end_time, id_set

    def get_processed_ids(self, end_time_min, end_time_max, size = 10000):
        """Query for id's of all apps stored in aggregations
        which completed from end_time_min to end_time_max.
        It is needed to compare against app id's from Spark History
        in order to skip runs processed in the previous iteration

        end_time_min -- minimum complition time of an app
        end_time_max -- maximum completion time of an app
        size -- max number of ids to request"""

        query_body = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "attempt.endTime": {
                                    "from": end_time_min,
                                    "to": end_time_max
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "attempt.endTime": {
                        "order": "desc"
                    }
                }
            ],
            "_source": ["id"],
            "size": size
        }

        res = self.__do_request(self._es.search,
                                index=self._agg_index,
                                body=query_body)
        hits = res.get('hits').get('hits')
        for item in hits:
            yield item.get("_source").get("id")

    def log_indexes_stats(self):
        for name, count, size_bytes in self.get_indexes_stats():
            logger.debug(f'index: {name} '
                         f'count:{count} '
                         f'size: {sizeof_fmt(size_bytes)}')

    # YARN RELATED
    def save_yarn_cluster_stats(self, clust_stats):
        self._insert_item(self._yarn_clust_index, None, clust_stats)

    def get_yarn_latest_finished_time(self):
        if not self.__do_request(self._es.indices.exists, index=self._yarn_apps_index):
            return None

        # get max end_time
        body_max_end_time = {
            'size': 0,
            'aggs': {
                'max_endTime': {'max': {'field': 'finishedTime'}}
            }
        }
        res_time = self.__do_request(self._es.search,
                                     index = self._yarn_apps_index,
                                     body = body_max_end_time)
        # es uses epoch_millis internally
        timestamp = res_time['aggregations']['max_endTime']['value']
        if timestamp is None:
            return None

        max_end_time = datetime.utcfromtimestamp(timestamp/1000.0)
        return max_end_time

    def _prepare_yarn_apps(self, apps):
        for app in apps:
            item = dict()
            item['_id'] = app.get('id')
            item['_index'] = self._yarn_apps_index
            item['_op_type'] = 'index'
            item['_type'] = '_doc'
            item['_source'] = app
            yield item

    def save_yarn_apps(self, apps):
        res = self.__do_request(bulk, self._es, self._prepare_yarn_apps(apps))

    def _prepare_yarn_scheduler_docs(self, docs):
        for doc in docs:
            item = dict()
            item['_index'] = self._yarn_scheduler_index
            item['_op_type'] = 'create'
            item['_type'] = '_doc'
            item['_source'] = doc
            yield item

    def save_yarn_scheduler_docs(self, docs):
        res = self.__do_request(bulk, self._es, self._prepare_yarn_scheduler_docs(docs))

    # STATS QUERIES

    def get_indexes_stats(self):
        indexes = [
            self._raw_index,
            self._agg_index,
            self._err_index
        ]
        for index in indexes:
            if self.__do_request(self._es.indices.exists, index=index):
                yield self._get_index_stats(index)
            else:
                logger.warning(f'index {index} does not exist')

    def _get_index_stats(self, index):
        res = self.__do_request(self._es.indices.stats, index=index)
        primaries = res['indices'][index]['primaries']
        docs_count = primaries['docs']['count']
        size_bytes = primaries['store']['size_in_bytes']
        return index, docs_count, size_bytes

    def get_top_tags(self, min_count=8):
        body = {
            "size": 0,
            "aggs": {
                "top_tags": {
                    "terms": {
                        "field": "app_specific_data.tag.keyword",
                        "size": 100500
                    },
                    "aggs": {
                        "my_filter": {
                            "bucket_selector": {
                                "buckets_path": {
                                    "the_doc_count": "_count"
                                },
                                "script": f'params.the_doc_count >= {min_count}'
                            }
                        }
                    }
                }
            }
        }
        res = self.__do_request(self._es.search,
                                index = self._raw_index,
                                body = body)
        logger.debug(res)
        buckets = res.get('aggregations').get('top_tags').get('buckets')
        for bucket in buckets:
            yield bucket.get("key"), bucket.get("doc_count")

    def get_by_tag(self, tag):
        body = {
            "query": {
                "term": {
                    "app_specific_data.tag": tag
                }
            }
        }
        res = self.__do_request(self._es.search,
                                index = self._raw_index,
                                body = body)
        logger.debug(res)
        hits = res.get('hits').get('hits')
        for hit in hits:
            yield hit.get('_source')

    def get_by_id(self, id):
        res = self.__do_request(self._es.get,
                                index = self._raw_index,
                                id = id)
        return res.get('_source')


def main():
    logger.info(f'Starting elastic experiments')
    conf = SpotConfig()
    elastic = Elastic(conf)

    min_end_time = datetime.now() - timedelta(days=365)
    max_end_time = datetime.now()

    completed_ids = elastic.get_processed_ids(min_end_time, max_end_time)

    for id in completed_ids:
        print(id)


if __name__ == '__main__':
    main()
