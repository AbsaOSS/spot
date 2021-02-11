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
from elasticsearch.exceptions import AuthorizationException
import pandas as pd
from datetime import datetime
import json

from spot.crawler.commons import sizeof_fmt
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
        self._raw_index = self._conf.elastic_raw_index
        self._agg_index = self._conf.elastic_agg_index
        self._err_index = self._conf.elastic_err_index

        logger.debug("Initializing elasticsearch, checking indexes")
        self.log_indexes_stats()

    def __do_request(self, request_func, **kwargs):
        '''
        Passes an Elasticsearch function to be called, catching AuthorizationException and refreshing the
        '''
        try:
            return request_func(**kwargs)
        except AuthorizationException as ae:
            if (ae.status_code == 403) and (self._conf.auth_type == 'cognito'):
                logger.debug("Status code of {0} returned, token refresh required".format(ae.status_code))
                self._es.transport.connection_pool.connections[0].session.auth = auth_config(self._conf)
                logger.debug("Auth token refreshed")
            return request_func(**kwargs)

    def _insert_item(self, index, uid, item):
        if uid is not None:
            res = self.__do_request(self._es.index,
                                index = index,
                                op_type = 'create',
                                id = uid,
                                body = item,
                                ignore = [],
                                request_timeout = REQUEST_TIMEOUT)
        else:
            res = self.__do_request(self._es.index, index = index,
                                 op_type = 'create',
                                 body = item,
                                 ignore = [],
                                 request_timeout = REQUEST_TIMEOUT)

        if res.get('result') == 'created':
            logger.debug(f'{uid} added to {index}')
        # else:
        #    self._process_elasticsearch_error(res)

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

    def log_indexes_stats(self):
        for name, count, size_bytes in self.get_indexes_stats():
            logger.debug(f'index: {name} '
                         f'count:{count} '
                         f'size: {sizeof_fmt(size_bytes)}')

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
    es = Elastic(conf.elastic_raw_index)
    top_tags = list(es.get_top_tags(8))

    for tag, count in top_tags:
        print(f'{tag}: {count}')

    tag1, count1 = top_tags[0]
    runs = es.get_by_tag(tag1)

    # convert to df
    df = pd.io.json.json_normalize(list(runs)).set_index('id')
    for column in df.columns:
        print(column)

    print(df[['name', 'app_specific_data.tag']])


if __name__ == '__main__':
    main()
