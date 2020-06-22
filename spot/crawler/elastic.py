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
from datetime import datetime
from collections import deque
import elasticsearch
from elasticsearch.helpers import bulk
from pprint import pprint
import json
from bson import json_util
import pandas as pd


from spot.crawler.commons import sizeof_fmt
import spot.utils.setup_logger
from spot.utils.config import SpotConfig

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30

dynamic_mapping = {
    "mappings": {
        "dynamic_templates": [
            {
                "attempts": {
                    "match": "attempts",
                    "mapping": {
                        "type": "nested"
                    }
                }
            },
            {
                "allexecutors": {
                    "match": "allexecutors",
                    "mapping": {
                        "type": "nested"
                    }
                }
            },
            {
                "stages": {
                    "match": "stages",
                    "mapping": {
                        "type": "nested"
                    }
                }
            },
            {
                "memoryMetrics": {
                    "match": "memoryMetrics",
                    "mapping": {
                        "type": "nested"
                    }
                }
            },
            {
                "environment": {
                    "match": "environment",
                    "mapping": {
                        "type": "object"
                    }
                }
            },
            {
                "runtime": {
                    "match": "runtime",
                    "mapping": {
                        "type": "object"
                    }
                }
            },
            {
                "sparkProperties": {
                    "match": "sparkProperties",
                    "mapping": {
                        "type": "object"
                    }
                }
            },
        ]
    }
}


class Elastic:

    def __init__(self,
                 raw_index_name='raw_default',
                 agg_index_name='agg_default',
                 host='localhost',
                 port=9200,
                 #app_spec_mapping=None #remove
                 ):
        connection = {
            'host': host,
            'port': port
        }
        self._es = elasticsearch.Elasticsearch([connection],
                                               sniff_on_start=True,
                                               sniff_on_connection_fail=True,
                                               sniffer_timeout=REQUEST_TIMEOUT,
                                               retry_on_timeout=True)
        self._raw_index = raw_index_name
        self._agg_index = agg_index_name

        logger.debug(f'Initializing elasticsearch, checking indexes')
        self.log_indexes_stats()

    def _insert_item(self, index, uid, item):
        res = self._es.index(index=index,
                             op_type = 'create',
                             id=uid,
                             body=item,
                             ignore=[400, 409],
                             request_timeout=REQUEST_TIMEOUT)
        if res.get('result') == 'created':
            logger.debug(f'{uid} added to {self._raw_index}')
        elif res.get('status') == 409:
            logger.warning(f'Run already indexed: {res.get("error").get("reason")}')
        elif res.get('status') == 400:
            logger.warning(res.get('error').get('reason'))

    def save_app(self, app):
        uid = app.get('id')
        self._insert_item(self._raw_index, uid, app)

    def save_agg(self, agg):
        app_id = agg.get('id')
        attempt_id = agg.get('attempt').get('attemptId', 0)
        uid = f'{app_id}-{attempt_id}'
        self._insert_item(self._agg_index, uid, agg)

    def get_latests_time_ids(self):
        id_set = set()
        if not self._es.indices.exists(index=self._raw_index):
            return None, id_set

        # get max end_time
        body_max_end_time = {
            'size': 0,
            'aggs' : {
                'max_endTime': {'max': {'field': 'attempts.endTime'}}
            }
        }

        res_time = self._es.search(index=self._raw_index,
                              body=body_max_end_time)
        # es uses epoch_millis internally
        timestamp = res_time['aggregations']['max_endTime']['value']
        # elastic search does not understand it's own internal time format in queries,
        # therefore using string
        str_max_end_time = res_time['aggregations']['max_endTime']['value_as_string']
        if timestamp is None:
            return None, id_set

        # get list of ids fot the same date
        body_id_list = {
           "stored_fields": [],
           "query": {
               "match": {
                  "attempts.endTime": str_max_end_time
               }
            }
        }

        res_ids = self._es.search(index=self._raw_index,
                                  body=body_id_list)

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
            self._agg_index
        ]
        for index in indexes:
            if self._es.indices.exists(index=index):
                yield self._get_index_stats(index)
            else:
                logger.warning(f'index {index} does not exist')

    def _get_index_stats(self, index):
        res = self._es.indices.stats(index=index)
        primaries = res['indices'][index]['primaries']
        docs_count = primaries['docs']['count']
        bytes = primaries['store']['size_in_bytes']
        return index, docs_count, bytes

    def get_top_tags(self, min_count = 8):
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
        res = self._es.search(index=self._raw_index, body=body)
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
        res = self._es.search(index=self._raw_index, body=body)
        logger.debug(res)
        hits = res.get('hits').get('hits')
        for hit in hits:
            yield hit.get('_source')

    def get_by_id(self, id):
        res = self._es.get(index=self._raw_index, id=id)
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
