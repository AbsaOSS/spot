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
import requests
from pprint import pprint
from datetime import datetime

import spot.utils.setup_logger
from spot.crawler.commons import default_enrich
from spot.enceladus.classification import get_classification, is_enceladus_app, get_tag
import spot.yarn.yarn_api as yarn_api

logger = logging.getLogger(__name__)


_time_keys_dict = {
    'app': [
        'startedTime',
        'finishedTime'
    ]
}

_default_apptypes = ['all', 'SPARK', 'MAPREDUCE']


def datetime_to_timestamp_ms(dt):
    return round(dt.timestamp()* 1000)


def timestamp_ms_to_datetime(ts):
    return datetime.fromtimestamp(ts / 1000.0)


def _cast_timestamps(doc, doc_type):
    key_list = _time_keys_dict.get(doc_type)
    if (doc is not None) and (key_list is not None):
        for key in key_list:
            if key in doc:
                doc[key] = timestamp_ms_to_datetime(doc.get(key))
    return doc


def _process_enceladus_app(app):
    app_name = app.get('name')
    data = {}
    clfsion = get_classification(app_name)
    data['classification'] = clfsion
    data['tag'] = get_tag(clfsion)
    app['app_specific_data'] = data
    return app

def _process_app(app):
    app = _cast_timestamps(app, 'app')
    app = default_enrich(app)
    return app


class YarnWrapper:

    def __init__(self, yarn_base_url):
        self._api = yarn_api.Yarn(yarn_base_url)

    def get_app(self, app_id):
        doc = self._api.get_app(app_id)
        app = doc.get('app')
        if is_enceladus_app(app.get('name')):
            _process_enceladus_app(app)
        else:
            _process_app(app)
        return app

    def get_apps(self,
                 states=None,
                 finalStatus=None,
                 user=None,
                 queue=None,
                 limit=None,
                 startedTimeBegin=None,
                 startedTimeEnd=None,
                 finishedTimeBegin=None,
                 finishedTimeEnd=None,
                 applicationTypes=None,
                 applicationTags=None,
                 name=None,
                 deSelects=None
                 ):

        stb = None if startedTimeBegin is None else datetime_to_timestamp_ms(startedTimeBegin)
        ste = None if startedTimeEnd is None else datetime_to_timestamp_ms(startedTimeEnd)
        ftb = None if finishedTimeBegin is None else datetime_to_timestamp_ms(finishedTimeBegin)
        fte = None if finishedTimeEnd is None else datetime_to_timestamp_ms(finishedTimeEnd)

        res = self._api.get_apps(states=states,
                                 finalStatus=finalStatus,
                                 user=user,
                                 queue=queue,
                                 limit=limit,
                                 startedTimeBegin=stb,
                                 startedTimeEnd=ste,
                                 finishedTimeBegin=ftb,
                                 finishedTimeEnd=fte,
                                 applicationTypes=applicationTypes,
                                 applicationTags=applicationTags,
                                 name=name,
                                 deSelects=deSelects)

        for app in res['apps']['app']:
            yield _process_app(app)


    def get_cluster_stats(self):
        doc = dict()
        #doc['info'] = self._api.get_cluster_info()
        doc['clusterMetrics'] = self._api.get_cluster_metrics().get('clusterMetrics')
        #doc['scheduler'] = self._api.get_cluster_scheduler()
        doc['appStatInfo'] = self.get_app_stats().get('appStatInfo')
        return doc

    def get_app_stats(self, states=None, types=_default_apptypes):
        doc = {'appStatInfo': {}}
        for app_type in types:
            doc['appStatInfo'][app_type] = {}
            req_types = [app_type]
            if app_type == 'all':
                req_types = None
            res = self._api.get_appstatistics(states=states, applicationTypes=req_types)
            for item in res.get('appStatInfo').get('statItem'):
                state = item.get('state')
                doc['appStatInfo'][app_type][state] = item.get('count')
        return doc


def main():
    bas_uri = 'http://jhbpsr000001014.corp.dsarena.com:8088/ws/v1'
    yarn = YarnWrapper(bas_uri)
    app_id = 'application_1618993715398_405265'

    #app = yarn.get_app(app_id)
    #pprint(app)

    apps = yarn.get_apps(limit=3)
    for app in apps:
        pprint(app)

    #clust_stats = yarn.get_cluster_stats()
    #pprint(clust_stats)

if __name__ == '__main__':
    main()
