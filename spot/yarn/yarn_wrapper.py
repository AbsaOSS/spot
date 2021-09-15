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
from pprint import pprint
from datetime import datetime, timezone


from spot.crawler.commons import default_enrich, datetime_to_utc_timestamp_ms, utc_from_timestamp_ms
from spot.enceladus.classification import get_classification, is_enceladus_app, get_tag
import spot.yarn.yarn_api as yarn_api
from urllib.parse import urlparse

import spot.utils.setup_logger

logger = logging.getLogger(__name__)


_time_keys_dict = {
    'app': [
        'startedTime',
        'finishedTime'
    ]
}

_default_apptypes = ['all', 'SPARK', 'MAPREDUCE']


def datetime_to_timestamp_ms(dt):
    return round(datetime_to_utc_timestamp_ms)


def _cast_timestamps(doc, doc_type):
    key_list = _time_keys_dict.get(doc_type)
    if (doc is not None) and (key_list is not None):
        for key in key_list:
            if key in doc:
                doc[key] = utc_from_timestamp_ms(doc.get(key))
    return doc


class YarnWrapper:

    def __init__(self, yarn_base_url):
        self._api = yarn_api.Yarn(yarn_base_url)
        self._host = urlparse(yarn_base_url).hostname

    def get_app(self, app_id):
        doc = self._api.get_app(app_id)
        app = doc.get('app')
        if is_enceladus_app(app.get('name')):
            self._process_enceladus_app(app)
        else:
            self._process_app(app)
        return app

    def _add_spot_meta(self, doc):
        doc['spot'] = {
            'time_processed': datetime.now(tz=timezone.utc),
            'yarn_host': self._host
        }
        return doc

    def _process_app(self, app):
        app = _cast_timestamps(app, 'app')
        app = default_enrich(app)
        app = self._add_spot_meta(app)
        return app

    def _process_enceladus_app(self, app):
        app_name = app.get('name')
        data = {}
        clfsion = get_classification(app_name)
        data['classification'] = clfsion
        data['tag'] = get_tag(clfsion)
        app['app_specific_data'] = data
        app = self._add_spot_meta(app)
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
            yield self._process_app(app)

    def get_cluster_stats(self):
        doc = dict()
        #doc['info'] = self._api.get_cluster_info()
        doc['clusterMetrics'] = self._api.get_cluster_metrics().get('clusterMetrics')
        doc['appStatInfo'] = self.get_app_stats().get('appStatInfo')
        doc['schedulerInfo'] = self.get_scheduler_stats_single_doc()
        self._add_spot_meta(doc)
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

    def get_scheduler_stats_single_doc(self):
        res = self._api.get_cluster_scheduler()
        doc = res.get('scheduler').get('schedulerInfo')

        # drop excessive details
        doc.pop('capacities')
        doc.pop('health')

        # process queues
        queues = doc.get('queues').get('queue')
        q = {}
        for queue in queues:
            queueName = queue.get('queueName')
            # drop excessive details
            queue.pop('capacities')
            queue.pop('resources')
            queue.pop('users')

            q[queueName] = queue
        doc['queues'] = q

        return doc

    def get_scheduler_docs(self):
        res = self._api.get_cluster_scheduler()

        # process queueCapacitiesByPartition
        queueCapacitiesByPartition = res.get('scheduler').get('schedulerInfo').get('capacities').get('queueCapacitiesByPartition')

        for cap in queueCapacitiesByPartition:
            self._add_spot_meta(cap)
            cap['spot']['doc_type'] = 'queueCapacitiesByPartition'
            yield cap

        # process queues
        queues = res.get('scheduler').get('schedulerInfo').get('queues').get('queue')
        for queue in queues:
            queueName = queue.get('queueName')
            # process queueCapacitiesByPartition
            queueCapacitiesByPartition = queue.get('capacities').get('queueCapacitiesByPartition')
            for cap in queueCapacitiesByPartition:
                cap['queueName'] = queueName
                self._add_spot_meta(cap)
                cap['spot']['doc_type'] = 'queue_queueCapacitiesByPartition'
                yield cap
            queue.pop('capacities')

            # process resourceUsagesByPartition
            resourceUsagesByPartition = queue.get('resources').get('resourceUsagesByPartition')
            for res in resourceUsagesByPartition:
                res['queueName'] = queueName
                self._add_spot_meta(res)
                res['spot']['doc_type'] = 'queue_resourceUsagesByPartition'
                yield res

            queue.pop('resources')
            # process users
            if queue.get('users') is not None:
                users = queue.get('users').get('user')
                for user in users:
                    username = user.get('username')
                    # process user's resourceUsagesByPartition
                    resourceUsagesByPartition = user.get('resources').get('resourceUsagesByPartition')
                    for res in resourceUsagesByPartition:
                        res['queueName'] = queueName
                        res['username'] = username
                        self._add_spot_meta(res)
                        res['spot']['doc_type'] = 'user_resourceUsagesByPartition'
                        yield res
                    user.pop('resources')
                    user['queueName'] = queueName
                    self._add_spot_meta(user)
                    user['spot']['doc_type'] = 'user'
                    yield user

            queue.pop('users')
            self._add_spot_meta(queue)
            queue['spot']['doc_type'] = 'queue'
            yield queue


def main():
    # for testing/debugging purposes
    base_uri = ''
    yarn = YarnWrapper(base_uri)
    #app_id = ''

    #app = yarn.get_app(app_id)
    #pprint(app)

    #apps = yarn.get_apps(limit=3)
    #for app in apps:
    #    pprint(app)

    #clust_stats = yarn.get_cluster_stats()
    #pprint(clust_stats)

    #for doc in yarn.get_scheduler_docs():
    #    pprint(doc)


if __name__ == '__main__':
    main()
