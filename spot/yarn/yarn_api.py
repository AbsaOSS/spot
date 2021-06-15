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
import datetime

import spot.utils.setup_logger

logger = logging.getLogger(__name__)


#see YARN API at https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
class Yarn:
    def __init__(self, yarn_base_url):
        self._yarn_base_url = yarn_base_url
        self._session = None

    def _init_session(self):
        logger.debug('starting new YARN session')
        self._session = requests.Session()
        retries = requests.packages.urllib3.util.retry.Retry(total=10, backoff_factor=1, status_forcelist=[])
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        self._session.mount(self._yarn_base_url, adapter)

    def _get_data(self, path, params={}):
        if self._session is None:
            self._init_session()

        url = f"{self._yarn_base_url}/{path}"
        logger.debug(f"sending request to {url} with params {params}")
        headers = {'Accept': 'application/json'}
        response = self._session.get(url, params=params, headers=headers)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        return response.json()

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
        logger.debug(f"Fetching apps from: {self._yarn_base_url}")
        path = 'cluster/apps'
        params = {'states':states,
                  'finalStatus':finalStatus,
                  'user': user,
                  'queue': queue,
                  'limit': limit,
                  'startedTimeBegin': startedTimeBegin,
                  'startedTimeEnd': startedTimeEnd,
                  'finishedTimeBegin': finishedTimeBegin,
                  'finishedTimeEnd': finishedTimeEnd,
                  'applicationTypes': applicationTypes,
                  'applicationTags': applicationTags,
                  'name': name,
                  'deSelects': deSelects
                  }
        data = self._get_data(path, params)
        return data

    def get_app(self, app_id):
        logger.debug(f"Fetching app {app_id} from: {self._yarn_base_url}")
        path = f"cluster/apps/{app_id}"
        data = self._get_data(path)
        return data

    def get_appattempts(self, app_id):
        logger.debug(f"Fetching app {app_id} attempts from: {self._yarn_base_url}")
        path = f"cluster/apps/{app_id}/appattempts"
        data = self._get_data(path)
        return data

    def get_appattemp_containers(self, app_id, attempt_id):
        logger.debug(f"Fetching app {app_id} attempt {attempt_id} containersfrom: {self._yarn_base_url}")
        path = f"cluster/apps/{app_id}/appattempts/{attempt_id}/containers"
        data = self._get_data(path)
        return data

    def get_cluster_info(self):
        logger.debug(f"Fetching cluster info from: {self._yarn_base_url}")
        path='cluster/info'
        data = self._get_data(path)
        return data

    def get_cluster_metrics(self):
        logger.debug(f"Fetching cluster metrics from: {self._yarn_base_url}")
        path='cluster/metrics'
        data = self._get_data(path)
        return data

    def get_cluster_scheduler(self):
        logger.debug(f"Fetching cluster scheduler from: {self._yarn_base_url}")
        path='cluster/scheduler'
        data = self._get_data(path)
        return data

    def get_appstatistics(self,
                          states=None,
                          applicationTypes=None):
        logger.debug(f"Fetching appstatistics: {self._yarn_base_url}")
        path = 'cluster/appstatistics'
        params = {'states':states,
                  'applicationTypes': applicationTypes
                  }
        data = self._get_data(path, params)
        return data






def main():
    bas_uri = 'http://jhbpsr000001014.corp.dsarena.com:8088/ws/v1'
    yarn_cli = Yarn(bas_uri)

    #cluster_info = yarn_cli.get_cluster_info()
    #pprint(cluster_info)

    #cluster_metrics = yarn_cli.get_cluster_metrics()
    #pprint(cluster_metrics)

    cluster_scheduler = yarn_cli.get_cluster_scheduler()
    pprint(cluster_scheduler)


    #appstatistics = yarn_cli.get_appstatistics(applicationTypes=['spark'])
    #pprint(appstatistics)

    #app_id='application_1618993715398_231994'

    #app = yarn_cli.get_app(app_id)
    #pprint(app)

    #attempts = yarn_cli.get_appattempts(app_id)
    #pprint(attempts)

    #att = attempts['appAttempts']['appAttempt'][0]['appAttemptId']

    #containers=yarn_cli.get_appattemp_containers(app_id,att)
    #pprint(containers)

    #states=['FINISHED']
    #datetime_format = "%Y-%m-%dT%H:%M:%S"
    #finishedTimeBegin = int(round(datetime.datetime.strptime('2021-04-29T01:00:00', datetime_format).timestamp()* 1000))
    #finishedTimeEnd = int(round(datetime.datetime.strptime('2021-04-30T05:01:00', datetime_format).timestamp()* 1000))
    #apps = yarn_cli.get_apps(#limit=100,
    #                         states=states,
    #                         finishedTimeBegin=finishedTimeBegin,
    #                         finishedTimeEnd=finishedTimeEnd
    #                         )
    #pprint(apps)

    #for app in apps['apps']['app']:
    #    print(f"{app['id']}\t{app['user']}\t{app['state']}\t{app['finalStatus']}\t{app['startedTime']}\t{app['finishedTime']}")


    #print(f"Apps found: {len(apps['apps']['app'])}")


if __name__ == '__main__':
    main()
