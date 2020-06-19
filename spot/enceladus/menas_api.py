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

import requests
import logging
import time
from requests.packages.urllib3.util.retry import Retry

import spot.utils.setup_logger

logger = logging.getLogger(__name__)


class MenasApi:

    def __init__(self, api_base_url, username, password):
        logger.debug("initializing Menas connector")
        self.base_url = api_base_url
        self.username = username
        self.password = password
        self.login_url = f'{api_base_url}/login'
        self._session = None

    def _login(self):
        if hasattr(self, 'session'):
            logger.debug('restaring Menas session')

        logger.debug('satring new Menas session')
        self._session = requests.Session()
        retries = Retry(total=10, backoff_factor=1)
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        self._session.mount(self.base_url, adapter)

        querystring = {"username": self.username,"password": self.password}
        response = self._session.post(self.login_url, params=querystring)
        logger.debug(response.status_code)
        #logger.debug(self.session.cookies)

    def _get_data(self, path, params={}):
        if self._session is None:
            self._login()

        url = f'{self.base_url}/{path}'

        max_retries = 10
        login_timeout = 1
        for i in range(max_retries):
            logger.debug(f'sending request to {url} with params {params}'
                         f' (attempt {i})')
            response = self._session.get(url, params={})
            if response.status_code == requests.codes.ok:
                return response.json()
            elif response.status_code == requests.codes.not_found:
                logger.warning(f'{response.status_code}. url: {url} '
                               f'params: {params}')
                return {}
            elif response.status_code == requests.codes.unauthorized:
                logger.warning(f'{response.status_code}. url: {url} '
                               f'Restarting session in {login_timeout} s')
                time.sleep(login_timeout)
                login_timeout = 2 * login_timeout
                self._login()
            else:
                logger.error(f'{response.status_code}. url: {url} '
                             f'params: {params}')
                response.raise_for_status()
                return {}
        return {}

    # deprecated
    def _get_data_old(self, path, params={}):
        #if not hasattr(self, 'session'):
        if self._session is None:
            self._login()
        url = f'{self.base_url}/{path}'
        logger.debug(f'sending request to {url} with params {params}')

        response = self._session.get(url, params=params)

        result = {}
        if response.status_code == requests.codes.ok:
            result = response.json()
        elif response.status_code == requests.codes.not_found:
            logger.warning(f'{response.status_code}. url: {url} '
                       f'params: {params}')
        elif response.status_code == requests.codes.unauthorized:
            logger.warning(f'{response.status_code}. url: {url} Restarting session')
            self._login()
        else:
            logger.error(f'{response.status_code}. url: {url} '
                           f'params: {params}')
            response.raise_for_status()
        return result

    def get_dataset(self, dataset_name, dataset_version):
        path = f'dataset/detail/{dataset_name}/{dataset_version}'
        return self._get_data(path)

    def get_schema(self, schema_name, schema_version):
        path = f'schema/json/{schema_name}/{schema_version}'
        return self._get_data(path)

    def get_dataset_runs(self, dataset_name):
        path = f'runs/{dataset_name}'
        return self._get_data(path)

    def get_dataset_version_runs(self, dataset_name, dataset_version):
        path = f'runs/{dataset_name}/{dataset_version}'
        return self._get_data(path)

    def get_run(self, dataset_name, dataset_version, run_id):
        path = f'runs/{dataset_name}/{dataset_version}/{run_id}'
        return self._get_data(path)

    def get_dataset_version_latest_run(self, dataset_name, dataset_version):
        path = f'runs/{dataset_name}/{dataset_version}/latestrun'
        return self._get_data(path)

    def get_runs_by_spark_id(self, spark_app_id):
        path = f'runs/bySparkAppId/{spark_app_id}'
        return self._get_data(path)

