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

import spot.utils.setup_logger

logger = logging.getLogger(__name__)


class SparkHistory:
    def __init__(self, spark_history_base_url):
        self._spark_history_base_url = spark_history_base_url
        self._session = None

    def _init_session(self):
        logger.debug('starting new Spark History session')
        self._session = requests.Session()
        retries = requests.packages.urllib3.util.retry.Retry(total=10, backoff_factor=1, status_forcelist=['503'])
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        self._session.mount(self._spark_history_base_url, adapter)

    @staticmethod
    def _merge_attempt_id(app_id, attempt):
        if attempt is None:
            return app_id
        else:
            return f"{app_id}/{attempt}"

    def _get_data(self, path, params={}):
        if self._session is None:
            self._init_session()

        url = f"{self._spark_history_base_url}/{path}"
        logger.debug(f"sending request to {url} with params {params}")
        headers = {'Accept': 'application/json'}
        response = self._session.get(url, params=params, headers=headers)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        return response.json()

    def get_app_attempts(self,
                         status=None,
                         min_date=None,
                         max_date=None,
                         min_end_date=None,
                         max_end_date=None,
                         apps_limit=None,
                         ):
        logger.info(f"Fetching apps from: {self._spark_history_base_url}")
        app_path = 'applications'
        params = {
            'status': status,
            'minDate': min_date,
            'maxDate': max_date,
            'minEndDate': min_end_date,
            'maxEndDate': max_end_date,
            'limit': apps_limit
        }
        data = self._get_data(app_path, params)
        return data

    def get_environment(self, app_id, attempt):
        attempt_id = self._merge_attempt_id(app_id, attempt)
        logger.debug(f"getting environment for {attempt_id}")
        path = f"applications/{attempt_id}/environment"
        data = self._get_data(path)
        return data

    def get_allexecutors(self, app_id, attempt):
        attempt_id = self._merge_attempt_id(app_id, attempt)
        logger.debug(f'getting all executors for {attempt_id}')
        path = f"applications/{attempt_id}/allexecutors"
        data = self._get_data(path)
        return data

    def get_stages(self, app_id, attempt, status=None):
        attempt_id = self._merge_attempt_id(app_id, attempt)
        logger.debug(f"getting stages for {attempt_id}")
        path = f"applications/{attempt_id}/stages"
        params = {'status': status}
        data = self._get_data(path, params)
        return data
