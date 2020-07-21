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

import spot.crawler.history_api as history_api
from spot.crawler.commons import get_last_attempt
import spot.utils.setup_logger

logger = logging.getLogger(__name__)

_time_keys_dict = {
    'attempt': [
        'startTime',
        'endTime',
        'lastUpdated'
    ],
    'executor': [
        'addTime',
        'removeTime'
    ],
    'stage': [
        'submissionTime',
        'firstTaskLaunchedTime',
        'completionTime'
    ]
}

_remove_keys_dict = {
    'executor': [
        'hostPort',
        'executorLogs'
    ],
    'attempt': [
#        'sparkUser'
    ],
    'environment': [
      'systemProperties',
      'classpathEntries'
    ],
    'sparkProperties': [
        'spark_driver_host',
        'spark_driver_port',
        'spark_jars',
        'spark_eventLog_dir',
        'spark_driver_appUIAddress',
        'spark_ui_filters',
        'spark_org_apache_hadoop_yarn_server_webproxy_amfilter'
        '_AmIpFilter_param_PROXY_HOSTS',
        'spark_org_apache_hadoop_yarn_server_webproxy_amfilter'
        '_AmIpFilter_param_PROXY_URI_BASES',
        'spark_yarn_app_container_log_dir',

    ],
    'runtime': [
        'javaHome'
    ]
}

_dt_format = "%Y-%m-%dT%H:%M:%S.%f%Z"


class HistoryAggregator:

    def __init__(self,
                 spark_history_base_url,
                 remove_keys_dict=_remove_keys_dict,
                 time_keys_dict=_time_keys_dict,
                 dt_format=_dt_format,
                 last_attempt_only=False):
        logger.debug('Initialized hist aggregator')
        self._hist = history_api.SparkHistory(spark_history_base_url)
        self._remove_keys_dict = remove_keys_dict
        self._time_keys_dict = time_keys_dict
        self._dt_format = dt_format
        self.last_attempt_only = last_attempt_only

    def _remove_keys(self, doc, doc_type):
        key_list = self._remove_keys_dict.get(doc_type)
        if (doc is not None) and (key_list is not None):
            for key in key_list:
                doc.pop(key, None)
        return doc

    def _cast_datetime_values(self, doc, doc_type):
        key_list = self._time_keys_dict.get(doc_type)
        if (doc is not None) and (key_list is not None):
            for key in key_list:
                if key in doc:
                    doc[key] = datetime.strptime(doc.get(key), self._dt_format)
        return doc

    # for sending API requests
    @staticmethod
    def _datetime_to_str(dt):
        if dt is not None:
            # Spark hist cannot understand microseconds
            # It needs format 2020-01-15T14:59:33.707GMT
            return dt.isoformat(sep='T', timespec='milliseconds') + 'GMT'
        return None

    @staticmethod
    def _tuple_list_to_valid_mongo_dict(alist):
        result = {}
        for atuple in alist:
            key = atuple[0].replace('.', '_')
            value = atuple[1]
            if isinstance(value, str):
                if value.isdigit():
                    value = int(value)
            result[key] = value
        return result

    def get_all_executors(self, app_id, attempt_id):
        executors = self._hist.get_allexecutors(app_id,
                                                attempt_id)
        for executor in executors:
            self._cast_datetime_values(executor, 'executor')
            self._remove_keys(executor, 'executor')
        return executors

    def get_stages(self, app_id, attempt_id, status):
        stages = self._hist.get_stages(app_id,
                                       attempt_id,
                                       status=status)
        for stage in stages:
            self._cast_datetime_values(stage, 'stage')
            self._remove_keys(stage, 'stage')
        return stages

    def get_environment(self, app_id, attempt_id):
        environment = self._hist.get_environment(app_id,
                                                 attempt_id)

        spark_props = self._tuple_list_to_valid_mongo_dict(
            environment.get('sparkProperties'))
        self._remove_keys(spark_props, 'sparkProperties')
        environment['sparkProperties'] = spark_props

        runtime = environment.get('runtime')
        self._remove_keys(runtime, 'runtime')
        environment['runtime'] = runtime

        self._remove_keys(environment, 'environment')
        return environment

    def next_app(self,
                 app_status=None,
                 min_date=None,
                 max_date=None,
                 min_end_date=None,
                 max_end_date=None,
                 apps_limit=None):
        min_date_str = self._datetime_to_str(min_date)
        max_date_str = self._datetime_to_str(max_date)
        min_end_date_str = self._datetime_to_str(min_end_date)
        max_end_date_str = self._datetime_to_str(max_end_date)

        apps = self._hist.get_app_attempts(status=app_status,
                                           min_date=min_date_str,
                                           max_date=max_date_str,
                                           min_end_date=min_end_date_str,
                                           max_end_date=max_end_date_str,
                                           apps_limit=apps_limit)
        logger.debug(f'{len(apps)} apps found')

        # we assume the app are returned in reverses chronological order
        for app in reversed(apps):
            yield self._process_app(app)

    def _process_app(self, app):
        if self.last_attempt_only:
            app['attempts'] = get_last_attempt(app)
        for attempt in app.get('attempts'):
            self._cast_datetime_values(attempt, 'attempt')
            self._remove_keys(attempt, 'attempt')
        return app

    def add_app_data(self, app, stage_status=None,):
        app_id = app.get('id')
        logger.debug(f'fetching app details: {app_id}')
        for attempt in app.get('attempts'):
            attempt_id = attempt.get('attemptId')
            attempt['allexecutors'] = self.get_all_executors(app_id,
                                                             attempt_id)
            attempt['stages'] = self.get_stages(app_id,
                                                attempt_id,
                                                status=stage_status)
            attempt['environment'] = self.get_environment(app_id,
                                                          attempt_id)
        return app
