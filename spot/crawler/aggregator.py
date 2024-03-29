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
from datetime import datetime, timezone

import spot.crawler.history_api as history_api
from spot.crawler.commons import get_last_attempt, parse_to_bytes, parse_to_bytes_default_MiB, parse_to_bytes_default_KiB,\
    string_to_bool, parse_to_ms
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
    'stage': [
        'details'
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
        'spark_history_kerberos_keytab',
        'spark_org_apache_hadoop_yarn_server_webproxy_amfilter_AmIpFilter_param_RM_HA_URLS',
        'spark_history_kerberos_principal',
        'spark_driver_extraJavaOptions',
        'spark_executor_extraJavaOptions',
        'spark_executorEnv_PYTHONPATH',
        'spark_yarn_am_extraJavaOptions',
        'spark_yarn_keytab',
        'spark_yarn_principal'


    ],
    'runtime': [
        'javaHome'
    ]
}

# dict of cast functions to apply to each Spark property
_cast_sparkProperties_dict = {
    'spark_port_maxRetries': int,
    'spark_executor_instances': int,
    'spark_driver_cores': int,
    'spark_executor_cores': int,
    'spark_dynamicAllocation_maxExecutors': int,
    'spark_dynamicAllocation_minExecutors': int,
    'spark_yarn_maxAppAttempts': int,
    'spark_yarn_containerLauncherMaxThreads': int,
    'spark_yarn_submit_file_replication' : int,


    'spark_dynamicAllocation_executorAllocationRatio': float,
    'spark_memory_fraction': float,

    'spark_eventLog_enabled': string_to_bool,
    'spark_logConf': string_to_bool,
    'spark_hadoop_yarn_timeline-service_enabled': string_to_bool,
    'spark_dynamicAllocation_enabled': string_to_bool,
    'spark_history_fs_cleaner_enabled': string_to_bool,
    'spark_rdd_compress': string_to_bool,
    'spark_shuffle_service_enabled': string_to_bool,
    'spark_speculation': string_to_bool,
    'spark_sql_adaptive_enabled': string_to_bool,
    'spark_sql_hive_convertMetastoreParquet': string_to_bool,
    'spark_sql_parquet_writeLegacyFormat': string_to_bool,
    'spark_history_kerberos_enabled': string_to_bool,
    'spark_yarn_preserve_staging_files': string_to_bool,
    'spark_yarn_submit_waitAppCompletion': string_to_bool,

    'spark_driver_memory': parse_to_bytes,
    'spark_executor_memory': parse_to_bytes,
    'spark_driver_maxResultSize': parse_to_bytes,
    'spark_sql_adaptive_shuffle_targetPostShuffleInputSize': parse_to_bytes,
    'spark_sql_autoBroadcastJoinThreshold': parse_to_bytes,
    'spark_files_maxPartitionBytes': parse_to_bytes,
    'spark_files_openCostInBytes': parse_to_bytes,

    'spark_driver_memoryOverhead': parse_to_bytes_default_MiB,
    'spark_executor_memoryOverhead': parse_to_bytes_default_MiB,
    'spark_yarn_driver_memoryOverhead': parse_to_bytes_default_MiB,
    'spark_yarn_executor_memoryOverhead': parse_to_bytes_default_MiB,
    'spark_yarn_am_memoryOverhead': parse_to_bytes_default_MiB,
    'spark_executor_pyspark_memory': parse_to_bytes_default_MiB,
    'spark_reducer_maxSizeInFlight': parse_to_bytes_default_MiB,
    'spark_kryoserializer_buffer_max': parse_to_bytes_default_MiB,
    'spark_rpc_message_maxSize': parse_to_bytes_default_MiB,

    'spark_shuffle_file_buffer': parse_to_bytes_default_KiB,
    'spark_eventLog_buffer_kb': parse_to_bytes_default_KiB,
    'spark_kryoserializer_buffer': parse_to_bytes_default_KiB,
    'spark_broadcast_blockSize': parse_to_bytes_default_KiB,



    'spark_sql_broadcastTimeout': parse_to_ms,
    'spark_executor_heartbeatInterval': parse_to_ms,
    'spark_sql_broadcastTimeout': parse_to_ms,
    'spark_dynamicAllocation_executorIdleTimeout': parse_to_ms,
    'spark_network_timeout': parse_to_ms,
    'spark_yarn_scheduler_heartbeat_interval-ms': parse_to_ms
}

_dt_format = "%Y-%m-%dT%H:%M:%S.%fGMT"


def _parse_datetime(str_datetime, format=_dt_format):
    return datetime.strptime(str_datetime, format).replace(tzinfo=timezone.utc)


class HistoryAggregator:

    def __init__(self,
                 spark_history_base_url,
                 ssl_path=None,
                 remove_keys_dict=_remove_keys_dict,
                 time_keys_dict=_time_keys_dict,
                 cast_sparkProperties_dict=_cast_sparkProperties_dict,
                 last_attempt_only=False):
        logger.debug(f"Initializing hist aggregator. base URL: {spark_history_base_url} cert: {ssl_path}")
        self._hist = history_api.SparkHistory(spark_history_base_url, ssl_path=ssl_path)
        self._remove_keys_dict = remove_keys_dict
        self._time_keys_dict = time_keys_dict
        self.cast_sparkProperties_dict = cast_sparkProperties_dict
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
                    doc[key] = _parse_datetime(doc.get(key))
        return doc

    # for sending API requests
    @staticmethod
    def _datetime_to_str(dt):
        if dt is not None:
            # Spark hist cannot understand microseconds
            # It needs format 2020-01-15T14:59:33.707GMT
            return dt.astimezone(tz=timezone.utc).replace(tzinfo=None).isoformat(sep='T', timespec='milliseconds') + 'GMT'
        return None

    def _process_sparkProperties(self, alist):
        """Transform list of tuples to json dict and cast specific values."""
        result = {}
        for key, value in alist:
            key = key.replace('.', '_')
            if key in self.cast_sparkProperties_dict:
                value = self.cast_sparkProperties_dict[key](value)

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

        spark_props = self._process_sparkProperties(
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
