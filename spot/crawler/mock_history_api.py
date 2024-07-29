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
import json
from datetime import datetime, timedelta, timezone
import random
import string

import spot.utils.setup_logger

_dt_format = "%Y-%m-%dT%H:%M:%S.%fGMT"


def _parse_datetime(str_datetime, format=_dt_format):
    return datetime.strptime(str_datetime, format).replace(tzinfo=timezone.utc)

logger = logging.getLogger(__name__)


class SparkHistory:
    def __init__(self, spark_history_base_url, ssl_path=None):
        self._spark_history_base_url = spark_history_base_url
        self.verify = ssl_path
        self._session = None
        logger.debug('MOCK Spark History API is being used. No real data.')


    @staticmethod
    def _merge_attempt_id(app_id, attempt):
        if attempt is None:
            return app_id
        else:
            return f"{app_id}/{attempt}"


    def get_app_attempts(self,
                         status=None,
                         min_date=None,
                         max_date=None,
                         min_end_date=None,
                         max_end_date=None,
                         apps_limit=None,
                         ):
        logger.info(f"Generationg MOCK data for app attempts")
        if max_end_date:
            parsed_max_end_date = _parse_datetime(max_end_date)
        else:
            parsed_max_end_date = datetime.now(timezone.utc)

        if min_end_date:
            parsed_min_end_date = _parse_datetime(min_end_date)
        else:
            parsed_min_end_date = datetime.now(timezone.utc) - timedelta(hours=12)
        td = parsed_max_end_date - parsed_min_end_date

        if not apps_limit:
            apps_limit = 1000000

        data = []
        for i in range(0, apps_limit-1):
            endTime = parsed_min_end_date + random.random() * td
            duration = random.randint(2000, 3600000)
            startTime = endTime - timedelta(milliseconds=duration)
            data.append({
                'id': f"app_{i}",
                'name': f"mock_{i}",
                'attempts': [{
                    "startTime": startTime.strftime(_dt_format),
                    "endTime": endTime.strftime(_dt_format),
                    "lastUpdated": endTime.strftime(_dt_format),
                    "duration": duration,
                    "sparkUser": "mock",
                    "completed": True,
                    "appSparkVersion": "100500.42",
                    "startTimeEpoch": startTime.timestamp() * 1000,
                    "endTimeEpoch": endTime.timestamp() * 1000,
                    "lastUpdatedEpoch": endTime.timestamp() * 1000,
                }]
            })
        return data

    def get_environment(self, app_id, attempt):
        attempt_id = self._merge_attempt_id(app_id, attempt)
        logger.debug(f"generating MOCK environment for {attempt_id}")
        data = {
            "runtime": {
                "javaVersion": "100500.42",
                "javaHome": "/not/real/path",
                "scalaVersion": "version 100500.42"
            },
            "sparkProperties": [
                [
                    "spark.app.id",
                    app_id
                ],
                [
                    "spark.app.name",
                    ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
                ],
                [
                    "spark.driver.cores",
                    "1"
                ],
                [
                    "spark.driver.host",
                    "192.168.0.1"
                ],
                [
                    "spark.driver.memory",
                    f"{random.randint(1,16)}g"
                ],
                [
                    "spark.driver.port",
                    "100500"
                ],
                [
                    "spark.dynamicAllocation.enabled",
                    "true"
                ],
                [
                    "spark.dynamicAllocation.executorAllocationRatio",
                    "0.5"
                ],
                [
                    "spark.dynamicAllocation.executorIdleTimeout",
                    "60s"
                ],
                [
                    "spark.dynamicAllocation.maxExecutors",
                    f"{random.randint(8,16)}"
                ],
                [
                    "spark.dynamicAllocation.minExecutors",
                    "0"
                ],
                [
                    "spark.eventLog.dir",
                    "hdfs:///spark-history/"
                ],
                [
                    "spark.eventLog.enabled",
                    "true"
                ],
                [
                    "spark.executor.cores",
                    "1"
                ],
                [
                    "spark.executor.extraLibraryPath",
                    "/some/path"
                ],
                [
                    "spark.executor.heartbeatInterval",
                    "1000000"
                ],
                [
                    "spark.executor.id",
                    "driver"
                ],
                [
                    "spark.executor.memory",
                    "4g"
                ],
                [
                    "spark.history.fs.cleaner.enabled",
                    "true"
                ],
                [
                    "spark.history.fs.cleaner.interval",
                    "1d"
                ],
                [
                    "spark.history.fs.cleaner.maxAge",
                    "7d"
                ],
                [
                    "spark.history.fs.cleaner.maxNum",
                    "100000"
                ],
                [
                    "spark.history.fs.logDirectory",
                    "hdfs:///spark-history/"
                ],
                [
                    "spark.history.fs.numReplayThreads",
                    "8"
                ],
                [
                    "spark.history.provider",
                    "org.apache.spark.deploy.history.FsHistoryProvider"
                ],
                [
                    "spark.history.retainedApplications",
                    "50"
                ],
                [
                    "spark.history.ui.port",
                    "18081"
                ],
                [
                    "spark.master",
                    "yarn"
                ],
                [
                    "spark.network.timeout",
                    "100000"
                ],
                [
                    "spark.port.maxRetries",
                    "2800"
                ],
                [
                    "spark.scheduler.mode",
                    "FIFO"
                ],
                [
                    "spark.shuffle.service.enabled",
                    "true"
                ],
                [
                    "spark.sql.adaptive.enabled",
                    "true"
                ],
                [
                    "spark.sql.adaptive.shuffle.targetPostShuffleInputSize",
                    "134217728"
                ],
                [
                    "spark.submit.deployMode",
                    "cluster"
                ],
                [
                    "spark.yarn.am.memoryOverhead",
                    "1024"
                ],
                [
                    "spark.yarn.driver.memoryOverhead",
                    "1024"
                ],
                [
                    "spark.yarn.executor.memoryOverhead",
                    "1024"
                ],
                [
                    "spark.yarn.historyServer.address",
                    "localhost:18081"
                ],
                [
                    "spark.yarn.queue",
                    "default"
                ]
            ],
            "hadoopProperties": [],
            "metricsProperties": [],
            "classpathEntries": [],
            "resourceProfiles": []
        }
        return data

    def get_allexecutors(self, app_id, attempt):
        attempt_id = self._merge_attempt_id(app_id, attempt)
        logger.debug(f'GENERATING MOCK all executors for {attempt_id}')
        path = f"applications/{attempt_id}/allexecutors"
        data = [{
            "id": "driver",
            "hostPort": "0.0.0.0:100500",
            "isActive": False,
            "rddBlocks": 0,
            "memoryUsed": 0,
            "diskUsed": 0,
            "totalCores": 1,
            "maxTasks": 8,
            "activeTasks": 0,
            "failedTasks": 0,
            "completedTasks": 0,
            "totalTasks": 0,
            "totalDuration": random.randint(2000, 3600000),
            "totalGCTime": 0,
            "totalInputBytes": 0,
            "totalShuffleRead": 0,
            "totalShuffleWrite": 0,
            "isBlacklisted": False,
            "maxMemory": random.randint(1024,2101975449),
            "addTime": "2024-05-25T20:55:30.044GMT",
            "executorLogs": {},
            "memoryMetrics" : {
                "usedOnHeapStorageMemory": 0,
                "usedOffHeapStorageMemory": 0,
                "totalOnHeapStorageMemory": random.randint(1024,2101975449),
                "totalOffHeapStorageMemory": 0
            },
            "blacklistedInStages": [],
            "attributes": {},
            "resources": {},
            "resourceProfileId": 0,
            "isExcluded": False,
            "excludedInStages": []
        }]
        for i in range(0, random.randint(1, 8)):
            data.append({
                "id": f"{i}",
                "hostPort": f"192.168.0.{i}:100500",
                "isActive": False,
                "rddBlocks": 0,
                "memoryUsed": 0,
                "diskUsed": 0,
                "totalCores": 1,
                "maxTasks": random.randint(0, 8),
                "activeTasks": 0,
                "failedTasks": 0,
                "completedTasks": random.randint(0, 8),
                "totalTasks": random.randint(0, 8),
                "totalDuration": random.randint(2000, 3600000),
                "totalGCTime": 0,
                "totalInputBytes": random.randint(0, 100500),
                "totalShuffleRead": random.randint(0, 100500),
                "totalShuffleWrite": random.randint(0, 100500),
                "isBlacklisted": False,
                "maxMemory": random.randint(1024,2101975449),
                "addTime": "2024-05-25T20:55:30.044GMT",
                "executorLogs": {},
                "memoryMetrics" : {
                    "usedOnHeapStorageMemory": 0,
                    "usedOffHeapStorageMemory": 0,
                    "totalOnHeapStorageMemory": random.randint(1024,2101975449),
                    "totalOffHeapStorageMemory": 0
                },
                "blacklistedInStages": [],
                "attributes": {},
                "resources": {},
                "resourceProfileId": 0,
                "isExcluded": False,
                "excludedInStages": []
            })
        return data

    def get_stages(self, app_id, attempt, status=None):
        attempt_id = self._merge_attempt_id(app_id, attempt)
        logger.debug(f"GENERATING MOCK stages for {attempt_id}")
        data = []
        for i in range(0, random.randint(0, 10)):
            data.append({
                "status": "COMPLETE",
                "stageId": i,
                "attemptId": 0,
                "numActiveTasks": 0,
                "numCompleteTasks": random.randint(0, 10),
                "numFailedTasks": 0,
                "numKilledTasks": 0,
                "executorRunTime": random.randint(2000, 3600000),
                "submissionTime": "2024-05-25T20:55:30.044GMT",
                "firstTaskLaunchedTime": "2024-05-25T20:54:42.544GMT",
                "completionTime": "2024-05-25T20:55:30.044GMT",
                "executorDeserializeTime" : 359,
                "executorDeserializeCpuTime" : 311793618,
                "executorRunTime": random.randint(1,3959),
                "executorCpuTime": random.randint(1000,3214392685),
                "resultSize": random.randint(10,1987),
                "jvmGcTime": 0,
                "resultSerializationTime": 1,
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
                "peakExecutionMemory": 0,
                "inputBytes": random.randint(0,100500),
                "inputRecords" : random.randint(0,100500),
                "outputBytes" : random.randint(0,100500),
                "outputRecords" : random.randint(100,100500),
                "shuffleRemoteBlocksFetched": 0,
                "shuffleLocalBlocksFetched": 0,
                "shuffleFetchWaitTime": 0,
                "shuffleRemoteBytesRead": 0,
                "shuffleRemoteBytesReadToDisk": 0,
                "shuffleLocalBytesRead": 0,
                "shuffleReadBytes": 0,
                "shuffleReadRecords": 0,
                "shuffleCorruptMergedBlockChunks": 0,
                "shuffleMergedFetchFallbackCount": 0,
                "shuffleMergedRemoteBlocksFetched": 0,
                "shuffleMergedLocalBlocksFetched": 0,
                "shuffleMergedRemoteChunksFetched": 0,
                "shuffleMergedLocalChunksFetched": 0,
                "shuffleMergedRemoteBytesRead": 0,
                "shuffleMergedLocalBytesRead": 0,
                "shuffleRemoteReqsDuration": 0,
                "shuffleMergedRemoteReqsDuration": 0,
                "shuffleWriteBytes": random.randint(0,59),
                "shuffleWriteTime": random.randint(0,4440289),
                "shuffleWriteRecords": random.randint(1, 42),
                "name": ''.join(random.choices(string.ascii_uppercase + string.digits, k=20)),
                "details": ''.join(random.choices(string.ascii_uppercase + string.digits, k=30)),
                "schedulingPool": "default",
                "accumulatorUpdates": [],
                "rddIds": [],
                "killedTasksSummary": {},
                "resourceProfileId": 0
            })
        return data
