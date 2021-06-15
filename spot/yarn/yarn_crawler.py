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
import time
from urllib.parse import urlparse

from spot.yarn.yarn_wrapper import YarnWrapper
from spot.crawler.elastic import Elastic
from spot.utils.config import SpotConfig
import spot.utils.setup_logger

logger = logging.getLogger(__name__)


def main():
    logger.info(f'Starting YARN crawler')
    conf = SpotConfig()

    # yarn_api_base_url
    #yarn_clust_index = spot_yarn_cluster_1
    #yarn_apps_index = spot_yarn_cluster_1
    #yarn_sleep_seconds

    host = urlparse(conf.yarn_api_base_url).hostname

    elastic = Elastic(conf)
    yarn = YarnWrapper(conf.yarn_api_base_url)



    while True:
        logger.debug(f"Getting data from YARN at {conf.yarn_api_base_url}")
        # cluster stats
        clust_stats = yarn.get_cluster_stats()
        #pprint(clust_stats)
        elastic.save_yarn_cluster_stats(clust_stats)
        elastic.log_indexes_stats()

        # apps stats
        finishedTimeBegin = elastic.get_yarn_latest_finished_time()
        apps = yarn.get_apps(states=['FINISHED'],finishedTimeBegin=finishedTimeBegin)
        elastic.save_yarn_apps(apps)

        #for app in apps:
        scheduler_docs = yarn.get_scheduler_docs()
        elastic.save_yarn_scheduler_docs(scheduler_docs)


        time.sleep(conf.yarn_sleep_seconds)


if __name__ == '__main__':
    main()
