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

    host = urlparse(conf.yarn_api_base_url).hostname

    elastic = Elastic(conf)
    yarn = YarnWrapper(conf.yarn_api_base_url)

    def _handle_processing_exception_(e, stage_name):
        error_msg = str(e)
        logger.warning(
            f"Failed to process {stage_name}: {error_msg}")
        err = {
            'spot': {
                'time_processed': datetime.now(tz=timezone.utc),
                'yarn_host': host,
                'error': {
                    'type': e.__class__.__name__,
                    'message': error_msg,
                    'stage': stage_name
                }
            }
        }
        elastic.save_err(err)
        if not conf.crawler_skip_exceptions:
            logger.warning('Skipping malformed metadata is disabled')
            raise e

    while True:
        logger.debug(f"Getting data from YARN at {conf.yarn_api_base_url}")

        # cluster stats
        try:
            clust_stats = yarn.get_cluster_stats()
            elastic.save_yarn_cluster_stats(clust_stats)
        except Exception as e:
            _handle_processing_exception_(e, 'yarn_cluster_stats')


        # apps stats
        try:
            finishedTimeBegin = elastic.get_yarn_latest_finished_time()
            apps = yarn.get_apps(states=['FINISHED'],finishedTimeBegin=finishedTimeBegin)
            elastic.save_yarn_apps(apps)
        except Exception as e:
            _handle_processing_exception_(e, 'yarn_apps')

        # scheduler stats:
        try:
            scheduler_docs = yarn.get_scheduler_docs()
            elastic.save_yarn_scheduler_docs(scheduler_docs)
        except Exception as e:
            _handle_processing_exception_(e, 'yarn_scheduler_docs')

        elastic.log_indexes_stats()
        time.sleep(conf.yarn_sleep_seconds)


if __name__ == '__main__':
    main()
