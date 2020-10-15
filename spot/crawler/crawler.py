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
import time
import re
from datetime import datetime
from pprint import pprint

from urllib.parse import urlparse
from spot.utils.config import SpotConfig
from spot.crawler.flattener import flatten_app
from spot.crawler.aggregator import HistoryAggregator
from spot.crawler.elastic import Elastic
from spot.crawler.crawler_args import CrawlerArgs
import spot.utils.setup_logger

from spot.enceladus.menas_aggregator import MenasAggregator

logger = logging.getLogger(__name__)


def _include_all_filter(app_name):
    return True


def get_default_classification(name):
    classification = {
        'app': name,
        'type': name,
    }
    values = re.split(r'[ ;,.\-\%\_]', name)
    i = 1
    for val in values:
        classification[i] = val
        i += 1
    return classification


def get_default_tag(classification):
    tag = classification.get('app', None)
    return tag


def default_enrich(app):
    app_name = app.get('name')
    data = {}
    clfsion = get_default_classification(app_name)
    data['classification'] = clfsion
    data['tag'] = get_default_tag(clfsion)
    app['app_specific_data'] = data
    return app


class DefaultSaver:

    def __init__(self):
        pass

    @staticmethod
    def save_app(app):
        pprint(app)

    @staticmethod
    def save_agg(agg):
        pprint(agg)

    @staticmethod
    def save_err(app):
        pprint(app)

    @staticmethod
    def log_indexes_stats():
        pass


class Crawler:

    def __init__(self, spark_history_url,
                 name_filter_func=_include_all_filter,
                 app_specific_obj=None,
                 save_obj=DefaultSaver(),
                 last_date=None,
                 seen_app_ids=set()):
        self._agg = HistoryAggregator(spark_history_url)
        self._history_host = urlparse(spark_history_url).hostname
        self._name_filter_func = name_filter_func
        self._save_obj = save_obj
        self._app_specific_obj = app_specific_obj
        self._latest_seen_date = last_date
        # list of apps with the same last date, seen in the previous iteration
        self._previous_tabu_set = seen_app_ids
        # tabu list being constructed for the next iteration
        self._new_tabu_set = set()

    def _process_raw(self, app):
        # add data
        try:
            self._agg.add_app_data(app)
            app = default_enrich(app)
            if self._app_specific_obj is not None:
                if self._app_specific_obj.is_matching_app(app):
                    app = self._app_specific_obj.enrich(app)
        except Exception as e:
            error_msg = str(e)
            logger.warning(
                f"Failed to process raw for app: {app.get('id')} error: {error_msg}")
            app['spot']['error'] = {
                'type': e.__class__.__name__,
                'message': error_msg,
                'stage': 'raw'
            }
            self._save_obj.save_err(app)
            return False
        # save
        self._save_obj.save_app(app)
        return True

    def _process_aggs(self, app):
        # get aggregations
        try:
            if self._app_specific_obj is not None:
                if self._app_specific_obj.is_matching_app(app):
                    app = self._app_specific_obj.aggregate(app)

            aggs = flatten_app(app)
        except Exception as e:
            error_msg = str(e)
            logger.warning(
                f"Failed to process agg for app: {app.get('id')} error: {error_msg}")
            app['spot']['error'] = {
                'type': e.__class__.__name__,
                'message': error_msg,
                'stage': 'aggregations'
            }
            self._save_obj.save_err(app)
            return False

        # save aggregations
        for agg in aggs:
            self._save_obj.save_agg(agg)
        return True

    def _process_app(self, app):
        app['history_host'] = self._history_host
        app['spot'] = {
            'time_processed': datetime.now()
        }
        success = self._process_raw(app)
        if success:  # if no exceptions while getting data
            self._process_aggs(app)

    def process_new_runs(self):
        processing_start = datetime.now()
        processing_counter = 0
        logger.info(
            f"Fetching new apps, completed since {self._latest_seen_date}")
        apps = self._agg.next_app(min_end_date=self._latest_seen_date,
                                  app_status='completed')
        new_counter = 0
        matched_counter = 0
        self._new_tabu_set = set()
        for app in apps:
            # update latest seen date
            self._update_latest_seen_date(app)
            # check if app seen before
            app_id = app.get('id')
            if app_id not in self._previous_tabu_set:
                new_counter += 1
                app_name = app.get('name')
                # filter apps of interest
                if self._name_filter_func(app_name):
                    matched_counter += 1
                    self._process_app(app)
                    processing_counter += 1
                    if processing_counter % 10 == 0:
                        delta_seconds = (datetime.now() -
                                         processing_start).total_seconds()
                        per_hour = processing_counter * 3600 / delta_seconds
                        logger.debug(f"processed {processing_counter} runs "
                                     f"in {delta_seconds} seconds "
                                     f"average rate: {per_hour} runs/hour")
                        self._save_obj.log_indexes_stats()

        self._previous_tabu_set = self._new_tabu_set
        logger.info(f"Iteration finished. New apps: {new_counter} "
                    f"matching apps : {matched_counter}")
        logger.debug(f"tabu_set: {self._previous_tabu_set}"
                     f" last date: {self._latest_seen_date}")

    # We need to keep track of which applications were already processed.
    # For this reason, we store the latest seen completion date.
    # We use that date in the next iteration when fetching new apps from Spark history.
    # Because multiple apps may have completed at the same time,
    # and, also, the last seen application will always appear in the next iteration again,
    # we also store a _tabu_set: a set of app ids, completed at _latest_seen_date
    def _update_latest_seen_date(self, app):
        app_id = app.get('id')
        for attempt in app.get('attempts'):
            end_time = attempt.get('endTime')
            if end_time is not None:
                if (self._latest_seen_date is None) \
                        or (end_time > self._latest_seen_date):
                    # new latest app found
                    self._latest_seen_date = end_time
                    self._new_tabu_set = {app_id}
                elif end_time == self._latest_seen_date:
                    # another app completed same time
                    self._new_tabu_set.add(app_id)
                    logger.debug(f'added app {app_id} '
                                 f'to tabu list')


def main():
    logger.info(f'Starting crawler')
    cmd_args = CrawlerArgs().parse_args()
    conf = SpotConfig()

    if conf.menas_api_url is not None:
        logger.info(f"adding Menas aggregator, api url {conf.menas_api_url}")
        menas_ag = MenasAggregator(conf.menas_api_url,
                                   conf.menas_username,
                                   conf.menas_password)
    else:
        menas_ag = None
        logger.info(
            'Menas integration disabled as api url not provided in config')

    elastic = Elastic(host=conf.elastic_host,
                      port=conf.elastic_port,
                      username=conf.elastic_username,
                      password=conf.elastic_password,
                      raw_index_name=conf.elastic_raw_index,
                      agg_index_name=conf.elastic_agg_index,
                      err_index_name=conf.elastic_err_index,
                      elasticsearch_url=conf.elasticsearch_url,
                      ssl=conf.ssl)

    # find starting end date and list of seen apps
    last_seen_end_date, seen_ids = elastic.get_latests_time_ids()
    logger.debug(
        f'Latest stored end date: {last_seen_end_date} seen apps: {seen_ids}')

    logger.debug(f'param_end_date: {cmd_args.min_end_date}')

    if (cmd_args.min_end_date is not None)
    and (
        (last_seen_end_date is None)
        or (last_seen_end_date < cmd_args.min_end_date)
    ):
        last_seen_end_date = cmd_args.min_end_date
        seen_ids = dict()
    logger.debug(f'Will get apps completed after: {last_seen_end_date}')

    crawler = Crawler(conf.spark_history_url,
                      app_specific_obj=menas_ag,
                      save_obj=elastic,
                      last_date=last_seen_end_date,
                      seen_app_ids=seen_ids
                      )

    sleep_seconds = conf.crawler_sleep_seconds

    while True:
        crawler.process_new_runs()
        elastic.log_indexes_stats()
        time.sleep(sleep_seconds)


if __name__ == '__main__':
    main()
