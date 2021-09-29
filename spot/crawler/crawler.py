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

from datetime import datetime, timedelta, timezone
from dateutil import tz
from pprint import pprint
from json.decoder import JSONDecodeError
from urllib.parse import urlparse

from spot.utils.config import SpotConfig
from spot.crawler.flattener import flatten_app
from spot.crawler.aggregator import HistoryAggregator
from spot.crawler.elastic import Elastic
from spot.crawler.crawler_args import CrawlerArgs
from spot.crawler.commons import default_enrich
from spot.utils.auth import auth_config
import spot.utils.setup_logger

from spot.enceladus.menas_aggregator import MenasAggregator

logger = logging.getLogger(__name__)


def _include_all_filter(app_name):
    return True


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
                 ssl_path=None,
                 name_filter_func=_include_all_filter,
                 app_specific_obj=None,
                 save_obj=DefaultSaver(),
                 last_date=None,
                 seen_app_ids=set(),
                 completion_timeout_seconds=0,
                 crawler_method='all',
                 lookback_hours=24*7,
                 time_step_seconds=3600,
                 skip_exceptions=False,
                 retry_attempts=24,
                 retry_sleep_seconds=900):
        self._agg = HistoryAggregator(spark_history_url, ssl_path=ssl_path)
        self._history_host = urlparse(spark_history_url).hostname
        self._name_filter_func = name_filter_func
        self._save_obj = save_obj
        self._app_specific_obj = app_specific_obj
        self.skip_exceptions = skip_exceptions
        self.completion_timeout_seconds = completion_timeout_seconds

        # set History retrieval method
        logger.debug(f'crawler_method: {crawler_method}')
        if crawler_method == 'all':
            self.process_new_runs = self.process_all_new_runs
        elif crawler_method == 'latest':
            self.process_new_runs = self.process_new_runs_since_latest_processed
        else:
            logger.warning(f'crawler_method {crawler_method} not recognized. Using the default')
            self.process_new_runs = self.process_all_new_runs

        self.lookback_delta = timedelta(hours=lookback_hours)
        self.time_step_seconds = time_step_seconds
        self.retry_sleep_seconds = retry_sleep_seconds
        self.retry_attempts = retry_attempts
        self.retry_attempts_remained = self.retry_attempts

        self._latest_seen_date = last_date
        # list of apps with the same last date, seen in the previous iteration
        self._previous_tabu_set = seen_app_ids
        # tabu list being constructed for the next iteration
        self._new_tabu_set = set()

    def _handle_processing_exception_(self, e, stage_name, id='unknown'):
        error_msg = str(e)
        logger.warning(
            f"Failed to process {stage_name} for app: {id} error: {error_msg}")
        err = {
            'spot': {
                'time_processed': datetime.now(tz=timezone.utc),
                'spark_app_id': id,
                'history_host': self._history_host,
                'error': {
                    'type': e.__class__.__name__,
                    'message': error_msg,
                    'stage': stage_name
                }
            }
        }
        self._save_obj.save_err(err)
        if not self.skip_exceptions:
            logger.warning('Skipping malformed metadata is disabled')
            raise e

    def _process_raw(self, app):
        # add data
        try:
            self._agg.add_app_data(app)
            app = default_enrich(app)
            if self._app_specific_obj:
                if self._app_specific_obj.is_matching_app(app):
                    app = self._app_specific_obj.enrich(app)

            # save
            self._save_obj.save_app(app)
            self.retry_attempts_remained = self.retry_attempts  # reset retries after successful attempt
            return True

        except Exception as e:
            self._handle_processing_exception_(e, 'raw', app.get('id', 'unknown'))
            # if skip_exceptions is set to False, the code will exit by this point
            if isinstance(e, JSONDecodeError) and str(e).startswith("Expecting value:"):
                # Error due to Spark History is in a bad state
                logger.error(f"Spark history responded with a wrong format. "
                             f"Please, reboot Spark History server.")
                if self.retry_attempts_remained > 0:
                    # wait and retry
                    logger.warning(f" Will retry in {self.retry_sleep_seconds} s. "
                                   f"{self.retry_attempts_remained} retries remained")
                    time.sleep(self.retry_sleep_seconds)
                    self.retry_attempts_remained -= 1
                    return self._process_raw(app)
                else:
                    # no retry attempts left
                    logger.error("No retry attempts left")
                    raise e

            return False

    def _process_aggs(self, app):
        # get aggregations
        try:
            if self._app_specific_obj:
                if self._app_specific_obj.is_matching_app(app):
                    app = self._app_specific_obj.aggregate(app)

            aggs = flatten_app(app)

            # save aggregations
            for agg in aggs:
                if self._app_specific_obj:
                    if self._app_specific_obj.is_matching_app(app):
                        agg = self._app_specific_obj.post_aggregate(agg)
                self._save_obj.save_agg(agg)
            return True
        except Exception as e:
            self._handle_processing_exception_(e, 'aggregations', app.get('id', 'unknown'))
            return False

    def _process_app(self, app):
        app['history_host'] = self._history_host
        app['spot'] = {
            'time_processed': datetime.now(tz=timezone.utc),
            'history_host': self._history_host
        }
        success = self._process_raw(app)
        if success:  # if no exceptions while getting data
            self._process_aggs(app)

    def _get_next_completed_app(self, min_end_date=None, max_end_date=None):
        try:
            apps = self._agg.next_app(min_end_date=min_end_date,
                                      max_end_date=max_end_date,
                                      app_status='completed')
            for app in apps:
                self.retry_attempts_remained = self.retry_attempts  # reset retries after successful attempt
                yield app
        except Exception as e:
            self._handle_processing_exception_(e, 'listing', 'n/a')
            # if skip_exceptions is set to False, the code will exit by this point
            if isinstance(e, JSONDecodeError) and str(e).startswith("Expecting value:"):
                # Error due to Spark History is in a bad state
                logger.error(f"Spark history responded with a wrong format. "
                             f"Please, reboot Spark History server.")
                if self.retry_attempts_remained > 0:
                    # wait and retry
                    logger.warning(f" Will retry in {self.retry_sleep_seconds} s. "
                                   f"{self.retry_attempts_remained} retries remaining")
                    time.sleep(self.retry_sleep_seconds)
                    self.retry_attempts_remained -= 1
                    return self._get_next_completed_app(min_end_date=min_end_date, max_end_date=max_end_date)
                else:
                    logger.error("No retry attempts left")
                    raise e

            # other unknown exception
            raise e

    def process_runs_within_time_step(self, start_time, finish_time):
        """Processes new runs completed within the given time step.
        First, gets a list of ids from the database of already processed runs which completed within the time step.
        Second, gets the list of all runs completed within the time step from Spark history.
        Finally, iterates over the list from Spark History,
        checks whether each id already exists in the list from the database
        and, if not, processes the run.
        This approach is necessary as the completed runs do not always appear in chronological order.
        The time step should be selected in such a way that it does not contain more than 10,000 jobs.
        :param start_time: start time of the time step
        :param finish_time: end time of the time step
        :return: number of new runs processed
        """
        processing_start = datetime.now(tz=timezone.utc)
        logger.debug(
            f"Processing completed apps within the time step from {start_time} to {finish_time}")
        apps = self._get_next_completed_app(min_end_date=start_time,
                                  max_end_date=finish_time)
        tabu_ids = self._save_obj.get_set_of_processed_ids(start_time, finish_time)

        apps_counter = 0
        matched_counter = 0
        new_counter = 0

        for app in apps:
            apps_counter += 1
            app_id = app.get('id')
            app_name = app.get('name')
            if self._name_filter_func(app_name):
                matched_counter += 1
                if app_id not in tabu_ids:
                    new_counter += 1
                    self._process_app(app)
                    if new_counter % 20 == 0:
                        self.log_processing_stats(processing_start, new_counter)
                else:
                    logger.debug(f"skipping app already processed before: {app_id} ")

        logger.debug(f"Time step {start_time} to {finish_time} processed. "
                    f"Applications total:{apps_counter}, matched: {matched_counter}, new: {new_counter}")
        if new_counter > 0:
            self.log_processing_stats(processing_start, new_counter)
        return new_counter

    def process_window_by_steps(self, window_start, window_end):
        """Processes new runs which completed within the larger time window and are not yet present in the database.
        The time window can be large (e.g. retention period of the Spark History)
        as it is sliced into smaller time steps which are processed iteratively.
        Previously processed runs are identified by id and skipped.
        :param window_start: minimum completion time of Spark apps
        :param window_end: maximum completion time of Spark apps
        :return: number of new apps processed
        """
        if not window_start < window_end:
            logger.warning(f"Time window error. window_start: {window_start}, window_end: {window_end}" )
            return 0

        delta = timedelta(seconds=self.time_step_seconds)
        step_start = window_start
        processing_start = datetime.now(tz=timezone.utc)
        new_counter = 0
        step_counter = 0
        logger.info(f"Starting processing of time window. window_start: {window_start}, window_end: {window_end}" )
        while step_start < window_end:
            step_counter += 1
            step_end = step_start + delta
            if step_end > window_end:
                step_end = window_end
            new_runs_iteration_counter = self.process_runs_within_time_step(step_start, step_end)
            logger.debug(f"Step {step_counter}, {step_start} - {step_start} , new runs: {new_runs_iteration_counter}")
            new_counter += new_runs_iteration_counter
            step_start = step_end
        logger.info(f"Time window {window_start} - {window_end}. processed. New runs: {new_counter}")
        self.log_processing_stats(processing_start, new_counter)
        return new_counter

    def process_all_new_runs(self):
        """Processes all new runs from Spark History server.
        The method processes completion time interval from lookback_hours to (time_now - lookback_delta).
        The interval is split into steps.
        The list of jobs completed within each time step is pulled from both Spark History and database, then compared.
        The new runs from Spark History which are not present in the database are then processed further and stored.
        This method provides more reliable retrieval of data from Spark History (especially for clusters under heavy load)
        but makes more API calls to both the database and the History server.

        :return: number of new processed runs
        """
        time_now = datetime.now(tz=timezone.utc)
        # interval to look back
        min_completion_time = time_now - self.lookback_delta

        # give Spark History time to process the most recent jobs
        timeout_delta = timedelta(seconds=self.completion_timeout_seconds)
        max_completion_time = time_now - timeout_delta

        new_counter = self.process_window_by_steps(min_completion_time, max_completion_time)
        return new_counter

    # Deprecated
    def process_new_runs_since_latest_processed(self):
        """ Processes new runs from Spark History server which completed later than _latest_seen_date.
        The _latest_seen_date is either set upon class initialization,
        stored from the previous call of the method
        or pulled from the database of previously processed runs.
        This method provides faster processing but is based on the assumption that the new runs appear in
        Spark History strictly in the order of their completion, which is not necessarily true,
        especially for clusters under heavy load.

        :return: list of new runs
        """
        processing_start = datetime.now(tz=timezone.utc)
        max_end_date = processing_start - timedelta(seconds=self.completion_timeout_seconds)

        logger.info(
            f"Fetching new apps, completed from {self._latest_seen_date} to {max_end_date}")
        apps = self._get_next_completed_app(min_end_date=self._latest_seen_date,
                                  max_end_date=max_end_date)
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
                    if matched_counter % 20 == 0:
                        self.log_processing_stats(processing_start, matched_counter)

        self._previous_tabu_set = self._new_tabu_set

        logger.info(f"Iteration finished. New apps: {new_counter} "
                    f"matching apps : {matched_counter}")
        if matched_counter > 0:
            self.log_processing_stats(processing_start, matched_counter)
        logger.debug(f"tabu_set: {self._previous_tabu_set}"
                     f" last date: {self._latest_seen_date}")
        return new_counter

    # Deprecated
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

    def log_processing_stats(self, start_time, runs_number):
        delta_seconds = (datetime.now(tz=timezone.utc) -
                         start_time).total_seconds()
        per_hour = runs_number * 3600 / delta_seconds
        logger.info(f"processed {runs_number} runs "
                    f"in {delta_seconds} seconds "
                    f"average rate: {per_hour} runs/hour")
        self._save_obj.log_indexes_stats()


def main():
    logger.info(f'Starting crawler')
    cmd_args = CrawlerArgs().parse_args()
    conf = SpotConfig()

    if conf.menas_api_url is not None:
        logger.info(f"adding Menas aggregator, api url {conf.menas_api_url}")
        menas_default_tzinfo = tz.gettz(name=conf.menas_default_timezone)
        if menas_default_tzinfo is None:
            menas_default_tzinfo = tz.tzutc()
        menas_ag = MenasAggregator(conf.menas_api_url,
                                   conf.menas_username,
                                   conf.menas_password,
                                   ssl_path=conf.menas_ssl_path,
                                   default_tzinfo=menas_default_tzinfo)
    else:
        menas_ag = None
        logger.info(
            'Menas integration disabled as api url not provided in config')

    elastic = Elastic(conf)

    # find starting end date and list of seen apps
    last_seen_end_date, seen_ids = elastic.get_latest_time_ids()
    logger.debug(f'Latest seen app in the db is from: {last_seen_end_date}')
    logger.debug(
        f'Latest stored end date: {last_seen_end_date} seen apps: {seen_ids}')

    logger.debug(f'param_end_date: {cmd_args.min_end_date}')

    if (cmd_args.min_end_date is not None) and\
        ((last_seen_end_date is None) or
         (last_seen_end_date < cmd_args.min_end_date)):
        last_seen_end_date = cmd_args.min_end_date
        seen_ids = dict()


    crawler = Crawler(conf.spark_history_url,
                      ssl_path=conf.history_ssl_path,
                      app_specific_obj=menas_ag,
                      save_obj=elastic,
                      last_date=last_seen_end_date,
                      seen_app_ids=seen_ids,
                      skip_exceptions=conf.crawler_skip_exceptions,
                      completion_timeout_seconds=conf.completion_timeout_seconds,
                      crawler_method=conf.crawler_method,
                      lookback_hours=conf.lookback_hours,
                      time_step_seconds=conf.time_step_seconds,
                      retry_sleep_seconds=conf.retry_sleep_seconds,
                      retry_attempts=conf.retry_attempts
                      )

    sleep_seconds = conf.crawler_sleep_seconds

    while True:
        crawler.process_new_runs()
        elastic.log_indexes_stats()
        time.sleep(sleep_seconds)


if __name__ == '__main__':
    main()
