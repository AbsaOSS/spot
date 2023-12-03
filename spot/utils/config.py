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

import os
import configparser
import logging

import spot.utils.setup_logger


logger = logging.getLogger(__name__)


class Config(object):
    _default_config_path = '../config/config.ini'

    def __init__(self, config_path=_default_config_path):
        if config_path == Config._default_config_path:
            config_path = os.path.abspath(os.path.join(
                os.path.dirname(__file__), config_path))
            logger.debug(f"Configuration file is default: {config_path}")
        else:
            logger.debug(f"Configuration file is provided: {config_path}")
        logger.debug(f"Reading configuration {config_path}")

        config = configparser.ConfigParser(interpolation=None)
        config.read(config_path)
        self._config = config

    def get_property(self, *property_path):
        prop = self._config.get(*property_path, fallback=None)
        if prop is None:
            logger.warning(f"property {property_path} not found")
            return None
        return prop.strip("\"\'")

    def get_boolean(self, *property_path):
        boolean = self._config.getboolean(*property_path, fallback=None)
        if boolean is None:
            logger.warning(f"property {property_path} not found")
        return boolean


class SpotConfig(Config):

    @property
    def spark_history_url(self):
        return self.get_property('SPARK_HISTORY', 'api_base_url')

    @property
    def history_ssl_path(self):
        return self.get_property('SPARK_HISTORY', 'ssl_path')

    @property
    def crawler_sleep_seconds(self):
        str_val = self.get_property('CRAWLER', 'sleep_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 60

    @property
    def completion_timeout_seconds(self):
        str_val = self.get_property('CRAWLER', 'completion_timeout_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 300

    @property
    def crawler_method(self):
        crawler_method = self.get_property('CRAWLER', 'crawler_method')
        if crawler_method is None:
            crawler_method = 'all'
        return crawler_method

    @property
    def lookback_hours(self):
        str_val = self.get_property('CRAWLER', 'lookback_hours')
        if str_val.isdigit():
            return int(str_val)
        return 24

    @property
    def time_step_seconds(self):
        str_val = self.get_property('CRAWLER', 'time_step_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 3600

    @property
    def crawler_skip_exceptions(self):
        if self.get_boolean('CRAWLER', 'skip_exceptions'):
            return True
        return False

    @property
    def crawler_batch(self):
        if self.get_boolean('CRAWLER', 'batch'):
            return True
        return False

    @property
    def retry_sleep_seconds(self):
        str_val = self.get_property('CRAWLER', 'retry_sleep_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 900

    @property
    def retry_attempts(self):
        str_val = self.get_property('CRAWLER', 'retry_attempts')
        if str_val.isdigit():
            return int(str_val)
        return 10

    @property
    def elasticsearch_url(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'elasticsearch_url')

    @property
    def elastic_username(self):
        username = self.get_property('SPOT_ELASTICSEARCH', 'username')
        if username is None:
            username = ''
        return username

    @property
    def elastic_password(self):
        password = self.get_property('SPOT_ELASTICSEARCH', 'password')
        if password is None:
            password = ''
        return password

    @property
    def elastic_raw_index(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'raw_index')

    @property
    def elastic_agg_index(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'agg_index')

    @property
    def elastic_err_index(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'err_index')

    @property
    def auth_type(self):
        val = self.get_property('SPOT_ELASTICSEARCH', 'auth_type')
        if val:
            return val.lower()
        return val

    @property
    def oauth_username(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'username')

    @property
    def oauth_password(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'password')

    @property
    def cognito_region(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'cognito_region')

    @property
    def elasticsearch_region(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'elasticsearch_region')

    @property
    def client_id(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'client_id')

    @property
    def client_secret(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'client_secret')

    @property
    def aws_account_id(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'aws_account_id')

    @property
    def user_pool_id(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'user_pool_id')

    @property
    def identity_pool_id(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'identity_pool_id')

    @property
    def elasticsearch_role_name(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'elasticsearch_role_name')

    @property
    def elasticsearch_limit_of_fields_increment(self):
        str_val = self.get_property('SPOT_ELASTICSEARCH', 'limit_of_fields_increment')
        if str_val.isdigit():
            return int(str_val)
        return 100

    @property
    def menas_api_url(self):
        return self.get_property('MENAS', 'api_base_url')

    @property
    def menas_ssl_path(self):
        return self.get_property('MENAS', 'menas_ssl_path')

    @property
    def menas_username(self):
        return self.get_property('MENAS', 'username')

    @property
    def menas_password(self):
        return self.get_property('MENAS', 'password')

    @property
    def menas_default_timezone(self):
        return self.get_property('MENAS', 'default_timezone')


    @property
    def yarn_api_base_url(self):
        return self.get_property('YARN', 'yarn_api_base_url')

    @property
    def yarn_clust_index(self):
        return self.get_property('YARN', 'yarn_clust_index')

    @property
    def yarn_apps_index(self):
        return self.get_property('YARN', 'yarn_apps_index')

    @property
    def yarn_scheduler_index(self):
        return self.get_property('YARN', 'yarn_scheduler_index')

    @property
    def yarn_sleep_seconds(self):
        str_val = self.get_property('YARN', 'yarn_sleep_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 60

