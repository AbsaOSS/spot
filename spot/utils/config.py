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
    def crawler_sleep_seconds(self):
        str_val = self.get_property('CRAWLER', 'sleep_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 60

    @property
    def crawler_skip_exceptions(self):
        if self.get_boolean('CRAWLER', 'skip_exceptions'):
            return True
        return False

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

    # Additional auth
    @property
    def auth_type(self):
        return self.get_property('AUTH', 'auth_type')

    @property
    def oauth_username(self):
        return self.get_property('AUTH', 'username')

    @property
    def oauth_password(self):
        return self.get_property('AUTH', 'password')

    @property
    def cognito_region(self):
        return self.get_property('AUTH', 'cognito_region')

    @property
    def elasticsearch_region(self):
        return self.get_property('AUTH', 'elasticsearch_region')

    @property
    def client_id(self):
        return self.get_property('AUTH', 'client_id')

    @property
    def client_secret(self):
        return self.get_property('AUTH', 'client_secret')

    @property
    def aws_account_id(self):
        return self.get_property('AUTH', 'aws_account_id')

    @property
    def user_pool_id(self):
        return self.get_property('AUTH', 'user_pool_id')

    @property
    def identity_pool_id(self):
        return self.get_property('AUTH', 'identity_pool_id')

    @property
    def elasticsearch_role_name(self):
        return self.get_property('AUTH', 'elasticsearch_role_name')
