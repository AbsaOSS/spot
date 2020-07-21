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

import configparser
import logging
import urllib.parse

import spot.utils.setup_logger


logger = logging.getLogger(__name__)


class Config(object):
    _default_config_path = '../../config.ini'

    def __init__(self, config_path=_default_config_path):
        logger.debug(f"Reading configuration {config_path}")
        config = configparser.ConfigParser()
        config.read(config_path)
        self._config = config

    def get_property(self, *property_path):
        prop = self._config.get(*property_path, fallback=None)
        if prop is None:  # we don't want KeyError
            logger.error(f"property {property_path} not found")
            return None  # just return None if not found
        return prop


class SpotConfig(Config):

    @property
    def spark_history_url(self):
        return self.get_property('SPARK_HISTORY','api_base_url')

    @property
    def menas_api_url(self):
        return self.get_property('MENAS', 'api_base_url')

    @property
    def menas_username(self):
        return self.get_property('MENAS', 'username')

    @property
    def menas_password(self):
        return self.get_property('MENAS', 'password')

    @property
    def crawler_sleep_seconds(self):
        str_val = self.get_property('CRAWLER', 'sleep_seconds')
        if str_val.isdigit():
            return int(str_val)
        return 60

    @property
    def elastic_raw_index(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'raw_index')

    @property
    def elastic_agg_index(self):
        return self.get_property('SPOT_ELASTICSEARCH', 'agg_index')
