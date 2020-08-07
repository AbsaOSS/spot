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

import argparse
import datetime


datetime_format = "%Y-%m-%dT%H:%M:%S"


class CrawlerArgs:

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--min_end_date",
                                 help=f"Retrieve apps completed after {datetime_format.replace('%', '%%')}",
                                 type=lambda s: datetime.datetime.strptime(s, datetime_format))
        self.parser.add_argument("--config_path",
                                 help=f"Absolute path to config.ini configuration file, \
                                    e.g. '/opt/config.ini'")

    def parse_args(self, argv=None):
        return self.parser.parse_args(argv)
