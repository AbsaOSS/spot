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


class CmndArgs:

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("app_type", help="Enceladus app type: [std/cnfrm]", type=str)
        self.parser.add_argument("dataset_name", help="name of the dataset", type=str)
        self.parser.add_argument("dataset_version", help="version of the dataset", type=int)
        self.parser.add_argument("info_date", help="information date yyyy-mm-dd", type=str)
        self.parser.add_argument("info_version", help="information version", type=int)

    def parse_args(self, argv=None):
        return self.parser.parse_args(argv)