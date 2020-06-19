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

from spot.enceladus import menas_api
from spot.utils import HDFSutils
from spot.enceladus.setter import CmndArgs


def get_input_path(dir, info_date, info_version):
    return dir + '/' + info_date.replace('-', '/') + '/v' + str(info_version)


def main():
    print("Starting Optimizer")
    cmd_args = CmndArgs.CmndArgs().parse_args()
    config = config.ConfigProvider()
    menas = menas_api.MenasApi(config)
    dataset_doc = menas.get_dataset(cmd_args.dataset_name, cmd_args.dataset_version)
    print(dataset_doc)
    if cmd_args.app_type == 'std':
        inputPath = get_input_path(dataset_doc['hdfsPath'], cmd_args.info_date, cmd_args.info_version)
        print(inputPath)
        hdfsUtil = HDFSutils.HDFSutils(config)
        bytes, blocks = hdfsUtil.get_input_size(inputPath)
    else:
        print('unsupported app type: {}'.format(cmd_args.app_type))


if __name__ == '__main__':
    main()





