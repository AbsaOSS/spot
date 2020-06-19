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

import subprocess

class HDFSutils:

    def __init__(self, config):
        self.hdfs_block_size = config.hdfs_block_size

    def get_input_size(self, dir_path):
        print('Checking input dir: {}'.format(dir_path))
        filelist_process = subprocess.run(['hdfs', 'dfs', '-ls', '-C', dir_path],
                                          check=True,
                                          stdout=subprocess.PIPE,
                                          universal_newlines=True)
        raw_filelist = filelist_process.stdout.split('\n')
        file_paths = []
        for path in raw_filelist:
            filename = path.split('/')[-1]
            print(filename)
            if (len(path) > 0) and (not filename[0] in ['_', '.']):
                file_paths.append(path)

        size_bytes = 0
        blocks = 0
        for file in file_paths:
            hdfs_stats_process = subprocess.run(['hdfs', 'fsck', file, '-files'],
                                                check=True,
                                                stdout=subprocess.PIPE,
                                                universal_newlines=True)
            stats = hdfs_stats_process.stdout.split('\n')[1].split(' ')
            file_bytes = int(stats[1])
            size_bytes += file_bytes
            file_blocks = int(stats[5])
            blocks += file_blocks
            print("{} blocks, {} bytes {}".format(file_blocks, file_bytes, file))

        print('Input totals: {} bytes, {} HDFS blocks'.format(size_bytes, blocks))
        return size_bytes, blocks

