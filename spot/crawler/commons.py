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

import spot.utils.setup_logger
import math

logger = logging.getLogger(__name__)

HDFS_block_size = 128 * 1024 * 1024

units_dict = {
    'B': 1,
    'K': 1024,
    'M': 1024 ** 2,
    'G': 1024 ** 3,
    'T': 1024 ** 4
}

def get_last_attempt(app):
    # we assume the attemts are sorted in reversed chronological order
    return app.get('attempts')[0]

    # depricated
    last_attempt = None
    for attempt in app.get('attempts'):
        if (last_attempt is None) or \
                (int(attempt.get('attemptId')) > \
                 int(last_attempt.get('attemptId'))):
            last_attempt = attempt
    return last_attempt


def bytes_to_hdfs_block(bytes):
    return math.ceil(bytes / HDFS_block_size)


def parse_to_bytes(size):
    stripped = size.strip().upper()
    value = stripped[:-1]
    units = stripped[-1:]
    if value.isdigit() and (units in units_dict):
        return int(value) * units_dict[units]
    else:
        logger.warning(f'Failed to parse string {size} to bytes')
        return None


def sizeof_fmt(num, suffix='B'):
    for unit in ['','k','M','G','T']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)


def cast_string_to_value(str_val):
    if str_val.isdigit():
        try:
            return int(str_val)
        except ValueError:
            return float(str_val)
    return str_val

