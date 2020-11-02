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

import math
import logging

import spot.utils.setup_logger


logger = logging.getLogger(__name__)

HDFS_block_size = (128 * 1024 * 1024)

size_units = {
    'B': 1,
    'K': 1024,
    'M': 1024 ** 2,
    'G': 1024 ** 3,
    'T': 1024 ** 4,
    'P': 1024 ** 5
}

size_units2 = {
    'k': 1024,
    'm': 1024 ** 2,
    'g': 1024 ** 3,
    't': 1024 ** 4,
    'p': 1024 ** 5,

    'kb': 1024,
    'mb': 1024 ** 2,
    'gb': 1024 ** 3,
    'tb': 1024 ** 4,
    'pb': 1024 ** 5,

    'b': 1,
}

time_units = {
    'ms': 1,
    's': 1000,
    'm': 60 * 1000,
    'min': 60 * 1000,
    'h': 60 * 60 * 1000,
    'd': 24 * 60 * 60 * 1000
}


def get_last_attempt(app):
    # we assume the attempts are sorted in reversed chronological order
    return app.get('attempts')[0]


def bytes_to_hdfs_block(size_bytes):
    return math.ceil(size_bytes / HDFS_block_size)


#def parse_to_bytes(size_str):
#    # see units at https://spark.apache.org/docs/latest/configuration.html#spark-properties
#    stripped = size_str.strip().upper()
#    value = stripped[:-1]
#    units = stripped[-1]
#    if value.isdigit() and (units in size_units):
#        return int(value) * size_units[units]
#    else:
#        logger.warning(f'Failed to parse string {size_str} to bytes')
#        return None

def parse_to_bytes(size_str):
    """ Parse size string with units into bytes, default unit is bytes when not specified."""
    # see units at https://spark.apache.org/docs/latest/configuration.html#spark-properties
    stripped = size_str.strip().lower()
    multiplier = 1
    for unit in size_units2.keys():
        if stripped.endswith(unit):
            stripped = stripped[:-len(unit)]
            multiplier = size_units2[unit]
            break
    if stripped.isdigit():
        return int(stripped) * multiplier
    else:
        logger.warning(f'Failed to parse string {size_str} to bytes')
        return None


def parse_to_ms(time_str):
    """ Parse time string with units into ms, default unit is seconds when not specified."""
    # see units at https://spark.apache.org/docs/latest/configuration.html#spark-properties
    stripped = time_str.strip().lower()
    multiplier = 1000
    for unit in time_units.keys():
        if stripped.endswith(unit):
            stripped = stripped[:-len(unit)]
            multiplier = time_units[unit]
            break
    if stripped.isdigit():
        return int(stripped) * multiplier
    else:
        logger.warning(f'Failed to parse string {time_str} to milliseconds')
        return None




def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'k', 'M', 'G', 'T']:
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


def bytes_to_gb(size_bytes):
    return size_bytes / (1024 * 1024 * 1024)


def string_to_bool(s):
    return s.lower() in ['true', '1', 'y', 'yes']
