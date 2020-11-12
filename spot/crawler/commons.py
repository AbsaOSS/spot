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
from datetime import datetime
import logging

import spot.utils.setup_logger


logger = logging.getLogger(__name__)

HDFS_block_size = (128 * 1024 * 1024)

size_units = {
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

date_formats = ["%d-%m-%Y %H:%M:%S %z", "%d-%m-%Y %H:%M:%S"]


def parse_date(text, formats=date_formats):
    """ Try parsing string to date using list of formats"""
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    logger.warning(f"No valid date format found for {text}")
    return


def get_last_attempt(app):
    # we assume the attempts are sorted in reversed chronological order
    return app.get('attempts')[0]


def bytes_to_hdfs_block(size_bytes):
    return math.ceil(size_bytes / HDFS_block_size)


def parse_to_bytes(size_str):
    """ Parse size string with units into bytes, default unit is bytes when not specified."""
    # see units at https://spark.apache.org/docs/latest/configuration.html#spark-properties
    stripped = size_str.strip().lower()
    multiplier = 1
    for unit in size_units.keys():
        if stripped.endswith(unit):
            stripped = stripped[:-len(unit)]
            multiplier = size_units[unit]
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
        return


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
    lower = s.lower()
    if lower in ['true', '1', 'y', 'yes']:
        return True
    if lower in ['false', '0', 'n', 'no']:
        return False
    logger.warning(f"Failed to parse string to bool: {s}")
    return


def get_attribute(doc, path_list):
    if doc is None:
        return

    if path_list:  # path_list not empty
        next_attribute = path_list.pop(0)
        if isinstance(doc, dict) and next_attribute in doc:
            return get_attribute(doc[next_attribute], path_list)
        else:
            return
    else:  # path_list is empty
        return doc


