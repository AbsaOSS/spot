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
import re

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
info_date_formats = ['%d-%m-%Y', '%Y-%m-%d']


def parse_date(text, formats=date_formats):
    """ Try parsing string to date using list of formats"""
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    logger.warning(f"No valid date format found for {text}")
    return


def isint(in_str):
    return re.match(r"^[-+]?\d+$", in_str) is not None


def isfloat(in_str):
    return re.match(r"^[+-]?\d(>?\.\d+)?$", in_str) is not None


def cast_to_number(in_str):
    if isint(in_str):
        return int(in_str)
    elif isfloat(in_str):
        return float(in_str)
    return None


def parse_percentage(text):
    """Parse a string representing percentage (e.g. '110.01 %') into a float ratio

    :param text: percentage as a string ending with ' %'
    :return: float presenting the corresponding ratio. None if the format is wrong
    """
    if text.endswith(' %'):
        str_val = text[:-2]
        try:
            percent = float(str_val)
            ratio = percent / 100.0
            return ratio
        except ValueError:
            logger.warning(f"Failed to parse percentage string to a float ratio: {str_val}")
            return
    return


def parse_command_line_args(text):
    """Parses string of command line args into a dict.
    Only basic parsing is supported at the moment.
    Each parameter starting with '--' is transformed into a key,
    and everything which follows is taken as a string value.
    Each value has a prefix ' ' in order to avoid schema inference in Elasticsearch.
    This is done, because Elasticsearch may wrongly interpret some of the values which would result in errors.

    :param text: string containing all command line args
    :return: dictionary {arg_name: ' 'string_value}
    """
    result = {}
    for param in text.split('--'):
        if param != '':
            words = param.strip(' ').split(' ')
            key = words[0]
            # Below we start with space to avoid auto schema inference in Elasticsearch,
            # so that all params are string
            value = ' ' + ''.join(words[1:])
            result[key] = value
    return result


def get_last_attempt(app):
    # we assume the attempts are sorted in reversed chronological order
    return app.get('attempts')[0]


def bytes_to_hdfs_block(size_bytes):
    return math.ceil(size_bytes / HDFS_block_size)


def parse_to_bytes(size_str, default_multiplier=1):
    """ Parse size string with units into bytes, default unit is bytes when not specified."""
    # see units at https://spark.apache.org/docs/latest/configuration.html#spark-properties
    stripped = size_str.strip().lower()
    multiplier = default_multiplier
    for unit in size_units.keys():
        if stripped.endswith(unit):
            stripped = stripped[:-len(unit)]
            multiplier = size_units[unit]
            break
    as_number = cast_to_number(stripped)
    if as_number is not None:
        return as_number * multiplier
    logger.warning(f'Failed to parse string {size_str} to bytes')
    return


# 1 MiB = 1024 * 1024 bytes = 1048576 bytes
# see units used in Spark properties at https://spark.apache.org/docs/latest/configuration.html#spark-properties
def parse_to_bytes_default_MiB(size_str):
    return parse_to_bytes(size_str, default_multiplier=1048576)

# 1 KiB = 1024 bytes
# see units used in Spark properties at https://spark.apache.org/docs/latest/configuration.html#spark-properties
def parse_to_bytes_default_KiB(size_str):
    return parse_to_bytes(size_str, default_multiplier=1024)


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
    as_number = cast_to_number(stripped)
    if as_number is not None:
        return as_number * multiplier

    logger.warning(f'Failed to parse string {time_str} to milliseconds')
    return


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'k', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)


def cast_string_to_value(str_val):
    as_number = cast_to_number(str_val)
    if as_number is not None:
        return as_number
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


def num_elements(x):
    if isinstance(x, dict):
        return sum([num_elements(_x) for _x in x.values()])
    else:
        return 1


def get_default_classification(name):
    classification = {
        'app': name,
        'type': name,
    }
    values = re.split(r'[ ;,.\-\%\_]', name)
    i = 1
    for val in values:
        classification[i] = val
        i += 1
    return classification


def get_default_tag(classification):
    tag = classification.get('app', None)
    return tag


def default_enrich(app):
    app_name = app.get('name')
    data = {}
    clfsion = get_default_classification(app_name)
    data['classification'] = clfsion
    data['tag'] = get_default_tag(clfsion)
    app['app_specific_data'] = data
    return app
