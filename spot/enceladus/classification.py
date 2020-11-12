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

import datetime
from spot.crawler.commons import parse_date

info_date_formats = ['%d-%m-%Y', '%Y-%m-%d']


def is_enceladus_app(name):
    return name.startswith('Enceladus') or _old_is_enceladus_app(name)


def get_classification(name):
    if name.startswith('Enceladus'): # new naming convention
        values = name.split(' ')
        classification = {
            'project': 'enceladus',
            'app': values[0].lower(),
            'type': values[1].lower(),
            'app_version': values[2],
            'dataset': values[3],
            'dataset_version': int(values[4]) if values[4].isdigit() else values[4],
            'info_date': values[5],
            'info_date_casted': parse_date(values[5], formats=info_date_formats),
            'info_version': int(values[6]) if values[6].isdigit() else values[6]
        }
        return classification
    else: # old naming convention
        return _old_get_classification(name)


def _old_is_standardization(name):
    spl = name.split(' ')
    return name.startswith('Standardisation ') and len(spl) == 6


def _old_is_conformance(name):
    spl = name.split(' ')
    return name.startswith('Dynamic Conformance ') and len(spl) == 7


def _old_is_enceladus_app(name):
    return _old_is_standardization(name) or _old_is_conformance(name)


def _old_get_classification(name):
    values = name.split(' ')
    if _old_is_standardization(name):
        app_type = 'standardization'
    if _old_is_conformance(name):
        app_type = 'conformance'
        values.pop(0)
    if app_type is not None:
        classification = {
            'project': 'enceladus',
            'app': 'enceladus',
            'type': app_type,
            'app_version': values[1],
            'dataset': values[2],
            'dataset_version': int(values[3]) if values[3].isdigit() else values[3],
            'info_date': values[4],
            'info_date_casted': parse_date(values[4], formats=info_date_formats),
            'info_version': int(values[5]) if values[5].isdigit() else values[5]
        }
    return classification


def get_tag(classification):
    tag = f'{classification.get("type")}_' \
        f'{classification.get("app_version")}_' \
        f'{classification.get("dataset")}_' \
        f'{classification.get("dataset_version")}'
    return tag
