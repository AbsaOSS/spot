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


def _is_standardization(name):
    spl = name.split(' ')
    return name.startswith('Standardisation ') and len(spl) == 6


def _is_conformance(name):
    spl = name.split(' ')
    return name.startswith('Dynamic Conformance ') and len(spl) == 7


def is_enceladus_app(name):
    return _is_standardization(name) or _is_conformance(name)


def get_classification(name):
    values = name.split(' ')
    if _is_standardization(name):
        app_type = 'standardization'
    if _is_conformance(name):
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
            'info_version': int(values[5]) if values[5].isdigit() else values[5]
        }
    return classification


def get_tag(classification):
    tag = f'{classification.get("type")}_' \
        f'{classification.get("app_version")}_' \
        f'{classification.get("dataset")}_' \
        f'{classification.get("dataset_version")}'
    return tag
