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

from spot.enceladus.menas_api import MenasApi
import spot.enceladus.classification as clsf
from spot.crawler.commons import cast_string_to_value
import spot.utils.setup_logger


logger = logging.getLogger(__name__)


def _match_values(left, right):
    if left == right:
        return True
    else:
        logger.debug(f"value mismatch {left} != {right}")
        return False


def _match_run(run, app_id, clfsion):
    dataset_name = run.get('dataset')
    dataset_version = run.get('datasetVersion')
    metadata = run.get('controlMeasure').get('metadata')
    # info_date = metadata.get('informationDate')
    info_version = metadata.get('version')
    add_info = metadata.get('additionalInfo')
    app_version = add_info.get('std_enceladus_version')

    run_app_id = ''
    if clfsion.get('type') == 'standardization&conformance':
        run_app_id = add_info.get('std_application_id')
    if clfsion.get('type') == 'standardization':
        run_app_id = add_info.get('std_application_id')
    elif clfsion.get('type') == 'conformance':
        run_app_id = add_info.get('conform_application_id')

    match = _match_values(dataset_name, clfsion.get('dataset')) \
            and _match_values(dataset_version, clfsion.get('dataset_version')) \
            and _match_values(info_version, clfsion.get('info_version')) \
            and _match_values(app_version, clfsion.get('app_version')) \
            and _match_values(run_app_id, app_id)
    # and _match_values(info_date, clfsion.get('info_date')) \

    if not match:
        logger.warning(f"run uniqueId:{run.get('uniqueId')} "
                       f"does not match Spark App {app_id}")

    return match


class MenasAggregator:

    def __init__(self, api_base_url, username, password):
        logger.debug('starting menas aggregator')
        self.menas_api = MenasApi(api_base_url, username, password)

    @staticmethod
    def cast_run_data(run):
        additional_info = run['controlMeasure']['metadata']['additionalInfo']
        for key, value in additional_info.items():
            additional_info[key] = cast_string_to_value(value)
        return run

    def get_runs(self, app_id, clfsion):
        runs = self.menas_api.get_runs_by_spark_id(app_id)
        if not runs:
            logger.warning(f"Run document for {app_id} not found")
            return []
        for run in runs:
            if not _match_run(run, app_id, clfsion):
                runs.remove(run)
            else:
                run = self.cast_run_data(run)
        if len(runs) > 1:
            logger.warning(f"Multiple run documents exist for {app_id}")
        elif len(runs) == 0:
            logger.warning(f"No matching run documents exist for {app_id}")
            return []
        return runs

    @staticmethod
    def is_matching_app(app):
        app_name = app.get('name')
        return clsf.is_enceladus_app(app_name)

    def enrich(self, app):
        app_id = app.get('id')
        app_name = app.get('name')
        data = {}
        clfsion = clsf.get_classification(app_name)
        data['classification'] = clfsion
        data['tag'] = clsf.get_tag(clfsion)
        app['app_specific_data'] = data

        # get run
        runs = self.get_runs(app_id, clfsion)
        attempts = app.get('attempts')
        if len(runs) != len(attempts):
            logger.error(f'{app_id} {app_name} runs and attempts mismatch')

        # we assume that Enceladus runs and spark attempts are sorted in opposite orders!
        for i in range(len(attempts)):
            run = None
            if i < len(runs):
                run = runs[-i]
            attempts[i]['app_specific_data'] = {'enceladus_run': run}

        # get dataset
        #dataset = self.menas_api.get_dataset(clfsion.get('dataset'), clfsion.get('dataset_version'))
        #data['dataset'] = dataset

        # get schema
        #schema_name = dataset.get('schemaName')
        #schema_version = dataset.get('schemaVersion')
        #schema = self.menas_api.get_schema(schema_name, schema_version)
        #data['schema'] = schema
        return app

    @staticmethod
    def aggregate(app):
        logger.debug(f"{app['name']} {app['id']}")
        for attempt in app.get('attempts'):
            run = attempt.get('app_specific_data', None).get('enceladus_run', None)
            if run is not None:
                run.get('controlMeasure', None).pop('checkpoints', None)  # remove checkpoints array from run
        return app
