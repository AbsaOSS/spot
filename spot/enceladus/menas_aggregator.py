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
from dateutil import tz

from spot.enceladus.menas_api import MenasApi
import spot.enceladus.classification as clsf
from spot.crawler.commons import cast_string_to_value, get_attribute, bytes_to_hdfs_block, parse_to_bytes, \
    parse_to_bytes_default_MiB, parse_percentage, parse_command_line_args, parse_date
import spot.utils.setup_logger


logger = logging.getLogger(__name__)

date_formats = ["%d-%m-%Y %H:%M:%S %z", "%Y-%m-%d %H:%M:%S %z", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]


_cast_additionalInfo_dict = {

    'conform_executor_memory_overhead': parse_to_bytes_default_MiB,
    "std_driver_memory_overhead": parse_to_bytes_default_MiB,

    "std_executor_memory": parse_to_bytes,
    "conform_driver_memory": parse_to_bytes,
    "conform_executor_memory": parse_to_bytes,
    "std_driver_memory": parse_to_bytes,

    "conform_data_size_ratio": parse_percentage,
    "std_size_ratio": parse_percentage,
    "std_data_size_ratio": parse_percentage,
    'conform_size_ratio': parse_percentage,

    "conform_cmd_line_args": parse_command_line_args,
    "std_cmd_line_args": parse_command_line_args,

}

_remove_cmd_line_args = [
    "menas-auth-keytab",
    "menas-credentials-file"
]


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

    def __init__(self, api_base_url, username, password, ssl_path=None, default_tzinfo=tz.tzutc()):
        logger.debug(f"starting menas aggregator url: {api_base_url} ssl:{ssl_path} default_tzinfo: {default_tzinfo}")
        self.menas_api = MenasApi(api_base_url, username, password, ssl_path=ssl_path)
        self.default_tzinfo = default_tzinfo

    def cast_run_data(self, run):
        additional_info = run['controlMeasure']['metadata']['additionalInfo']
        for key, value in additional_info.items():
            if key in _cast_additionalInfo_dict:
                additional_info[key] = _cast_additionalInfo_dict[key](value) # custom casting for certain fileds
            else:
                additional_info[key] = cast_string_to_value(value)  # default casting str -> int of float
            if key in ['conform_cmd_line_args', 'std_cmd_line_args']:  # remove certain arg values
                cmd_args = additional_info[key]
                for arg_name in cmd_args:
                    if arg_name in _remove_cmd_line_args:
                        cmd_args[arg_name] = 'spot_removed'

        start_date_time = parse_date(run.get('startDateTime'), date_formats, default_tz=self.default_tzinfo)
        run['startDateTime'] = start_date_time

        # handle info dates in order to avoid interpreting as date in Elasticsearch
        additional_info['enceladus_info_date'] = additional_info['enceladus_info_date'] + ' info_date'
        run['controlMeasure']['metadata']['informationDate'] = run['controlMeasure']['metadata']['informationDate'] + ' info_date'

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

    def is_matching_app(self, app):
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

    def aggregate(self, app):
        logger.debug(f"{app['name']} {app['id']}")
        for attempt in app.get('attempts'):
            run = attempt.get('app_specific_data', None).get('enceladus_run', None)
            if run is not None:
                # pop  original checkpoints array from run
                raw_checkpoints = run.get('controlMeasure', None).pop('checkpoints', None)
                if raw_checkpoints is not None:
                    # add aggregations of checkpoints to run
                    run['controlMeasure']['checkpoints'] = self._aggregate_checkpoints(raw_checkpoints)
        return app

    def _aggregate_checkpoints(self, raw_checkpoints):
        new_checkpoints = {'elements_count': len(raw_checkpoints)}
        controls_match = True
        agg_checkpoints = {}
        reference_controls = {}
        num_controls = 0
        for checkpoint in raw_checkpoints:
            checkpoint['name'] = checkpoint['name'].replace(' ', '')
            process_start_time = parse_date(checkpoint.pop('processStartTime'), date_formats,
                                                   default_tz=self.default_tzinfo, fail_on_unknown_format=False)
            process_end_time = parse_date(checkpoint.pop('processEndTime'), date_formats,
                                                 default_tz=self.default_tzinfo, fail_on_unknown_format=False)

            # if the times cannot be casted correctly, the fields are skipped, not to interfere with ES schema
            if (process_start_time is not None) and (process_end_time is not None):
                checkpoint['processStartTime'] = process_start_time
                checkpoint['processEndTime'] = process_end_time
                duration = (process_end_time - process_start_time).total_seconds() * 1000
                checkpoint['duration'] = duration

            controls = checkpoint.pop('controls', None)

            if controls_match:  # we only check control values until first mismatch
                if not reference_controls:  # the first checkpoint's controls are set as a reference
                    for control in controls:
                        reference_controls[control['controlName']] = control['controlValue']
                    num_controls = len(reference_controls)
                else:  # reference controls already set in first checkpoint
                    if len(controls) != num_controls:  # different number of controls in checkpoints
                        controls_match = False
                        controls_error = {
                            'checkpoint': checkpoint,
                            'incorrect_num_controls': True
                        }
                        new_checkpoints['controls_error'] = controls_error
                    else:
                        for control in controls:
                            # the control values are originally stored as strings
                            # we assume '123' and '123.0' are distinct values
                            # as they also should be of the same type and precision
                            # If control names are missing it is also an error
                            if not reference_controls.get(control['controlName'], None) == control['controlValue']:
                                controls_match = False
                                controls_error = {
                                    'checkpoint': checkpoint,
                                    'controlName':control['controlName'],
                                    'controlValue': control['controlValue'],
                                    'initialControlValue': reference_controls.get(control['controlName'], None),
                                    'incorrect_num_controls': False
                                }
                                new_checkpoints['controls_error'] = controls_error
                                break

            # save checkpoint
            agg_checkpoints[checkpoint['name']] = checkpoint

        new_checkpoints['agg_checkpoints'] = agg_checkpoints
        new_checkpoints['controls_match'] = controls_match
        new_checkpoints['num_controls'] = num_controls
        return new_checkpoints

    def post_aggregate(self, agg):
        """ Process aggregations of Enceladus runs.
        This method is called on each aggregation after enrich(app), aggregate(app) and flatten(app).
        """
        post_aggregations = {}
        input_in_memory = get_attribute(agg, ['attempt', 'aggs', 'stages', 'inputBytes', 'max'])
        if input_in_memory is None:
            logger.warning(f"Missing attempt.aggs.stages.inputBytes.max in "
                           f"app  {agg.get('id', 'missing_app_id')} "
                           f"attempt {get_attribute(agg, ['attempt', 'attemptId'])}")
            return agg
        output_in_memory = get_attribute(agg, ['attempt','aggs', 'stages', 'outputBytes', 'max'])
        est_peak_memory = get_attribute(agg, ['attempt', 'aggs', 'summary', 'est_peak_memory_usage'])

        additional_info = get_attribute(agg, ['attempt', 'app_specific_data', 'enceladus_run', 'controlMeasure', 'metadata',
                                       'additionalInfo'])

        if additional_info is None:
            logger.warning(f"Missing attempt.app_specific_data.enceladus_run.controlMeasure.metadata.additionalInfo in "
                           f"app  {agg.get('id', 'missing_app_id')} "
                           f"attempt {get_attribute(agg, ['attempt', 'attemptId'])}")
            return agg

        enceladus_type = agg['app_specific_data']['classification']['type']
        input_in_storage = None
        output_in_storage = None

        if enceladus_type == 'standardization':
            input_in_storage = additional_info.get('std_input_data_size', None)
            output_in_storage = additional_info.get('std_output_data_size', None)
        elif enceladus_type == 'conformance':
            input_in_storage = additional_info.get('conform_input_data_size', None)
            output_in_storage = additional_info.get('conform_output_data_size', None)
        elif enceladus_type == 'standardization&conformance':
            input_in_storage = additional_info.get('std_input_data_size', None)
            output_in_storage = additional_info.get('conform_output_data_size', None)
        else:
            logger.error(f"Unrecognised Enceladus app type: {enceladus_type}")
            return agg

        if input_in_storage:
            post_aggregations['input_in_storage_HDFS_blocks'] = bytes_to_hdfs_block(input_in_storage)
            if input_in_memory != 0 and input_in_storage != 0:
                post_aggregations['input_deserialization_factor'] = input_in_memory /input_in_storage
                if est_peak_memory:
                    post_aggregations['peak_to_input_memory_factor'] = est_peak_memory / input_in_storage

        if output_in_storage:
            post_aggregations['output_in_storage_HDFS_blocks'] = bytes_to_hdfs_block(output_in_storage)
            if output_in_memory != 0 and output_in_storage != 0:
                post_aggregations['output_deserialization_factor'] = output_in_memory / output_in_storage
                if est_peak_memory:
                    post_aggregations['peak_to_output_memory_factor'] = est_peak_memory / output_in_storage

        if input_in_storage and output_in_storage and input_in_storage != 0 and output_in_storage != 0:
            post_aggregations['output_to_input_storage_size_ratio'] = output_in_storage / input_in_storage

        agg['attempt']['aggs']['summary']['postprocessing'] = post_aggregations
        return agg



