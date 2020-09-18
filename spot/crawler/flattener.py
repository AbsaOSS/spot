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

import pandas as pd
import numpy as np

import logging

from spot.crawler.elastic import Elastic
from spot.utils.config import SpotConfig
from spot.crawler.commons import get_last_attempt, bytes_to_hdfs_block, parse_to_bytes, bytes_to_gb

import spot.utils.setup_logger

logger = logging.getLogger(__name__)

DF = pd.DataFrame

# custom aggregations


def count_zeroes(series):
    return (series.values == 0).sum()


def count_not_null(series):
    return int(series.count())


def concat_unique_values(series):
    return '|'.join(series.unique())


def rsd(series):
    """ Return Relative Standard Deviation (RSD) aka Coefficient of Variation (CV) of a numeric series."""
    mean = series.mean()
    if mean == 0:
        return np.NaN
    std = series.std()
    rsd = std / abs(mean)
    return rsd


# This dictionary defines which aggregations are applied to each column type
# see https://pandas.pydata.org/pandas-docs/stable/reference/series.html
default_type_aggregations = {
    np.number: [DF.min, DF.max, DF.sum, DF.mean, DF.std, DF.nunique, count_zeroes, count_not_null, rsd],
    np.object: [count_not_null, pd.Series.nunique, concat_unique_values], # corresponds to string type
    np.datetime64: [min, max],
    bool: [DF.any, DF.all, DF.sum]
}


def aggregate_by_col_type(df, type_aggregations=default_type_aggregations):
    """Apply lists of aggregations to specified column types of DataFrame. Return results as a dict.

    If the result of an aggregation is NA, NaN, None or NaT it is omitted in the output dict.
    Keyword arguments:
    type_aggregations -- dict of {type: [list, of, aggregations]}
    """
    n = len(df.index)
    result = {'elements_count': n}
    if n == 0:
        return result
    # apply aggregations to columns of different types
    for t, aggs in type_aggregations.items(): # t - column type, aggs - aggregations
        # select subset of columns of type t
        subset = df.select_dtypes([t])
        if len(subset.columns) ==0: # if no columns of selected type
            continue
        # apply aggregations for the given type
        res1 = subset.agg(aggs)
        for column_name, elements in res1.items():
            if column_name not in result.keys():
                result[column_name] = {}
            for agg_name, value in elements.items():
                if pd.isna(value): # skip property if the value is NA, NaN, None or NaT
                    logger.debug(f"isna value skipped in aggregations: "
                                 f"column_name : {column_name}, agg_name: {agg_name}, value: {value}")
                    continue
                if t is bool: # aggregations over bool columns have to be casted for compatibility w Elasticsearch
                    if isinstance(value, np.bool_):
                        value = bool(value)
                    if isinstance(value, np.int64):
                        value = int(value)
                result[column_name][agg_name] = value
    return result


def add_custom_executor_metrics(attempt, ex):
    attempt_start = attempt.get('startTime')
    attempt_end = attempt.get('endTime')

    ex['x_startTime'] = ex.get('addTime', attempt_start)
    ex['x_stopTime'] = ex.get('removeTime', attempt_end)
    duration_ms = (ex['x_stopTime'] - ex['x_startTime']).total_seconds() * 1000
    ex['x_durationMilliseconds'] = duration_ms
    ex['x_coreCost'] = ex['totalCores'] * duration_ms
    storage_gb = bytes_to_gb(ex['maxMemory'])
    ex['x_storageCost'] = storage_gb * duration_ms
    return ex


def flatten_executors(attempt):
    executors = attempt.get('allexecutors')
    driver = {}
    for ex in executors:
        add_custom_executor_metrics(attempt, ex)
        if ex['id'] == 'driver':
            driver = ex
    df = pd.io.json.json_normalize(executors)
    df_executors = df[df['id'] != 'driver']
    ex_aggregations = aggregate_by_col_type(df_executors)
    result = {
        'driver': driver,
        'executors': ex_aggregations
    }
    return result


def add_custom_stage_metrics(attempt, stage):
    if ('completionTime' in stage) and ('firstTaskLaunchedTime' in stage):
        scheduling_overhead_ms = (stage['firstTaskLaunchedTime'] - stage['submissionTime']).total_seconds() * 1000
        duration_ms = (stage['completionTime'] - stage['firstTaskLaunchedTime']).total_seconds() * 1000
        throughput_bytes = stage['inputBytes'] / duration_ms
        throughput_records = stage['inputRecords'] / duration_ms
    else:
        logger.warning(f'Incomplete stage {stage["stageId"]} in'
                       f' {attempt["environment"]["sparkProperties"]["spark_app_id"]}'
                       f'attempt {stage.get("attemptId", None)}')
        scheduling_overhead_ms = 0
        duration_ms = 0
        throughput_bytes = -1
        throughput_records = -1
    CPUtime_ms = stage['executorCpuTime'] / 1000000  # nanoseconds to milliseconds

    stage['x_scheduling_overhead'] = scheduling_overhead_ms
    stage['x_duration'] = duration_ms
    stage['x_executorCpuTime_ms'] = CPUtime_ms

    stage['throughput_bytes'] = throughput_bytes
    stage['throughput_records'] = throughput_records


def flatten_stages(attempt):
    stages = attempt.get('stages')
    for stage in stages:
        add_custom_stage_metrics(attempt, stage)

    df = pd.io.json.json_normalize(stages)
    aggregations = aggregate_by_col_type(df)
    return aggregations


def flatten_app(app):
    attempts = app.get('attempts')
    last_attempt = get_last_attempt(app)
    last_attempt_id = last_attempt.get('attemptId')
    for attempt in attempts:
        res = app.copy()
        res.pop('attempts', None)  # remove attempts array from result
        attempt_id = attempt.get('attemptId')
        if (attempt_id is None) or (attempt_id == last_attempt_id):
            res['isFinalAttempt'] = True
        else:
            res['isFinalAttempt'] = False
        flat_attempt = attempt.copy()
        aggs = get_attempt_aggregations(attempt)
        flat_attempt['aggs'] = aggs

        # remove raw details
        flat_attempt.pop('allexecutors', None)
        flat_attempt.pop('stages', None)

        res['attempt'] = flat_attempt
        yield res


def get_attempt_aggregations(attempt):
    aggs = dict()
    aggs['allexecutors'] = flatten_executors(attempt)
    aggs['stages'] = flatten_stages(attempt)
    aggs['summary'] = calculate_summary(attempt, aggs)
    return aggs


def calculate_parallelism(stages):
    intervals_overlap = False
    total = 0

    # filter out stages with missing values:
    filtered_stages = []
    for stage in stages:
        if ('firstTaskLaunchedTime' in stage) and ('completionTime' in stage):
            filtered_stages.append(stage)

    # stages are sorted in reverse submission order by Spark History (not guaranteed)
    # we assume the overhead between submition and firstTaskLaunchedTime is tiny
    # see https://www.geeksforgeeks.org/merging-intervals/ for the algorithm

    # sort stages explicitly
    stages_sorted = sorted(filtered_stages, key=lambda i: i['firstTaskLaunchedTime'])

    cursor_first = None
    cursor_completion = None
    for stage in stages_sorted:
        stage_first = stage['firstTaskLaunchedTime']
        stage_completion = stage['completionTime']
        if cursor_first is None :
            # first completed stage found
            cursor_first = stage_first
            cursor_completion = stage_completion
            continue
        if stage_first < cursor_completion:
            intervals_overlap = True
            cursor_first = min([cursor_first, stage_first])
            cursor_completion = max([cursor_completion, stage_completion])

        else:
            # intervals don't overlap
            # add current cursor
            total += (cursor_completion - cursor_first).total_seconds() * 1000
            # update cursor
            cursor_first = stage_first
            cursor_completion = stage_completion

    # add the last interval
    if cursor_first is not None:
        total += (cursor_completion - cursor_first).total_seconds() * 1000

    return total, intervals_overlap


def calculate_summary(attempt, aggs):
    summary = {}
    if aggs['allexecutors']['executors']['elements_count'] > 0 \
            and aggs['stages']['elements_count'] > 0 \
            and 'firstTaskLaunchedTime' in aggs['stages']\
            and 'completionTime' in aggs['stages']:

        # duration of all tasks (if executed sequentially)
        parallel_work = aggs['allexecutors']['executors']['totalDuration']['sum']

        # duration of all stages
        stages_sum = aggs['stages']['x_duration']['sum']
        # stages could be executed in parallel
        first_stage_start = aggs['stages']['firstTaskLaunchedTime']['min']
        last_stage_finish = aggs['stages']['completionTime']['max']
        stages_interval = (last_stage_finish - first_stage_start).total_seconds() * 1000

        parallel_part, stages_in_parallel = calculate_parallelism(attempt.get('stages', []))
        duration = attempt['duration']

        # duration of sequential part
        seq_part = duration - parallel_part
        if seq_part < 0:
            logger.warning(f"calculated seq_part < 0")

        # estimated time on 1 core
        est_seq_time = seq_part + parallel_work

        # for all executors sum (totalCores * (removeTime - addTime))
        core_cost = aggs['allexecutors']['executors']['x_coreCost']['sum']

        stages_max_input_blocks = bytes_to_hdfs_block(aggs['stages']['inputBytes']['max'])
        executors_total_input_blocks = bytes_to_hdfs_block(aggs['allexecutors']['executors']['totalInputBytes']['sum'])
        unused_storage_memory = aggs['allexecutors']['executors']['maxMemory']['sum'] \
            - aggs['stages']['inputBytes']['max']

        summary = {

            'parallel_work': parallel_work,
            'parallel_part': parallel_part,
            'seq_part': seq_part,
            'est_seq_time': est_seq_time,
            'core_cost': core_cost,
            'stages_in_parallel': stages_in_parallel,
            'stages_sum':  stages_sum,
            'stages_interval': stages_interval,
            'stages_max_input_blocks': stages_max_input_blocks,
            'executors_total_input_blocks': executors_total_input_blocks,
            'unused_storage_memory': unused_storage_memory
        }
        if duration != 0:
            speedup = est_seq_time / duration
            summary['estimated_speedup'] = speedup

            parallel_fraction = parallel_part / duration
            summary['parallel_fraction'] = parallel_fraction

            seq_fraction = seq_part / duration
            summary['seq_fraction'] = seq_fraction

            average_throughput = bytes_to_hdfs_block(aggs['stages']['inputBytes']['max']) / duration
            summary['average_throughput'] = average_throughput

        if core_cost != 0:
            efficiency = parallel_work / core_cost
            summary['estimated_core_efficiency'] = efficiency

            unused_core_cost = core_cost - parallel_work
            summary['unused_core_cost'] = unused_core_cost

        props = attempt['environment']['sparkProperties']
        if 'spark_executor_memory' in props:
            summary['executor_memory_bytes'] = parse_to_bytes(props['spark_executor_memory'])

        if 'spark_driver_memory' in props:
            summary['driver_memory_bytes'] = parse_to_bytes(props['spark_driver_memory'])

        if aggs['allexecutors']['executors']['maxMemory']['sum'] != 0:
            storage_memory_usage = aggs['stages']['inputBytes']['max'] \
                                   / aggs['allexecutors']['executors']['maxMemory']['sum']
            summary['storage_memory_usage'] = storage_memory_usage

    return summary
