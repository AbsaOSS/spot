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
from pprint import pprint
import pandas as pd
import numpy as np
import math
from scipy.optimize import curve_fit

from spot.crawler.commons import bytes_to_hdfs_block
import spot.utils.setup_logger

logger = logging.getLogger(__name__)


def lightweight(x, t_seq, wave_time):
    num_executors, executor_cores, data_size = x

    blocks = bytes_to_hdfs_block(data_size)
    total_cores = num_executors * executor_cores

    t = t_seq + wave_time * np.ceil(blocks/total_cores)
    return t


def input_filter(x):
    """ Filters input variables applicable for the model

    :param X: tuple of input variables
    :return: bool, if the input variables are applicable for the model
    """
    num_executors, executor_cores, data_size = x

    blocks = bytes_to_hdfs_block(data_size)

    return blocks > 1



class Lightweight:
    """ Performance Prediction Model (PPM) for Spark jobs.

    It is based on paper:
    Y. Amannejad, S. Shah, D. Krishnamurthy and M. Wang,
    "Fast and Lightweight Execution Time Predictions for Spark Applications,"
    2019 IEEE 12th International Conference on Cloud Computing (CLOUD),
    Milan, Italy, 2019, pp. 493-495, doi: 10.1109/CLOUD.2019.00088.
    https://ieeexplore.ieee.org/abstract/document/8814547

    We also add an additional assumptions based on our observations:
    - sequential part of the app has a constant duration
    - partitioning is not changed during the execution. As a result, if the app contains multiple stages,
    the number of waves in each stage is the same. Therefore, such multiple stages can be modeled as a single stage,
    where average wave time is a sum of average wave times across all stages.

    Formula:
    t = t_seq + wave_time * ceil(ceil(data_size/hdfs_block_size)/total_cores)

    t - duration of app run
    total_cores = num_executors * executor_cores - total number of allocated cores across all executors

    Parameters:
        t_seq - duration of sequential part (in ms)
        wave_time - average time per wave
                    Can also be understood as sum of average task duration of all parallel stages
                    (excluding e.g. collect)
        hdfs_block_size - size of HDFS block in bytes.
                          We assume each block corresponds to one Spark partition and one task.

    Input variables:
        num_executors - total number of allocated executors
        executor_cores - cores per executor
        data_size - input data size in bytes


    Output variable:
        t - duration of the Spark app in milliseconds
    """

    def __init__(self):
        pass

    def train(self, df):
        total_runs_no = len(df.index)

        # select variables

        num_executors = df['attempt.aggs.allexecutors.executors.elements_count']
        executor_cores = df['attempt.aggs.allexecutors.executors.totalCores.max']
        data_size = df['attempt.aggs.stages.inputBytes.max']

        # filter applicable runs
        filtered_index = input_filter((num_executors, executor_cores, data_size))

        y = df['attempt.duration'][filtered_index]
        x = (num_executors[filtered_index], executor_cores[filtered_index], data_size[filtered_index])

        filtered_runs_no = len(y.index)

        if filtered_runs_no > 2:
            popt, pcov = curve_fit(lightweight, x, y, bounds=(0,np.inf))
            perr = np.sqrt(np.diag(pcov))
            relErr = np.abs(perr/popt)
            t_seq = popt[0]
            wave_time = popt[1]
            logger.info(f"Total runs: {total_runs_no} Filtered runs: {filtered_runs_no} Vals: {popt} Err: {perr} RelErr: {relErr}")
        else:
            logger.info(f"Total runs: {total_runs_no} Filtered runs: {filtered_runs_no} NOT ENOUGH DATA TO FIT MODEL"křiž)

        return

    def predict(self, job_params):
        pass


