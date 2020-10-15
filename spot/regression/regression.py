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

from spot.regression.ppms.lightweight import Lightweight
from spot.utils.config import SpotConfig
from spot.crawler.elastic import Elastic
import spot.utils.setup_logger


logger = logging.getLogger(__name__)


def main():
    logger.info('Starting regression')
    conf = SpotConfig()
    elastic = Elastic(host=conf.elastic_host,
                      port=conf.elastic_port,
                      username=conf.elastic_username,
                      password=conf.elastic_password,
                      raw_index_name=conf.elastic_raw_index,
                      agg_index_name=conf.elastic_agg_index,
                      err_index_name=conf.elastic_err_index)

    #top_tags = elastic.get_top_tags()
    lightweight = Lightweight()

    logger.debug('tag, count, variance, distinct_cores, distinct_blocks')
    for bucket in elastic.get_training_tags():
        tag = bucket.get("key")
        count = bucket.get("doc_count")
        sample_variance = bucket.get("sample_variance").get('value')
        distinct_cores_count = bucket.get("distinct_cores_count").get('value')
        distinct_input_blocks = bucket.get("distinct_input_blocks").get('value')
        logger.info(f"{tag} {count} {sample_variance}, {distinct_cores_count}, {distinct_input_blocks}")

        df = pd.io.json.json_normalize(elastic.get_by_tag(tag, count))

        #pd.set_option('display.max_rows', None)
        #pd.set_option('display.max_columns', None)
        #pd.set_option('display.width', 50000)

        #df = df[df['attempt.aggs.summary.stages_max_input_blocks'] > 1]

        #print(df[['attempt.aggs.allexecutors.executors.totalCores.sum', 'attempt.aggs.summary.stages_max_input_blocks',
        #          'attempt.aggs.stages.numCompleteTasks.max', 'attempt.duration']].head(20))
        if len(df.index) > 4:
            lightweight.train(df)




if __name__ == '__main__':
    main()
