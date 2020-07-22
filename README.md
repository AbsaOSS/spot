![Spot logo](https://user-images.githubusercontent.com/8556576/87431742-5a38a380-c5e7-11ea-9952-b8bf7f218aae.png)

**S**park[*](https://github.com/apache/spark) **P**arameter **O**p**T**imizer.
<!-- toc -->
- [What is Spot?](#what-is-spot)
- [How Spot is used to tune Spark apps?](#how-spot-is-used-to-tune-Spark-apps?)
- [Modules](#modules)
    - [Crawler](#crawler)
    - [Regression](#regression)
    - [Setter](#setter)
    - [Enceladus](#enceladus)
- [Deployment](#deployment)
<!-- tocstop -->

## What is Spot


![Spot architecture](https://user-images.githubusercontent.com/8556576/87431759-5e64c100-c5e7-11ea-84bb-ae1e2403c84a.png)

## How Spot is used to tune Spark apps? 
In this section we provide examples of plots and analysis which demonstrate how Spot is applied to tune preformance of Spark jobs.

#### Example 1: Dynamic VS Fixed resource allocation 
![Dynamic Resource Allocation](https://user-images.githubusercontent.com/8556576/88194526-3a385e00-cc3f-11ea-817b-b72254f16cf9.png)
The plot above shows the dependence of time of Enceladus runs for a particular dataset on the input size and number of allocated CPU cores. 
The left sub-plot corresponds to fixed resource allocation which was the default. Due to great variation of input size 
in data pipelines, fixed allocation often leads to either: 1) extended time in case of under-allocation or 2) wasted 
resources in case of over-allocation. The right sub-plot demonstrates how 
[Dynamic Resource Allocation](http://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation), 
set as a new default, solves this issue. 
Here, the number of cores is adjusted to the input size, and as a result the job time becomes more stable and efficiency
 improves.
 
 #### Example 2: Small files issue
 ![Small files issue](https://user-images.githubusercontent.com/8556576/88194561-41f80280-cc3f-11ea-97ed-75657585392f.png)
 When shuffle operations are present, Spark creates 200 partitions by default regardless of the data size. Excessive 
 fragmentation of small files compromises HDFS performance.  The presented plot, produced by Spot, shows how the number 
 of output partitions depends on input size with old/new configurations. As it can be seen in the plot, 
 [Adaptive Execution](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
  creates a reasonable number of partitions proportional to data size. Based on such analysis, enabled by Spot, 
  it was set as a new default for Enceladus.
  
#### Example 3: Parallelism
 ![Parallelism per job](https://user-images.githubusercontent.com/8556576/88194763-7a97dc00-cc3f-11ea-8530-48a66dde8160.png)
 
 
## Modules

### Crawler
The Crawler module aggregates [Spark history](https://spark.apache.org/docs/latest/monitoring.html#rest-api) data and 
stores it in [Elasticsearch](https://github.com/elastic/elasticsearch) for further analysis by tools such as [Kibana](https://github.com/elastic/kibana).
The Spark History data are merged from several [APIs](https://spark.apache.org/docs/latest/monitoring.html#rest-api) 
(attempts, executors, stages) into a single raw JSON document.

Information from external services (currently: Menas) is added for supported 
applications (currently: [Enceladus](https://github.com/AbsaOSS/enceladus)). The raw documents are stored in a separate
Elasticsearch collection. Aggregations for each document are stored in a separate
collection. The aggregations are performed in the following way: custom aggregations
(e.g. min, max, mean, non-zero) are calculated for each value (e.g. completed tasks)
across elements of each array in the original raw document (e.g. executors). Custom
calculated values are added, e.g. total CPU allocation, estimated efficiency and speedup.


### Regression
(Future) The regression models are using the stored data in order to interpolate time VS. config values. 

### Setter
(Future) The Setter module suggests config values for new runs of Spark apps based on the regression model.

### Enceladus
The Enceladus module provides integration capabilities for SPOT usage with [Enceladus](https://github.com/AbsaOSS/enceladus)

## Deployment
- Install Python 3 (recommended 3.7 and later)
- Install required modules (see requirements.txt)
- Clone code to a location of your choice
- Add project root directory to PYTHONPATH e.g. `export PYTHONPATH=/path/to/spot`
- Check access to external services:
    - Elasticsearch and Kibana
    - Spark History (2.4 and later recommended)
    - (Optional) [Menas](https://github.com/AbsaOSS/enceladus) (2.1.0 and later recommended) Requires username and password
- Create configuration: copy config.ini.template to config.ini and set parameters from the above step
    - For new deployment set raw_index and agg_index to new index names which do not exist in elasticsearch
- Configure logging in logging_confg.ini (see [Logging](https://docs.python.org/2/library/logging.config.html#configuration-file-format)) 


### Running Crawler
`cd spot/crawler`

`python3 crawler.py [options]`

|    Option     |      Default     |                    Description                 |
|---------------|------------------|------------------------------------------------|
|--min_end_date | None             |Optional. Minimal completion date of the Spark job in the format YYYY-MM-DDThh:mm:ss. Crawler processes Spark jobs completed after the latest of a) the max completion date among already processed jobs (stored in the database) b) this option. In the first run, when there are niether processed jobs in the database nor this option is specified, the crawler starts with the earliest completed job in Spark History.|

This will start the main loop of the crawler. It gets new completed apps, processes and stores them in the database. When all the new apps are processed the crawler sleeps `sleep_seconds` (see config.ini) before the next iteration. To exit the loop, kill the process.
