![Spot logo](https://user-images.githubusercontent.com/8556576/88801204-9d7b4080-d1a9-11ea-9dcb-d704be2292cf.png)

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

## What is Spot?
Spot is a set of tools for monitoring and performance tuning of [Spark](https://github.com/apache/spark) applications. 
The main idea is to continuously apply statistical analysis on repeating (production) runs of the same applications. 
This allows to compare target metrics (e.g. time, cluster load, cloud cost) between different code versions and configurations.
Further, ML models and optimization techniques can be applied to automate configuration of new application runs. 

One of the primarily considered use cases is ETL (Extract Transform Load) application in batch mode. 
[Enceladus](https://github.com/AbsaOSS/enceladus) is an example of such project. Such application repeatedly runs 
(e.g. thousands runs per hour) on new data instances which greatly vary in size and processing complexity. For this reason,
a uniform setup would not be optimal for the entire spectrum of runs. In contrast, a statistical approach allows to 
categorize cases and set appropriate configuration automatically.  

Spot relies on metadata available in Spark History and, therefore, does not require additional instrumentation of Spark apps.
This enables collection of statistics of production jobs without compromising their performance.

Spot consists of the following modules:

|     Module     |          Short description          |
|----------------|-------------------------------------|
| Crawler        | Provides collection and initial processing of Spark history data. The output is stored in elasticsearch and can be visualized with Kibana for monitoring |                                    |
| Regression     |(Future) The regression models are using the stored data in order to interpolate time VS. config values. |
| Setter         |(Future) The Setter module suggests config values for new runs of Spark apps based on the regression model.|
| Enceladus      |The Enceladus module provides integration capabilities for Spot usage with [Enceladus](https://github.com/AbsaOSS/enceladus)|
A detailed description of each model can be found in section [Modules](#modules). 

The diagram below shows current Spot architecture. 

![Spot architecture](https://user-images.githubusercontent.com/8556576/87431759-5e64c100-c5e7-11ea-84bb-ae1e2403c84a.png)

## How Spot is used to tune Spark apps? 
In this section we provide examples of plots and analysis which demonstrate how Spot is applied to monitor and tune 
performance of Spark jobs.

#### Example: Cluster usage over time
![Cluster usage](https://user-images.githubusercontent.com/8556576/88381248-5efb1580-cda6-11ea-8eb1-80524b4f167a.png)
This plot shows how many CPU cores were allocated for Spark apps of each user over time. Similar plots can be made for memory used by executors,
amount of shuffled data and so on. Also, the series can be split by application name or other metadata. Kibana time series, used in this example, does not account for duration of allocation, but this is planned to be addressed using custom plots in future. 

#### Example: Characteristics of a particular Spark application
When an application is running repeatedly, statistics of runs can be used to focuse code optimization towards most 
critical and common cases. Also, such statistics can be used to compare app versions.
The next two plots show histograms of run duration in milliseconds (attempt.duration) and size of input data in bytes 
(attempts.aggs.stages.inputBytes.max). Filters on max values are applyed in both plots in order to keep a reasonable scale. 
![Time histogram](https://user-images.githubusercontent.com/8556576/88382148-23614b00-cda8-11ea-9965-654b1b0bf691.png)
![Size histogram](https://user-images.githubusercontent.com/8556576/88382162-2e1be000-cda8-11ea-93e3-d9cc47a27f15.png)
The next figure shows statistics of configurations used in different runs of the same app. 
![Configurations](https://user-images.githubusercontent.com/8556576/88383534-01b59300-cdab-11ea-8080-f8fc6c454a9d.png)
When too many executors are allocated to a relatively small job, or partitioning is not working properly 
(e.g. unsplitable data formats), some of the executors remain idle for the entire run. In such case the resource 
allocation can be  safely decreased in order to reduce the cluster load. The next histogram illustrates such case.
![Zero tasks](https://user-images.githubusercontent.com/8556576/88386609-0aa96300-cdb1-11ea-9cf5-be970e53eec6.png)

#### Example: Dynamic VS Fixed resource allocation 
![Dynamic Resource Allocation](https://user-images.githubusercontent.com/8556576/88194526-3a385e00-cc3f-11ea-817b-b72254f16cf9.png)
The plot above shows the dependence of time of Enceladus runs for a particular dataset on the input size and number of allocated CPU cores. 
The left sub-plot corresponds to fixed resource allocation which was the default. Due to great variation of input size 
in data pipelines, fixed allocation often leads to either: 1) extended time in case of under-allocation or 2) wasted 
resources in case of over-allocation. The right sub-plot demonstrates how 
[Dynamic Resource Allocation](http://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation), 
set as a new default, solves this issue. 
Here, the number of cores is adjusted to the input size, and as a result the job time becomes more stable and efficiency
 improves.
 
 #### Example: Small files issue
![Small files issue](https://user-images.githubusercontent.com/8556576/88194561-41f80280-cc3f-11ea-97ed-75657585392f.png)
When shuffle operations are present, Spark creates 200 partitions by default regardless of the data size. Excessive 
fragmentation of small files compromises HDFS performance.  The presented plot, produced by Spot, shows how the number 
of output partitions depends on input size with old/new configurations. As it can be seen in the plot, 
[Adaptive Execution](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
creates a reasonable number of partitions proportional to data size. Based on such analysis, enabled by Spot, 
it was set as a new default for Enceladus.
  
#### Example: Parallelism
Here we demonstrate how to apply selected metrics from [parellel algorithms theory](https://en.wikipedia.org/wiki/Analysis_of_parallel_algorithms) 
to [Spark execution model](https://spark.apache.org/docs/latest/cluster-overview.html#cluster-mode-overview).

The diagram below shows execution timeline of a Spark app. Here, for simplicity of the demonstration, we assume each 
executor has a single CPU core. The duration of the run on _m_ executors is denoted _T(m)_. The allocation time of each 
executor is presented with a dotted orange rectangle. Tasks at the executors are shown as green rectangles. The tasks
are organized in stages which may overlap. Tasks of each stage are executed in parallel. 
Those parts of the driver program which do not overlap with stages are pictured as red rectangles. In our analysis,
we assume those parts make the _sequential part_ of the program which has a fixed duration. 
It includes the code which is not parallelizable (on executors): 
startup and scheduling overheads, Spark's query optimizations, external API calls, custom driver code and etc. In other words, 
it is the part of the program when there are no tasks at executors. 
The rest of the run duration, corresponds to _parallel part_, i.e. when tasks can be executed.
![Spark parallelism](https://user-images.githubusercontent.com/8556576/88536230-bc43d080-d00b-11ea-8841-f7a9b925ef6b.png)
Total _allocated core time_ is a sum of products of the allocation time and the number of cores over all executors.
Knowing the sequential part and the total duration of all of the tasks, we can also estimate the duration of a (hypothetical)
run with a single executor. The next plot shows an example how Spot visualizes the described metrics.
Here, the values averaged over multiple runs are shown for two types of Enceladus apps.  

![Parallelism per job](https://user-images.githubusercontent.com/8556576/88376482-ca8cb500-cd9d-11ea-9692-78b659f8b2f9.png)

The efficiency and speedup are estimated using the following formulas:
<img src="https://user-images.githubusercontent.com/8556576/88538274-578a7500-d00f-11ea-9b91-bc1391504f97.png" width="350px" />
 
Please, note, that in this analysis we focus on parallelism on executors, the possible parallelism of the driver part 
on multiple driver cores requires a separate investigation. 

The next two plots demonstrate histograms of efficiency and speedup for multiple runs of a sample Spark app with different
 inputs and configurations. 
![Efficiency hist](https://user-images.githubusercontent.com/8556576/88551797-93c7d080-d023-11ea-876c-de6ff173dbc4.png)
![Speedup hist](https://user-images.githubusercontent.com/8556576/88552001-cffb3100-d023-11ea-8c85-fd8b97e8359e.png)  
Further analysis of such metrics may include dependencies on particular configuration values. 
 
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
The Enceladus module provides integration capabilities for Spot usage with [Enceladus](https://github.com/AbsaOSS/enceladus)

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
