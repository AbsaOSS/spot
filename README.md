![Spot logo](https://user-images.githubusercontent.com/8556576/87431742-5a38a380-c5e7-11ea-9952-b8bf7f218aae.png)

**S**park[*](https://github.com/apache/spark) **P**arameter **O**p**T**imizer.
<!-- toc -->
- [What is Spot?](#what-is-spot)
- [Modules](#modules)
    - [Crawler](#crawler)
    - [Regression](#regression)
    - [Setter](#setter)
    - [Enceladus](#enceladus)
- [Deployment](#deployment)
<!-- tocstop -->

## What is Spot


![Spot architecture](https://user-images.githubusercontent.com/8556576/87431759-5e64c100-c5e7-11ea-84bb-ae1e2403c84a.png)

It includes the following key modules:


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
