![Spot logo](https://user-images.githubusercontent.com/8556576/87431742-5a38a380-c5e7-11ea-9952-b8bf7f218aae.png)

**S**park[*](https://github.com/apache/spark) **P**arameter **O**p**T**imizer.
<!-- toc -->
- [What is Spot?](#what-is-spot)
- [Modules](#modules)
    - [Crawler](#crawler)
    - [Regression](#regression)
    - [Setter](#setter)
    - [Enceladus](#enceladus)
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

