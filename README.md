![Alt text](images/spot_logo.png?raw=true)

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


![Alt text](images/architecture.png?raw=true)

It includes the following key modules:


## Modules

### Crawler
Aggregates [Spark history](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
 data and stores it to [elasticsearch](https://github.com/elastic/elasticsearch) 
for further analysis e.g. using [Kibana](https://github.com/elastic/kibana).
Spark history data from several 
[APIs](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
(attempts, executors, stages) is merged into single raw json document. 
Information from external services (currently: Menas) is added for suppoted 
applications (currently: [Enceladus](https://github.com/AbsaOSS/enceladus)). The raw documents are stored to a separate
elasticsearch collection. Aggregations for each document are stored in a separate
collection. The aggregations are performed in the following way: custom aggregations
(e.g. min, max, mean, non-zero) are calculated for each value (e.g. completed tasks)
across elements of each array in the original raw document (e.g. executors). Custom
calculated values are added, e.g. total CPU allocation, estimated efficiency and speedup.


### Regression
Regression models using the stored data

### Setter
Suggests parameters for new runs of Spark jobs based on the regression models output

### Enceladus
Integration for usage with [Enceladus](https://github.com/AbsaOSS/enceladus)
