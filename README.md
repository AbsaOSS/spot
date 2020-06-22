# Spot
![Alt text](spot_logo.png?raw=true "Title")

**Spot** is **S**park **P**arameter **O**p**t**imizer.
It includes the following key modules:

### Crawler
Aggregates Spark history data and stores it (to elsaticsearch) 
for further analysis e.g. using Kibana. Spark history data from several APIs
(attempts, executors, stages) is merged into single raw json document. 
Information from external services (currently: Menas) is added for suppoted 
applications (currently: Enceladus). The raw documents are stored to a separate
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
