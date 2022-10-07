# Log-analytics-lambda-arch

# Log analysis using lambda architecture

## Overview
Ever wonder what kind of bots visits your website? This project helps you identify the major search engine bots that visits your website and the activities they perform on your website. This project was built using lambda architecture, combining both real-time and batch data pipeline together. \
`ps`: You can also configure this project to use other kafka producer, rather than the producer used by apache nifi  

## Architecture
<img src="resources/log_analytics.png">

## Dashboard
<img src="resources/visual.png">

## PROCESS
Stream log file from apache nifi  using kafka and zookeeper to  stream data to spark structured streaming, storing historical data in hdfs master dataset and computing batch view in cassandra, while prodcuing real-time view as well. The batch view could be done once a day, or twice as it uses  `precomputation algorithm, while the speed layer uses incremental`. YOU CAN USE TOOLS SUCH AD AS CRON OR  AIFRFLOW FOR SCHEDULING BATCH JOB AND DROPPING DATA IN REAL-TIME VIEW OF DATA PIPELINE

## Setup
**This project requires you to have docker up and running**
- To build docker image run `docker build -t log-analytics .`
- Run `docker-compose -f docker-compose.yaml` to start the resources needed
- when docker-compose is up and running, run `bash ./setup.sh`

### Setup nifi
- loading.....
### Run real-time job 
`docker exec spark-master /spark/bin/spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 opt/spark_store/streaming/streaming-job.py`

### Run batch job
`docker exec spark-master /spark/bin/spark-submit  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --master spark://localhost:7077 opt/spark_store/batch/batch-job.py`

### Web UI
- spark cluster - http://localhost:8080/
- Hadoop - http://localhost:9870/
- dashboard/visualization - http://localhost:9870/
- apache nifi - http://localhost:9090/

### Access image
- Cassandra - `docker exec -it cassandra cqlsh /bin/bash`
- Spark - `docker exec -it spark-master /bin/bash`
- Hadoop `docker exec -it namenode /bin/bash` run hadoop commands here, such as `hdfs dfs -ls /data`