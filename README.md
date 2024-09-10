# Retail chain data pipeline for Analytics and Reporting


## Project Overview
<img height="500" src="images/system-architecture.png" width="800"/>

## Prerequisites
- Docker
- Python 3.9 or later

## Technologies Used
- Python
- Airflow
- HDFS
- Spark
- Hive
- Metabase
- MySQL, Postgres

## Data Modeling

### Source database schema:

<img src="images/source-db-schema.png"/>

### Star Schema:

<img src="images/start-schema.png"/>

## Getting Started

### Infrastructure setup:

1. Clone project repository

```bash
git clone <link.com>
```

2. Navigate to project directory

```bash
cd RetailChainDatawarehouse
```

3. Build hadoopbase docker image
    
```bash
make build-hadoopbase
```

4. Start up infrastructure

```bash
make up && make setup
```

### Airflow Setup

Come to http://localhost:8081 to access Airflow web UI and login with:
* username: airflow
* password: airflow

Go to Admin -> Connections and create a new spark connection with the following values:

![](images/aiflow2.png)

Come to Dags tab and click on the trigger button on `daily_pipeline` dag to run the pipeline. 

![](images/airflow1.png)

### Metabase Dashboard

Start Spark Thrift Server:

```bash
make start-thift
```

Come to http://localhost:4000 to access Metabase web UI and register new account.
    
Setup Spark Thrift Server connection to Metabase:

Access dashboad:

![](/images/dashboard1.png)

![](images/dashboard2.png)

![](images/dashboard3.png)
    

### Others Service
- Browser HDFS file:
    http://localhost:9870
- Spark History Server:
    http://localhost:18080
- Hadoop Yarn Web UI:
    http://localhost:8088
