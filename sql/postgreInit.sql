CREATE DATABASE metastore;

CREATE USER hive WITH ENCRYPTED PASSWORD 'hive';

GRANT ALL ON DATABASE metastore TO hive;

CREATE DATABASE airflow;

CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';

GRANT ALL ON DATABASE airflow TO airflow;


