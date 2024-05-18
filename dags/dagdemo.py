from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from datetime import datetime

dag = DAG("daily_pipeline", start_date=datetime(2021, 1, 1), schedule_interval="@once")


ingest_to_bronze_layer = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_to_bronze_layer',
    conn_id='spark_default',
    application_args=['--ingestion-date', '2023-01-01'],
    dag=dag
)
