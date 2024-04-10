from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG("demo_spark_submit", start_date=datetime(2021, 1, 1), schedule_interval="@once")

t1 = SparkSubmitOperator(
    application='/opt/airflow/dags/ingestOrders.py',
    task_id='spark_submit_job',
    conn_id='spark_default',
    dag=dag
)

t1

