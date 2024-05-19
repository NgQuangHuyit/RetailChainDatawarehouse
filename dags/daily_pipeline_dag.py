from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG("daily_pipeline", start_date=datetime(2021, 1, 1), schedule_interval="@once")


ingest_to_bronze_layer = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_to_bronze_layer',
    conn_id='spark_default',
    application_args=['--ingestion-date', '2023-01-01'],
    dag=dag
)


dim_customer = SparkSubmitOperator(
    application='/opt/airflow/dags/Transformation/get_dim_customer.py',
    task_id='get_dim_customer',
    conn_id='spark_default',
    dag=dag
)

dim_branch = SparkSubmitOperator(
    application='/opt/airflow/dags/Transformation/get_dim_branch.py',
    task_id='get_dim_branch',
    conn_id='spark_default',
    dag=dag
)

dim_promotion = SparkSubmitOperator(
    application='/opt/airflow/dags/Transformation/get_dim_promotion.py',
    task_id='get_dim_promotion',
    conn_id='spark_default',
    dag=dag
)

dim_product = SparkSubmitOperator(
    application='/opt/airflow/dags/Transformation/get_dim_product.py',
    task_id='get_dim_product',
    conn_id='spark_default',
    dag=dag
)

dim_employee = SparkSubmitOperator(
    application='/opt/airflow/dags/Transformation/get_dim_employee.py',
    task_id='get_dim_employee',
    conn_id='spark_default',
    dag=dag
)

fct_sale = SparkSubmitOperator(
    application='/opt/airflow/dags/Transformation/get_fct_sale.py',
    task_id='get_fct_sale',
    conn_id='spark_default',
    dag=dag
)

fct_promotion_coverage = SparkSubmitOperator(
    application='',
    task_id='get_fct_promotion_coverage',
    dag=dag
)

silver_layer_complete = EmptyOperator(
    task_id='silver_layer_complete',
    dag=dag
)

get_gold_layer = EmptyOperator(
    task_id='get_gold_layer',
    dag=dag
)

ingest_to_bronze_layer >> [dim_customer, dim_product, dim_employee, dim_branch, dim_promotion] >> fct_sale

[dim_product, dim_promotion, dim_branch] >> fct_promotion_coverage

[fct_sale, fct_promotion_coverage] >> silver_layer_complete >> get_gold_layer

