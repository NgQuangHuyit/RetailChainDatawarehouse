from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from datetime import datetime

dag = DAG("daily_pipeline", start_date=datetime(2021, 1, 1), schedule_interval="@once")


ingest_products = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_products',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.products',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/products.parquet'],
    dag=dag
)

ingest_branches = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_branches',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.branches',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/branches.parquet'],
    dag=dag
)

ingest_employees = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_employees',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.employees',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/employees.parquet'],
    dag=dag
)

ingest_address = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_address',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.address',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/address.parquet'],
    dag=dag
)

ingest_category = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_category',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.category',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/category.parquet'],
    dag=dag
)

ingest_city = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_city',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.city',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/city.parquet'],
    dag=dag
)

ingest_country = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_country',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.country',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/country.parquet'],
    dag=dag
)

ingest_color = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_color',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.color',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/color.parquet'],
    dag=dag
)

ingest_saleOrders = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_saleOrders',
    conn_id='spark_default',
    application_args=['--source-table', "(select * from oltp.saleOrders where orderDate = '2021-01-08') a",
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/saleOrders.parquet'],
    dag=dag
)

ingest_orderDetails = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_orderDetails',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.orderDetails',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/orderDetails.parquet'],
    dag=dag
)

ingest_customer = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_customer',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.customers',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/customers.parquet'],
    dag=dag
)

ingest_promotions = SparkSubmitOperator(
    application='/opt/airflow/dags/Ingestion/ingestion.py',
    task_id='ingest_promotions',
    conn_id='spark_default',
    application_args=['--source-table', 'oltp.promotions',
                      '--dest-path', 'hdfs://hadoop-namenode:9000/datalake/raw/promotions.parquet'],
    dag=dag
)

