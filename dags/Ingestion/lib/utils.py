import configparser
from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("/opt/airflow/dags/Ingestion/ingestion.ini")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def get_mysql_source_db_config():
    config = configparser.ConfigParser()
    config.read("/opt/airflow/dags/Ingestion/ingestion.ini")
    conf = {}
    for (key, val) in config.items("MYSQL_SOURCE_CONFIGS"):
        conf[key] = val
    return conf
