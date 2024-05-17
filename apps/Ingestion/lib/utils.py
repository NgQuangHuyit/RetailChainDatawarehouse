import configparser
from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def get_mysql_source_db_config():
    config = configparser.ConfigParser()
    config.read("ingestion.conf")
    conf = {}
    for (key, val) in config.items("MYSQL_SOURCE_DB_CONFIGS"):
        conf[key] = val
    return conf
