from pyspark.sql import  SparkSession
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    spark = SparkSession.builder \
        .config(conf=get_spark_app_config()) \
        .getOrCreate()

    product = spark.read \
        .option('format', 'parquet') \
        .option()