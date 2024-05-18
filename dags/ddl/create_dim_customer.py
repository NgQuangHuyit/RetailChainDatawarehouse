from pyspark.sql import SparkSession


def create_tables(
        spark: SparkSession,
        path: str = 'hdfs://hadoop-namenode:9000/datalake/raw/',
        schema: str = 'oltp',
):
    return None
