from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Create Tables") \
        .config("spark.executor.core", "1") \
        .config("spark.executor.instances", "1") \
        .getOrCreate()

