from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("create hive db") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("create database demo").show()
spark.stop()