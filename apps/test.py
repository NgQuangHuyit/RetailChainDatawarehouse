from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").enableHiveSupport().getOrCreate()

spark.sql("show databases").show()
spark.stop()