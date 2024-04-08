from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .appName("Hive test") \
            .enableHiveSupport() \
            .config("spark.warehouse.dir", "hdfs://hadoop-namenode:9000/user/hive/warehouse") \
            .getOrCreate()

    spark.sql("show databases").show()

    print(spark.version)
    spark.stop()