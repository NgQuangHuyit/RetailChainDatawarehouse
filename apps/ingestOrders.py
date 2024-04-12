from pyspark.sql import SparkSession
from os import getcwd
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ingestOrders") \
        .getOrCreate()


    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql-oltp:3306") \
        .option("dbtable", "oltp.saleOrders") \
        .option("user", "root") \
        .option("password", "root") \
        .load()
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "hdfs://hadoop-namenode:9000/datalake/raw/orders.parquet") \
        .option("parquet.block.size", 32 * 1024 * 1024) \
        .save()
    
    spark.stop()
    
