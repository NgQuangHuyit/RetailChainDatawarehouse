import sys
from pyspark.sql import SparkSession
from lib.logger import Log4j
from os.path import join
from os import getcwd
from lib.utils import load_survey_df

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master('yarn') \
            .appName("spark application").getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting HelloSpark")
    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql-oltp:3306") \
        .option("dbtable", "oltp.saleOrders") \
        .option("user", "root") \
        .option("password", "root") \
        .load()
    logger.info("Loaded data from MySQL")
    
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "hdfs://hadoop-namenode:9000/datalake/raw/orders.parquet") \
        .option("parquet.block.size", 32 * 1024 * 1024) \
        .save()
    logger.info("Saved data to HDFS")
    logger.info("Finished HelloSpark")
    spark.stop()
    