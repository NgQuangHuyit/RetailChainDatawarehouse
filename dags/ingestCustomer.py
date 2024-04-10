from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Ingest Customer Data") \
        .getOrCreate()
    
    df = spark.read \
        .format("jdbc") \
        .driver("com.mysql.jdbc.Driver") \
        .url("jdbc:mysql://mysql-oltp:3306/oltp") \
        .option("dbtable", "customer") \
        .option("user", "root") \
        .option("password", "root") \
        .load()
    
    df.show()

    df.write \
        .format("parquet") \
        .option("parquet.block.size", 32*1024*1024) \
        .option("path", "hdfs://hadoop-namenode:9000/datalake/raw/customer.parquet")\
        .mode("overwrite") \
        .option("parquet.block.size", 32 * 1024 * 1024) \
        .save()
    