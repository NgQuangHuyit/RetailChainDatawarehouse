from pyspark.sql import SparkSession


if __name__ == "__main__":

        spark = SparkSession.builder \
                .master("spark://spark-master:7077") \
                .appName("Ingestion") \
                .config("spark.hadoop.fs.block.size", "33554432") \
                .getOrCreate()

        df = spark.read \
                .format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://mysql-oltp:3306") \
                .option("dbtable", "oltp.orderDetails") \
                .option("user", "root") \
                .option("password", "root") \
                .load()
        df.show()
        df.write \
                .format("parquet") \
                .mode("overwrite") \
                .save("hdfs://hadoop-namenode:9000/datalake/orderDetails.parquet")

        print(spark.version)
        spark.stop()
    