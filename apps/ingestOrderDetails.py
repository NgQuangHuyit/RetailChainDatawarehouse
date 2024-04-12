from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.Builder \
        .appName("ingest OrderDetails") \
        