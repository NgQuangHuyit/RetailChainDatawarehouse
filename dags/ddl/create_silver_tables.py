from pyspark.sql import SparkSession


def create_tables(spark: SparkSession, schema: str = 'silver'):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql(f"USE {schema}")

    spark.sql(f"DROP TABLE IF EXISTS {schema}.dim_customer")

    spark.sql("""
    CREATE TABLE silver.dim_customer (
        customer_sk INT,
        customer_id STRING,
        fullname STRING,
        phone STRING,
        email STRING,
        city STRING,
        country STRING,
        effective_date DATE,
        expiration_date DATE,
        current_flag INT)
    """)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Create Silver Tables") \
        .config("spark.executor.core", "1") \
        .config("spark.executor.instances", "1") \
        .enableHiveSupport() \
        .master("yarn") \
        .getOrCreate()

    create_tables(spark)

    spark.stop()