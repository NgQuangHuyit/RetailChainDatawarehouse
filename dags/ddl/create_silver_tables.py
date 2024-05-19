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

    # Dim employee
    spark.sql(f"DROP TABLE IF EXISTS {schema}.dim_employee")

    spark.sql(f"""
    CREATE TABLE {schema}.dim_employee (
        employee_sk INT,
        employee_id STRING,
        fullname STRING,
        position STRING,
        email STRING,
        phone STRING,
        branch_name STRING,
        hire_date DATE,
        effective_date DATE,
        expiration_date DATE,
        current_flag INT)
    """)

    spark.sql(f"DROP TABLE IF EXISTS {schema}.dim_promotion")
    spark.sql(f"""
    CREATE TABLE {schema}.dim_promotion (
        promotion_sk INT,
        promotion_id STRING,
        promotion_name STRING,
        promotion_description STRING,
        start_date DATE,
        end_date DATE,
        ads_media_type STRING,
        promotion_type STRING)
    """)

    spark.sql(f"DROP TABLE IF EXISTS {schema}.dim_branch")

    spark.sql(f"""
    CREATE TABLE {schema}.dim_branch (
        branch_sk INT,
        branch_id STRING,
        phone STRING,
        email STRING,
        branch_name STRING,
        address STRING,
        postal_code STRING,
        district STRING,
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