from pyspark.sql import SparkSession


def create_tables(
        spark : SparkSession,
        storage_path : str = 'hdfs://hadoop-namenode:9000/user/hive/warehouse',
        database: str = 'bronze',
):
    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    spark.sql("show databases").show()

    # Create products table
    spark.sql(f"DROP TABLE IF EXISTS {database}.products")
    spark.sql(f"""
    CREATE TABLE {database}.products (
        product_id STRING, 
        product_name STRING,
        product_description STRING,
        original_price DOUBLE,
        selling_price DOUBLE,
        avail String,
        product_size STRING,
        product_line STRING,
        color_id STRING,
        category_id STRING)
        USING PARQUET
        LOCATION '{storage_path}/products'
    """)

    # Create address table

    spark.sql(f"DROP TABLE IF EXISTS {database}.address")
    spark.sql(f"""
    CREATE TABLE {database}.address (
        address_id SMALLINT,
        address STRING,
        address2 STRING,
        district STRING,
        city_id SMALLINT,
        postal_code STRING)
        USING PARQUET
        LOCATION '{storage_path}/address'
    """)

    # Create branches table
    spark.sql(f"DROP TABLE IF EXISTS {database}.branch")
    spark.sql(f"""
    CREATE TABLE {database}.branches (
        branch_id STRING,
        branch_name STRING,
        phone STRING, 
        email STRING,
        address_id SMALLINT)
        USING PARQUET
        LOCATION '{storage_path}/branch'
    """)

    # Create category table
    spark.sql(f"DROP TABLE IF EXISTS {database}.category")
    spark.sql(f"""
    CREATE TABLE {database}.category (
        category_id STRING,
        category_name STRING,
        parent_category_id STRING)
        USING PARQUET
        LOCATION '{storage_path}/category'
    """)

    spark.sql(f"DROP TABLE IF EXISTS {database}.city")
    spark.sql(f"""
    CREATE TABLE {database}.city (
        city_id SMALLINT,
        city_name STRING,
        country_id SMALLINT)
        USING PARQUET
        LOCATION '{storage_path}/city'
    """)

    # Create color table
    spark.sql(f"DROP TABLE IF EXISTS {database}.color")
    spark.sql(f"""
    CREATE TABLE {database}.color (
        color_id STRING,
        color_name STRING, 
        rgb_code STRING,
        hex_code STRING)
        USING PARQUET
        LOCATION '{storage_path}/color'
    """)

    # Create country table
    spark.sql(f"DROP TABLE IF EXISTS {database}.country")
    spark.sql(f"""
    CREATE TABLE {database}.country (
        country_id SMALLINT,
        country_name STRING)
        USING PARQUET
        LOCATION '{storage_path}/country'
    """)

    # Create customer table
    spark.sql(f"DROP TABLE IF EXISTS {database}.customer")
    spark.sql(f"""
    CREATE TABLE {database}.customer (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        phone STRING,
        email STRING,
        city_id SMALLINT)
        USING PARQUET
        LOCATION '{storage_path}/customer'
    """)

    # Create employee table
    spark.sql(f"DROP TABLE IF EXISTS {database}.employee")
    spark.sql(f"""
    CREATE TABLE {database}.employee (
        employee_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        position STRING,
        hire_date DATE,
        branch_id STRING)
        USING PARQUET
        LOCATION '{storage_path}/employee'
    """)

    # Create orderDetail table
    spark.sql(f"DROP TABLE IF EXISTS {database}.orderDetail")
    spark.sql(f"""
    CREATE TABLE {database}.orderDetail (
        orderDetail_id INTEGER,
        order_id STRING,
        product_id STRING,
        quantity SMALLINT,
        discount DOUBLE,
        unit_price DOUBLE,
        sub_total DOUBLE)
        USING PARQUET
        LOCATION '{storage_path}/orderDetail'
    """)

    # Create order table
    spark.sql(f"DROP TABLE IF EXISTS {database}.order")
    spark.sql(f"""
    CREATE TABLE {database}.order (
        order_id STRING,
        promotion_id STRING,
        customer_id STRING,
        employee_id STRING,
        branch_id STRING,
        order_date DATE,
        total_amount DOUBLE)
        USING PARQUET
        LOCATION '{storage_path}/order'
    """)

    # Create promotion table
    spark.sql(f"DROP TABLE IF EXISTS {database}.promotion")
    spark.sql(f"""
    CREATE TABLE {database}.promotion (
        promotion_id STRING,
        promotion_name STRING,
        promotion_description STRING,
        start_date DATE,
        end_date DATE,
        adsMediaType STRING,
        promotion_type STRING)
        USING PARQUET
        LOCATION '{storage_path}/promotion'
    """)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Create Tables") \
        .config("spark.executor.core", "1") \
        .config("spark.executor.instances", "1") \
        .master("yarn") \
        .getOrCreate()
    spark.sql("show databases").show()
    create_tables(spark)
    spark.stop()