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

    spark.sql(f"DROP TABLE IF EXISTS {schema}.dim_product")
    spark.sql(f"""
    CREATE TABLE {schema}.dim_product (
        product_sk INT,
        product_id STRING,
        product_name STRING,
        original_price DECIMAL(10, 2),
        selling_price DECIMAL(10, 2),
        avail STRING,
        product_size STRING,
        product_line STRING,
        color_name STRING,
        category_name STRING,
        parent_category_name STRING,
        effective_date DATE,
        expiration_date DATE,
        current_flag INT)
    """)

    spark.sql(f"DROP TABLE IF EXISTS {schema}.dim_date")
    spark.sql(f"""
        CREATE TABLE {schema}.dim_date (
        date_key INT,
        full_date DATE,
        day_of_week INT,
        day_num_in_month INT,
        day_num_overall INT,
        day_name STRING,
        day_abbr STRING,
        weekend_flag STRING,
        week_num_in_year INT,
        week_num_overall INT,
        week_begin_date DATE,
        week_begin_date_key INT,
        month INT,
        month_num_overall INT,
        month_name STRING,
        month_abbr STRING,
        quarter INT,
        year INT,
        year_month INT,
        fiscal_month INT,
        fiscal_quarter INT,
        fiscal_year INT,
        month_end_flag STRING,
        day_year_ago DATE
        )
    """)

    spark.sql(f"DROP TABLE IF EXISTS {schema}.fct_sale")
    spark.sql(f"""
        CREATE TABLE {schema}.fct_sale (
            date_key INT,
            employee_sk INT,
            customer_sk INT,
            product_sk INT,
            branch_sk INT,
            promotion_sk INT,
            quantity INT,
            discount DECIMAL(10, 2),
            unit_price DECIMAL(15, 2),
            sub_total DECIMAL(20, 2)
        )
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
