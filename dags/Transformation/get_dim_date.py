from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from lib.utils import get_spark_app_config
from lib.constant import SCHEMA, EOW_DATE, DATE_FORMAT
from pyspark.sql.functions import cast, to_date, col

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get dim date") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    schema = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("full_date", StringType()),
        StructField("day_of_week", IntegerType()),
        StructField("day_num_in_month", IntegerType()),
        StructField("day_num_overall", IntegerType()),
        StructField("day_name", StringType()),
        StructField("day_abbr", StringType()),
        StructField("weekend_flag", StringType()),
        StructField("week_num_in_year", IntegerType()),
        StructField("week_num_overall", IntegerType()),
        StructField("week_begin_date", StringType()),
        StructField("week_begin_date_key", IntegerType()),
        StructField("month", IntegerType()),
        StructField("month_num_overall", IntegerType()),
        StructField("month_name", StringType()),
        StructField("month_abbr", StringType()),
        StructField("quarter", IntegerType()),
        StructField("year", IntegerType()),
        StructField("year_month", IntegerType()),
        StructField("fiscal_month", IntegerType()),
        StructField("fiscal_quarter", IntegerType()),
        StructField("fiscal_year", IntegerType()),
        StructField("month_end_flag", StringType()),
        StructField("day_year_ago", StringType())
    ])
    dim_date = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("path", "hdfs://hadoop-namenode:9000/tmp/dim_date.csv") \
        .schema(schema) \
        .load()

    dim_date.withColumn("full_date", to_date(col("full_date"), "d/m/yyyy")) \
        .withColumn("week_begin_date", to_date(col("week_begin_date"), "d/m/yyyy")) \
        .withColumn("day_year_ago", to_date(col("day_year_ago"), "d/m/yyyy")) \
        .createOrReplaceTempView("dim_date")

    spark.sql(f"select * from dim_date") \
        .write \
        .mode("overwrite") \
        .insertInto(f"{SCHEMA}.dim_date")