from pyspark.sql.functions import concat, col, lit
from lib.scdhandler import SCDHandler
from lib.utils import get_spark_app_config
from lib.constant import SCHEMA, EOW_DATE, DATE_FORMAT
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get dim customer") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    city_df = spark.sql("select * from bronze.city")

    country_df = spark.sql("select * from bronze.country")

    customer_df = spark.sql("select * from bronze.customer")

    dim_customer_current = customer_df.join(city_df, customer_df.cityID == city_df.cityID, "left") \
        .join(country_df, city_df.countryID == country_df.countryID, "left") \
        .withColumn("fullname", concat(col("firstName"), lit(" "), col("lastName"))) \
        .select("customerID", "fullname", "phone", "email", "city", "country") \
        .withColumnRenamed("customerID", "customer_id")

    dim_customer_history = spark.sql(f"select * from {SCHEMA}.dim_customer")

    schHandler = SCDHandler(EOW_DATE, DATE_FORMAT)

    schHandler.enrich_to_scd2(
        dim_customer_current,
        dim_customer_history,
        ["phone", "email", "city", "country"],
        "customer_id",
        "customer_sk").createOrReplaceTempView("dim_customer_final")

    spark.sql(f"select * from dim_customer_final") \
        .write \
        .mode("overwrite") \
        .insertInto(f"{SCHEMA}.dim_customer")
    spark.stop()
