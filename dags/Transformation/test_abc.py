from pyspark.sql.functions import concat, col, lit, row_number
from lib.scdhandler import SCDHandler
from lib.utils import get_spark_app_config
from lib.constant import SCHEMA, EOW_DATE, DATE_FORMAT
from pyspark.sql import SparkSession, Window

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get dim employee") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    branch_df = spark.sql("select * from bronze.branch")
    address_df = spark.sql("select * from bronze.address")

    city_df = spark.sql("select * from bronze.city")

    country_df = spark.sql("select * from bronze.country")

    dim_branch_current = branch_df.join(address_df, branch_df.addressID == address_df.addressID, "left") \
        .join(city_df, address_df.cityID == city_df.cityID, "left") \
        .join(country_df, city_df.countryID == country_df.countryID, "left") \
        .withColumnRenamed("branchID", "branch_id") \
        .withColumnRenamed("branchName", "branch_name") \
        .withColumnRenamed("postalCode", "postal_code") \
        .select("branch_id", "phone", "email",
                "branch_name", "address", "postal_code",
                "district", "city", "country")

    dim_branch_history = spark.sql(f"select * from {SCHEMA}.dim_branch")

    scdHandler = SCDHandler(EOW_DATE, DATE_FORMAT)
    scdHandler.enrich_to_scd2(
        dim_branch_current,
        dim_branch_history,
        ["phone", "email", "branch_name", "address", "postal_code", "district", "city", "country"],
        "branch_id",
        "branch_sk").createOrReplaceTempView("dim_branch_final")

    spark.sql(f"select * from dim_branch_final") \
        .write \
        .mode("overwrite") \
        .insertInto(f"{SCHEMA}.dim_branch")



