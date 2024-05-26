from lib.scdhandler import SCDHandler
from lib.utils import get_spark_app_config
from lib.constant import SCHEMA, EOW_DATE, DATE_FORMAT
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get dim promotion") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    promotion_df = spark.sql("select * from bronze.promotion")

    dim_promotion_current = promotion_df.withColumnRenamed("promotionID", "promotion_id") \
        .withColumnRenamed("promotionName", "promotion_name") \
        .withColumnRenamed("promotionDescription", "promotion_description") \
        .withColumnRenamed("startDate", "start_date") \
        .withColumnRenamed("endDate", "end_date") \
        .withColumnRenamed("adsMediaType", "ads_media_type") \
        .withColumnRenamed("promotionType", "promotion_type")

    dim_promotion_history = spark.sql(f"select * from {SCHEMA}.dim_promotion")

    scdHandler = SCDHandler(EOW_DATE, DATE_FORMAT)
    scdHandler.enrich_to_scd1(
        dim_promotion_current,
        dim_promotion_history,
        ["promotion_name", "promotion_description", "start_date", "end_date", "ads_media_type", "promotion_type"],
        "promotion_id",
        "promotion_sk"
    ).createOrReplaceTempView("dim_promotion_final")

    spark.sql(f"select * from dim_promotion_final") \
        .write \
        .mode("overwrite") \
        .insertInto(f"{SCHEMA}.dim_promotion")
    spark.stop()




