from pyspark.sql.functions import col
from lib.scdhandler import SCDHandler
from lib.utils import get_spark_app_config
from lib.constant import SCHEMA, EOW_DATE, DATE_FORMAT
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get dim product") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    product_df = spark.sql("select * from bronze.product")

    color_df = spark.sql("select * from bronze.color")

    category_df = spark.sql("select * from bronze.category")
    category_alias = category_df.alias("ct")
    category_df = category_df.join(category_alias, col("category.parentCategoryID") == col("ct.categoryID"), "left") \
        .select(col("category.categoryID").alias("category_id"),
                col("category.categoryName").alias("category_name"),
                col("ct.categoryName").alias("parent_category_name"))

    dim_product_current = product_df.join(color_df, product_df.colorID == color_df.colorID, "left") \
        .join(category_df, product_df.categoryID == category_df.category_id, "left") \
        .withColumnRenamed("productID", "product_id") \
        .withColumnRenamed("productName", "product_name") \
        .withColumnRenamed("productDescription", "product_description") \
        .withColumnRenamed("originalPrice", "original_price") \
        .withColumnRenamed("sellingPrice", "selling_price") \
        .withColumnRenamed("avail", "avail") \
        .withColumnRenamed("productSize", "product_size") \
        .withColumnRenamed("productLine", "product_line") \
        .withColumnRenamed("colorName", "color_name") \
        .withColumnRenamed("category_name", "category_name") \
        .withColumnRenamed("parent_category_name", "parent_category_name") \
        .select("product_id", "product_name", "original_price", "selling_price",
                "avail", "product_size", "product_line", "color_name", "category_name", "parent_category_name")

    dim_product_history = spark.sql(f"select * from {SCHEMA}.dim_product")

    scdHandler = SCDHandler(EOW_DATE, DATE_FORMAT)
    scdHandler.enrich_to_scd2(
        dim_product_current,
        dim_product_history,
        ["product_name", "original_price", "selling_price", "avail", "product_size",
         "product_line", "color_name", "category_name", "parent_category_name"],
        "product_id",
        "product_sk"
    ).createOrReplaceTempView("dim_product_final")

    spark.sql("select * from dim_product_final").write \
        .mode("overwrite") \
        .insertInto(f"{SCHEMA}.dim_product")




