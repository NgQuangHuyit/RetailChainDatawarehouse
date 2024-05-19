from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config
from pyspark.sql.functions import concat, row_number, to_date, current_date, lit, col, when
from pyspark.sql.window import Window
from lib.constant import EOW_DATE, SCD_TYPE_2_COLS, SCHEMA, DATE_FORMAT
from lib.utils import column_rename, get_hash

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Get Dim Customer") \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    city_df = spark.sql("select * from bronze.city")

    country_df = spark.sql("select * from bronze.country")

    customer_df = spark.sql("select * from bronze.customer")

    dim_customer_current = customer_df.join(city_df, customer_df.cityID == city_df.cityID, "inner") \
        .join(country_df, city_df.countryID == country_df.countryID, "inner") \
        .withColumn("fullname", concat(col("firstName"), lit(" "), col("lastName"))) \
        .select("customerID", "fullname", "phone", "email", "city", "country") \
        .withColumnRenamed("customerID", "customer_id")

    dim_customer_history = spark.sql("select * from silver.dim_customer")

    max_sk = dim_customer_history.agg({"customer_sk": "max"}).collect()[0][0]
    if max_sk is None:
        max_sk = 1

    dim_customer_history_open = dim_customer_history \
        .where(col("current_flag") == lit(1))
    dim_customer_history_closed = dim_customer_history \
        .where(col("current_flag") == lit(0))

    type2_cols = ["phone", "email", "city", "country"]

    dim_customer_history_open_hash = column_rename(
        get_hash(dim_customer_history_open, type2_cols, "hash_md5"), "_history", True)

    dim_customer_current_hash = column_rename(
        get_hash(dim_customer_current, type2_cols, "hash_md5"), "_current", True)

    dim_customer_merged = dim_customer_current_hash \
        .join(dim_customer_history_open_hash, col("customer_id_current") == col("customer_id_history"), "full_outer") \
        .withColumn("action", when(col("hash_md5_current") == col("hash_md5_history"), "nochange")
                    .when(col("hash_md5_current") != col("hash_md5_history"), "update")
                    .when(col("customer_id_history").isNull(), "insert")
                    .when(col("customer_id_current").isNull(), "delete"))

    dim_customer_nochange = column_rename(dim_customer_merged.filter(col("action") == "nochange"), "_history", False) \
        .select(dim_customer_history.columns)

    # dim_customer_nochange.orderBy("customer_sk").show(30)

    dim_customer_insert = column_rename(dim_customer_merged.where(col("action") == "insert"), "_current", False) \
        .select(dim_customer_current.columns) \
        .withColumn("effective_date", current_date()) \
        .withColumn("expiration_date", to_date(lit(EOW_DATE), DATE_FORMAT)) \
        .withColumn("row_number", row_number().over(Window.orderBy("customer_id"))) \
        .withColumn("customer_sk", col("row_number") + max_sk) \
        .withColumn("current_flag", lit(1)) \
        .select(dim_customer_history.columns)

    # dim_customer_insert.show()

    max_sk = dim_customer_insert.agg({"customer_sk": "max"}).collect()[0][0]

    dim_customer_delete = column_rename(dim_customer_merged.where(col("action") == "delete"), "_history", False) \
        .select(dim_customer_history_open.columns) \
        .withColumn("expiration_date", current_date()) \
        .withColumn("current_flag", lit(0)) \

    # dim_customer_delete.show()

    dim_customer_update = column_rename(dim_customer_merged.where(col("action") == "update"), "_history", False) \
        .select(dim_customer_history_open.columns) \
        .withColumn("expiration_date", current_date()) \
        .withColumn("current_flag", lit(0)) \
        .unionByName(
        column_rename(dim_customer_merged.where(col("action") == "update"), "_current", False)
            .select(dim_customer_current.columns)
            .withColumn("effective_date", current_date())
            .withColumn("expiration_date", to_date(lit(EOW_DATE), DATE_FORMAT))
            .withColumn("row_number", row_number().over(Window.orderBy("customer_id")))
            .withColumn("customer_sk", col("row_number") + max_sk)
            .withColumn("current_flag", lit(1))
            .select(dim_customer_history.columns)
    )

    # dim_customer_update.show()
    # dim_customer_history_closed.show()

    dim_customer_history_closed \
        .unionByName(dim_customer_nochange) \
        .unionByName(dim_customer_insert) \
        .unionByName(dim_customer_delete) \
        .unionByName(dim_customer_update) \
        .createOrReplaceTempView("dim_customer_final_tmp")

    dim_customer_final = spark.sql("select * from dim_customer_final_tmp")

    # dim_customer_final.printSchema()
    # dim_customer_final.show()

    dim_customer_final.write \
        .mode("overwrite") \
        .format("hive") \
        .insertInto(f"{SCHEMA}.dim_customer")

    # win_spec = Window.orderBy("customerID")
    #
    # dim_customer = dim_customer.withColumn("customer_sk", row_number().over(win_spec)) \
    #     .withColumn("effective_date", current_date()) \
    #     .withColumn("expiration_date", to_date(lit(EOW_DATE), DATE_FORMAT)) \
    #     .withColumn("fullname", concat(col("firstName"), lit(" "),col("lastName"))) \
    #     .withColumn("current_flag", lit(1)) \
    #     .withColumn("customer_id", col("customerID")) \
    #     .select("customer_sk", "customer_id", "fullname", "phone", "email",
    #             "city", "country", "effective_date", "expiration_date", "current_flag")
    # dim_customer.show()
    # dim_customer.printSchema()
    # dim_customer.write.mode("append").format("hive").saveAsTable("silver.dim_customer")
