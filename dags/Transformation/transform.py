from pyspark.sql import  SparkSession
from pyspark.sql.functions import col
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    spark = SparkSession.builder \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    city_df = spark.sql("select * from bronze.city")

    city_df.show()

    country_df = spark.sql("select * from bronze.country")

    country_df.show()

    customer_df = spark.sql("select * from bronze.customer")

    customer_df.show()
    dim_customer = customer_df.join(city_df, customer_df.cityID == city_df.cityID, "inner") \
        .join(country_df, city_df.countryID == country_df.countryID, "inner") \

    dim_customer.show()