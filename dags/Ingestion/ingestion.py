from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
from lib.utils import get_spark_app_config, get_mysql_source_db_config
import argparse


def mysql_reader(
        spark: SparkSession,
        source_table: str,
) -> DataFrame:
    mysql_conf = get_mysql_source_db_config()

    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", f"jdbc:mysql://{mysql_conf['host']}:{mysql_conf['port']}") \
        .option("dbtable", source_table) \
        .option("user", mysql_conf['user']) \
        .option("password", mysql_conf['password']) \
        .load()
    return df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .config(conf=get_spark_app_config()) \
        .enableHiveSupport() \
        .getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion-date", help="", default="", type=str)
    parser.add_argument("--date-format", help="", default="%Y-%m-%d", type=str)
    args = parser.parse_args()

    print("---------------------------------------------------------")
    print(f"args: {args}")
    ingestion_date = ""
    if args.ingestion_date == "":
        ingestion_date = datetime.strptime(args.ingestion_date, args.date_format)

    try:
        ingestion_date = datetime.strptime(args.ingestion_date, args.date_format)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
    print("---------------------------------------------------------")
    print(f"Ingestion date: {ingestion_date}")
    Bronze_Tables = {
        "address": {
            "source_table": "oltp.address",
            "ingestion_type": "full",
            "destination_table": "bronze.address",
        },
        "branch": {
            "source_table": "oltp.branch",
            "ingestion_type": "full",
            "destination_table": "bronze.branch",
        },
        "category": {
            "source_table": "oltp.category",
            "ingestion_type": "full",
            "destination_table": "bronze.category",
        },
        "color": {
            "source_table": "oltp.color",
            "ingestion_type": "full",
            "destination_table": "bronze.color",
        },
        "city": {
            "source_table": "oltp.city",
            "ingestion_type": "full",
            "destination_table": "bronze.city",
        },
        "country": {
            "source_table": "oltp.country",
            "ingestion_type": "full",
            "destination_table": "bronze.country",
        },
        "customer": {
            "source_table": "oltp.customer",
            "ingestion_type": "full",
            "destination_table": "bronze.customer",
        },
        "employee": {
            "source_table": "oltp.employee",
            "ingestion_type": "full",
            "destination_table": "bronze.employee",
        },
        "product": {
            "source_table": "oltp.product",
            "ingestion_type": "full",
            "destination_table": "bronze.product",
        },
        "promotion": {
            "source_table": "oltp.promotion",
            "ingestion_type": "full",
            "destination_table": "bronze.promotion",
        },
        "saleOrder": {
            "source_table": "oltp.saleOrder",
            "ingestion_type": "incremental",
            "destination_table": "bronze.saleOrder",
            "incremental_query": f"""
                (select * 
                from oltp.saleOrder
                where orderDate = '{ingestion_date}') a"""
        },
        "orderDetail": {
            "source_table": "oltp.orderDetail",
            "ingestion_type": "incremental",
            "destination_table": "bronze.orderDetail",
            "incremental_query": f"""
                (select * 
                from oltp.orderDetail 
                where orderID IN (
                    select orderID 
                    from oltp.saleOrder 
                    where orderDate = '{ingestion_date}')
                ) b"""
        }
    }

    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze")

    for table, table_props in Bronze_Tables.items():
        if table_props["ingestion_type"] == "full":
            df = mysql_reader(spark, table_props["source_table"])
        else:
            df = mysql_reader(spark, table_props["incremental_query"])

        df.write \
            .mode("overwrite") \
            .option("format", "hive") \
            .option("parquet.block.size", 32 * 1024 * 1024) \
            .saveAsTable(table_props["destination_table"])



