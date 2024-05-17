from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config, get_mysql_source_db_config
import argparse

if __name__ == "__main__":
    spark = SparkSession.builder \
        .config(conf=get_spark_app_config()) \
        .getOrCreate()

    mysqlConf = get_mysql_source_db_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", help="mysql source table to ingest", type=str)
    parser.add_argument("--dest-path", help="destination path to write", type=str)
    args = parser.parse_args()

    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", f"jdbc:mysql://{mysqlConf['host']}:{mysqlConf['port']}") \
        .option("dbtable", args.source_table) \
        .option("user", mysqlConf['user']) \
        .option("password", mysqlConf['password']) \
        .load()

    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", args.dest_path) \
        .option("parquet.block.size", 32 * 1024 * 1024) \
        .save()


