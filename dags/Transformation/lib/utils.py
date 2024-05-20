import configparser
from pyspark import SparkConf
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import md5, concat, col, lit


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("/opt/airflow/dags/Ingestion/ingestion.ini")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def column_rename(df: DataFrame, suffix: str, append: bool):
    """
    Args:
        df: dataframe
        suffix: suffix that will be added or removed
        append: boolean value
            true if suffix will be added, false if suffix will be removed
    Returns: dataframe
    """
    if append:
        for col in df.columns:
            df = df.withColumnRenamed(col, col + suffix)
    else:
        for col in df.columns:
            df = df.withColumnRenamed(col, col.replace(suffix, ""))
    return df


def get_hash(df: DataFrame, cols: list[str], hash_col_name: str):
    """
    Args:
        df: dataframe
        cols: list of columns name that will be hashed
        hash_col_name: name of the new column that will store the hash value
    Returns: dataframe
    """
    columns = [col(c) for c in cols]
    if len(columns) > 1:
        df = df.withColumn(hash_col_name, md5(concat(*columns)))
    else:
        df = df.withColumn(hash_col_name, lit(1))
    return df
