from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, current_date, to_date, row_number, when
from .utils import get_hash, column_rename


class SCDHandler:
    def __init__(self, eow_date: str = '9999-12-31', date_format: str = 'yyyy-MM-dd'):
        self.EOW_DATE = eow_date
        self.DATE_FORMAT = date_format

    def enrich_to_scd2(
            self,
            current_df: DataFrame,
            history_df: DataFrame,
            type2_cols: list[str],
            natural_key: str,
            surrogate_key: str
    ) -> DataFrame:
        max_sk = history_df.agg({surrogate_key: "max"}).collect()[0][0]
        if max_sk is None:
            max_sk = 0

        history_df_open = history_df.where(col("current_flag") == lit(1))
        history_df_closed = history_df.where(col("current_flag") == lit(0))

        history_df_open_hash = column_rename(
            get_hash(history_df_open, type2_cols, "hash_md5"),
            "_history",
            True)

        current_df_hash = column_rename(
            get_hash(current_df, type2_cols, "hash_md5"),
            "_current",
            True)

        merged_df = current_df_hash \
            .join(history_df_open_hash, col(natural_key + "_current") == col(natural_key + "_history"), "full_outer") \
            .withColumn("action", when(col("hash_md5_current") == col("hash_md5_history"), "nochange")
                        .when(col("hash_md5_current") != col("hash_md5_history"), "update")
                        .when(col(natural_key + "_history").isNull(), "insert")
                        .when(col(natural_key + "_current").isNull(), "delete"))
        df_nochange = column_rename(merged_df.filter(col("action") == "nochange"), "_history", False) \
            .select(history_df.columns)

        df_insert = column_rename(merged_df.where(col("action") == "insert"), "_current", False) \
            .select(current_df.columns) \
            .withColumn("effective_date", current_date()) \
            .withColumn("expiration_date", to_date(lit(self.EOW_DATE), self.DATE_FORMAT)) \
            .withColumn("current_flag", lit(1)) \
            .withColumn("row_number", row_number().over(Window.orderBy(natural_key))) \
            .withColumn(surrogate_key, col("row_number") + max_sk) \
            .select(history_df.columns)

        max_insert_sk = df_insert.agg({surrogate_key: "max"}).collect()[0][0]
        if max_insert_sk is not None:
            max_sk = max_insert_sk

        df_update = column_rename(merged_df.where(col("action") == "update"), "_history", False) \
            .select(history_df_open.columns) \
            .withColumn("expiration_date", current_date()) \
            .withColumn("current_flag", lit(0)) \
            .unionByName(
            column_rename(merged_df.where(col("action") == "update"), "_current", False)
            .select(current_df.columns)
            .withColumn("effective_date", current_date())
            .withColumn("expiration_date", to_date(lit(self.EOW_DATE), self.DATE_FORMAT))
            .withColumn("row_number", row_number().over(Window.orderBy(natural_key)))
            .withColumn(surrogate_key, col("row_number") + max_sk)
            .withColumn("current_flag", lit(1))
            .select(history_df.columns)
        )

        df_delete = column_rename(merged_df.where(col("action") == "delete"), "_history", False) \
            .withColumn("expiration_date", current_date()) \
            .withColumn("current_flag", lit(0)) \
            .select(history_df_open.columns)

        df_final = history_df_closed \
            .unionByName(df_nochange) \
            .unionByName(df_insert) \
            .unionByName(df_update) \
            .unionByName(df_delete) \
            .orderBy(surrogate_key)

        return df_final

    def enrich_to_scd1(
            self,
            current_df: DataFrame,
            history_df: DataFrame,
            type1_cols: list[str],
            natural_key: str,
            surrogate_key: str
    ):
        max_sk = history_df.agg({surrogate_key: "max"}).collect()[0][0]
        if max_sk is None:
            max_sk = 0

        current_df_hash = column_rename(
            get_hash(current_df, type1_cols, "hash_md5"),
            "_current",
            True)

        history_df_hash = column_rename(
            get_hash(history_df, type1_cols, "hash_md5"),
            "_history",
            True)

        merged_df = current_df_hash \
            .join(history_df_hash, col(natural_key + "_current") == col(natural_key + "_history"), "full_outer") \
            .withColumn("action", when(col("hash_md5_current") == col("hash_md5_history"), "nochange")
                        .when(col("hash_md5_current") != col("hash_md5_history"), "update")
                        .when(col(natural_key + "_history").isNull(), "insert"))

        df_nochange = column_rename(merged_df.filter(col("action") == "nochange"), "_history", False) \
            .select(history_df.columns)

        df_insert = column_rename(merged_df.where(col("action") == "insert"), "_current", False) \
            .select(current_df.columns) \
            .withColumn("row_number", row_number().over(Window.orderBy(natural_key))) \
            .withColumn(surrogate_key, col("row_number") + max_sk) \
            .select(history_df.columns)

        df_update = column_rename(merged_df.where(col("action") == "update"), "_current", False) \
            .withColumn(surrogate_key, col(surrogate_key + "_history")) \
            .select(history_df.columns)

        df_final = df_nochange \
            .unionByName(df_insert) \
            .unionByName(df_update)\
            .orderBy(surrogate_key)
        return df_final
