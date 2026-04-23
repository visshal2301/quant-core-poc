"""Reusable Silver-layer helper functions."""

from pyspark.sql import Window
from pyspark.sql import functions as F

OPEN_ENDED_TS = "9999-12-31 23:59:59"
OPEN_ENDED_DT = "9999-12-31"


def add_surrogate_key(df, business_key, surrogate_key):
    window_spec = Window.orderBy(F.col(business_key))
    return df.withColumn(surrogate_key, F.row_number().over(window_spec))


def apply_bitemporal_columns(df, valid_from_col, valid_to_expr=None):
    valid_to_value = valid_to_expr if valid_to_expr is not None else F.to_timestamp(F.lit(OPEN_ENDED_TS))
    return (
        df.withColumn("valid_from_ts", F.col(valid_from_col).cast("timestamp"))
        .withColumn("valid_to_ts", valid_to_value)
        .withColumn("system_from_ts", F.current_timestamp())
        .withColumn("system_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
        .withColumn("is_current_valid", F.lit(True))
        .withColumn("is_current_system", F.lit(True))
        .withColumn("record_version", F.lit(1))
        .withColumn("change_type", F.lit("INSERT"))
    )


def build_tracking_hash(df, tracked_columns):
    normalized_columns = [F.coalesce(F.col(column_name).cast("string"), F.lit("<<NULL>>")) for column_name in tracked_columns]
    return df.withColumn("attribute_hash", F.sha2(F.concat_ws("||", *normalized_columns), 256))


def current_dimension_ref(spark, table_name, business_key, surrogate_key):
    return (
        spark.table(table_name)
        .where(F.col("is_current") & F.col("is_current_valid") & F.col("is_current_system"))
        .select(business_key, surrogate_key)
    )

