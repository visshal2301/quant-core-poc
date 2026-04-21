# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Silver Transformation

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "quant_core"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
OPEN_ENDED_TS = "9999-12-31 23:59:59"
OPEN_ENDED_DT = "9999-12-31"

if "dbutils" in globals():
    dbutils.widgets.text("target_yyyymm", "202601")
    TARGET_YYYYMM = dbutils.widgets.get("target_yyyymm")
else:
    TARGET_YYYYMM = "202601"


def add_surrogate_key(df, business_key: str, surrogate_key: str):
    window_spec = Window.orderBy(F.col(business_key))
    return df.withColumn(surrogate_key, F.row_number().over(window_spec))


def apply_bitemporal_columns(df, valid_from_col: str, valid_to_expr=None):
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


def current_dimension_ref(table_name: str, business_key: str, surrogate_key: str):
    return (
        spark.table(f"{SILVER}.{table_name}")
        .where(F.col("is_current") & F.col("is_current_valid") & F.col("is_current_system"))
        .select(business_key, surrogate_key)
    )


def build_tracking_hash(df, tracked_columns):
    normalized_columns = [F.coalesce(F.col(column_name).cast("string"), F.lit("<<NULL>>")) for column_name in tracked_columns]
    return df.withColumn("attribute_hash", F.sha2(F.concat_ws("||", *normalized_columns), 256))


def ensure_dimension_table(table_name: str, initial_df) -> None:
    if not spark.catalog.tableExists(f"{SILVER}.{table_name}"):
        initial_df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.{table_name}")


def scd2_dimension_merge(
    source_df,
    table_name: str,
    business_key: str,
    surrogate_key: str,
    tracked_columns,
) -> None:
    source_with_hash = build_tracking_hash(source_df.dropDuplicates([business_key]), tracked_columns)
    target_table_name = f"{SILVER}.{table_name}"

    if not spark.catalog.tableExists(target_table_name):
        initial_df = (
            source_with_hash.withColumn(surrogate_key, F.row_number().over(Window.orderBy(F.col(business_key))))
            .withColumn("record_version", F.lit(1))
            .withColumn("change_type", F.lit("INSERT"))
        )
        ensure_dimension_table(table_name, initial_df)
        return

    target_df = spark.table(target_table_name)
    current_df = (
        target_df.where(F.col("is_current") & F.col("is_current_valid") & F.col("is_current_system"))
        .select(business_key, surrogate_key, "record_version", "attribute_hash")
    )

    max_key_value = target_df.agg(F.coalesce(F.max(F.col(surrogate_key)), F.lit(0)).alias("max_sk")).collect()[0]["max_sk"]

    changes_df = (
        source_with_hash.alias("src")
        .join(current_df.alias("tgt"), business_key, "left")
        .where(F.col("tgt.attribute_hash").isNull() | (F.col("src.attribute_hash") != F.col("tgt.attribute_hash")))
        .select("src.*", F.col(f"tgt.{surrogate_key}").alias("existing_surrogate_key"), F.col("tgt.record_version").alias("existing_version"))
    )

    if changes_df.limit(1).count() == 0:
        return

    changed_existing_keys = (
        changes_df.where(F.col("existing_surrogate_key").isNotNull())
        .select(business_key)
        .dropDuplicates()
    )

    if changed_existing_keys.limit(1).count() > 0:
        target_delta = DeltaTable.forName(spark, target_table_name)
        (
            target_delta.alias("tgt")
            .merge(changed_existing_keys.alias("chg"), f"tgt.{business_key} = chg.{business_key} AND tgt.is_current = true")
            .whenMatchedUpdate(
                set={
                    "effective_to_dt": "date_sub(current_date(), 1)",
                    "is_current": "false",
                    "valid_to_ts": "current_timestamp()",
                    "system_to_ts": "current_timestamp()",
                    "is_current_valid": "false",
                    "is_current_system": "false",
                    "change_type": "'EXPIRE'",
                }
            )
            .execute()
        )

    new_versions = (
        changes_df.withColumn(
            surrogate_key,
            F.row_number().over(Window.orderBy(F.col(business_key), F.col("attribute_hash"))) + F.lit(max_key_value),
        )
        .withColumn("effective_from_dt", F.current_date())
        .withColumn("effective_to_dt", F.to_date(F.lit(OPEN_ENDED_DT)))
        .withColumn("is_current", F.lit(True))
        .withColumn("valid_from_ts", F.current_timestamp())
        .withColumn("valid_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
        .withColumn("system_from_ts", F.current_timestamp())
        .withColumn("system_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
        .withColumn("is_current_valid", F.lit(True))
        .withColumn("is_current_system", F.lit(True))
        .withColumn("record_version", F.coalesce(F.col("existing_version"), F.lit(0)) + F.lit(1))
        .withColumn(
            "change_type",
            F.when(F.col("existing_surrogate_key").isNull(), F.lit("INSERT")).otherwise(F.lit("UPDATE")),
        )
        .drop("existing_surrogate_key", "existing_version")
    )

    (
        new_versions.select(*target_df.columns)
        .write.format("delta")
        .mode("append")
        .saveAsTable(target_table_name)
    )


def overwrite_static_dimension(source_df, table_name: str, business_key: str, surrogate_key: str) -> None:
    df = add_surrogate_key(source_df.dropDuplicates([business_key]), business_key, surrogate_key)
    df = (
        df.withColumn("effective_from_dt", F.current_date())
        .withColumn("effective_to_dt", F.to_date(F.lit(OPEN_ENDED_DT)))
        .withColumn("is_current", F.lit(True))
    )
    df = apply_bitemporal_columns(df, "effective_from_dt").withColumn("attribute_hash", F.lit(None).cast("string"))
    df.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.{table_name}")


# COMMAND ----------

portfolio_source = (
    spark.table(f"{BRONZE}.portfolios_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .select("portfolio_id", "portfolio_name", "portfolio_type", "base_currency_code", "risk_policy_name")
)
scd2_dimension_merge(
    portfolio_source,
    "dim_portfolio",
    "portfolio_id",
    "portfolio_sk",
    ["portfolio_name", "portfolio_type", "base_currency_code", "risk_policy_name"],
)

instrument_source = (
    spark.table(f"{BRONZE}.instruments_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .select(
        "instrument_id",
        "instrument_name",
        "ticker",
        "isin",
        "asset_class_code",
        "currency_code",
        "issuer_name",
        "coupon_rate",
        "maturity_dt",
    )
)
scd2_dimension_merge(
    instrument_source,
    "dim_instrument",
    "instrument_id",
    "instrument_sk",
    ["instrument_name", "ticker", "isin", "asset_class_code", "currency_code", "issuer_name", "coupon_rate", "maturity_dt"],
)

counterparty_source = (
    spark.table(f"{BRONZE}.counterparties_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .select("counterparty_id", "counterparty_name", "counterparty_type", "country_code", "credit_rating")
)
scd2_dimension_merge(
    counterparty_source,
    "dim_counterparty",
    "counterparty_id",
    "counterparty_sk",
    ["counterparty_name", "counterparty_type", "country_code", "credit_rating"],
)

currency_source = (
    spark.table(f"{BRONZE}.currencies_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .select("currency_code", "currency_name", "decimal_precision")
)
overwrite_static_dimension(currency_source, "dim_currency", "currency_code", "currency_sk")

asset_class_source = (
    spark.table(f"{BRONZE}.asset_classes_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .select("asset_class_code", "asset_class_name", "risk_bucket")
)
overwrite_static_dimension(asset_class_source, "dim_asset_class", "asset_class_code", "asset_class_sk")

market_data_source_source = (
    spark.table(f"{BRONZE}.market_data_sources_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .select("source_system_code", "source_system_name", "vendor_name")
)
overwrite_static_dimension(
    market_data_source_source,
    "dim_market_data_source",
    "source_system_code",
    "market_data_source_sk",
)

date_df = spark.sql(
    """
    SELECT explode(sequence(to_date('2023-01-01'), to_date(current_date()), interval 1 day)) AS calendar_dt
    """
)
dim_date = (
    date_df.withColumn("date_sk", F.date_format("calendar_dt", "yyyyMMdd").cast("int"))
    .withColumn("year_num", F.year("calendar_dt"))
    .withColumn("quarter_num", F.quarter("calendar_dt"))
    .withColumn("month_num", F.month("calendar_dt"))
    .withColumn("day_num", F.dayofmonth("calendar_dt"))
    .withColumn("is_month_end", F.last_day("calendar_dt") == F.col("calendar_dt"))
    .withColumn("is_business_day", F.dayofweek("calendar_dt").isin([2, 3, 4, 5, 6]))
)
dim_date = apply_bitemporal_columns(dim_date, "calendar_dt").withColumn("attribute_hash", F.lit(None).cast("string"))
dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_date")


# COMMAND ----------

portfolio_ref = current_dimension_ref("dim_portfolio", "portfolio_id", "portfolio_sk")
instrument_ref = (
    spark.table(f"{SILVER}.dim_instrument")
    .where(F.col("is_current") & F.col("is_current_valid") & F.col("is_current_system"))
    .select("instrument_id", "instrument_sk", "asset_class_code")
)
counterparty_ref = current_dimension_ref("dim_counterparty", "counterparty_id", "counterparty_sk")
currency_ref = current_dimension_ref("dim_currency", "currency_code", "currency_sk")
market_data_source_ref = current_dimension_ref("dim_market_data_source", "source_system_code", "market_data_source_sk")

fact_transactions = (
    spark.table(f"{BRONZE}.transactions_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(portfolio_ref, "portfolio_id", "left")
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(counterparty_ref, "counterparty_id", "left")
    .join(currency_ref, "currency_code", "left")
    .select(
        "transaction_id",
        "portfolio_sk",
        "instrument_sk",
        "counterparty_sk",
        F.to_date("trade_dt").alias("trade_dt"),
        F.to_date("settlement_dt").alias("settlement_dt"),
        "transaction_type",
        F.col("quantity").cast("double").alias("quantity"),
        F.col("price").cast("double").alias("price"),
        F.col("gross_amount").cast("double").alias("gross_amount"),
        F.col("fees_amount").cast("double").alias("fees_amount"),
        F.col("net_amount").cast("double").alias("net_amount"),
        "currency_sk",
        "load_id",
        "source_yyyymm",
        F.date_format(F.to_date("trade_dt"), "yyyyMM").alias("trade_yyyymm"),
    )
)
fact_transactions = apply_bitemporal_columns(fact_transactions, "trade_dt")
fact_transactions.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_transactions")

fact_positions_daily = (
    spark.table(f"{BRONZE}.positions_daily_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(portfolio_ref, "portfolio_id", "left")
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(currency_ref, "currency_code", "left")
    .join(market_data_source_ref, "source_system_code", "left")
    .select(
        "position_id",
        "portfolio_sk",
        "instrument_sk",
        F.to_date("position_dt").alias("position_dt"),
        F.col("quantity").cast("double").alias("quantity"),
        F.col("end_of_day_price").cast("double").alias("end_of_day_price"),
        F.col("market_value").cast("double").alias("market_value"),
        F.col("unrealized_pnl").cast("double").alias("unrealized_pnl"),
        "currency_sk",
        "market_data_source_sk",
        "source_yyyymm",
        F.date_format(F.to_date("position_dt"), "yyyyMM").alias("position_yyyymm"),
    )
)
fact_positions_daily = apply_bitemporal_columns(
    fact_positions_daily,
    "position_dt",
    F.expr("cast(position_dt + interval 1 day as timestamp)"),
)
fact_positions_daily.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_positions_daily")

fact_market_prices_daily = (
    spark.table(f"{BRONZE}.market_prices_daily_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(currency_ref, "currency_code", "left")
    .join(market_data_source_ref, "source_system_code", "left")
    .select(
        "price_id",
        "instrument_sk",
        F.to_date("price_dt").alias("price_dt"),
        F.col("close_price").cast("double").alias("close_price"),
        F.col("return_pct").cast("double").alias("return_pct"),
        F.col("volatility_proxy").cast("double").alias("volatility_proxy"),
        "currency_sk",
        "market_data_source_sk",
        "source_yyyymm",
        F.date_format(F.to_date("price_dt"), "yyyyMM").alias("price_yyyymm"),
    )
)
fact_market_prices_daily = apply_bitemporal_columns(
    fact_market_prices_daily,
    "price_dt",
    F.expr("cast(price_dt + interval 1 day as timestamp)"),
)
fact_market_prices_daily.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_market_prices_daily")

fact_cashflows = (
    spark.table(f"{BRONZE}.cashflows_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(portfolio_ref, "portfolio_id", "left")
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(currency_ref, "currency_code", "left")
    .select(
        "cashflow_id",
        "portfolio_sk",
        "instrument_sk",
        F.to_date("cashflow_dt").alias("cashflow_dt"),
        "cashflow_type",
        F.col("cashflow_amount").cast("double").alias("cashflow_amount"),
        "currency_sk",
        "source_yyyymm",
        F.date_format(F.to_date("cashflow_dt"), "yyyyMM").alias("cashflow_yyyymm"),
    )
)
fact_cashflows = apply_bitemporal_columns(fact_cashflows, "cashflow_dt")
fact_cashflows.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_cashflows")

print(f"Silver dimensions and facts published with SCD2 for core dimensions for source month {TARGET_YYYYMM}.")
