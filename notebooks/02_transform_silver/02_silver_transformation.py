# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Silver Transformation

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "quant_core"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
OPEN_ENDED_TS = "9999-12-31 23:59:59"


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


# COMMAND ----------

dim_portfolio = (
    spark.table(f"{BRONZE}.portfolios_raw")
    .select("portfolio_id", "portfolio_name", "portfolio_type", "base_currency_code", "risk_policy_name")
    .dropDuplicates(["portfolio_id"])
)
dim_portfolio = (
    add_surrogate_key(dim_portfolio, "portfolio_id", "portfolio_sk")
    .withColumn("effective_from_dt", F.current_date())
    .withColumn("effective_to_dt", F.to_date(F.lit("9999-12-31")))
    .withColumn("is_current", F.lit(True))
)
dim_portfolio = apply_bitemporal_columns(dim_portfolio, "effective_from_dt")
dim_portfolio.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_portfolio")

dim_instrument = (
    spark.table(f"{BRONZE}.instruments_raw")
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
    .dropDuplicates(["instrument_id"])
)
dim_instrument = (
    add_surrogate_key(dim_instrument, "instrument_id", "instrument_sk")
    .withColumn("effective_from_dt", F.current_date())
    .withColumn("effective_to_dt", F.to_date(F.lit("9999-12-31")))
    .withColumn("is_current", F.lit(True))
)
dim_instrument = apply_bitemporal_columns(dim_instrument, "effective_from_dt")
dim_instrument.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_instrument")

dim_counterparty = (
    spark.table(f"{BRONZE}.counterparties_raw")
    .select("counterparty_id", "counterparty_name", "counterparty_type", "country_code", "credit_rating")
    .dropDuplicates(["counterparty_id"])
)
dim_counterparty = add_surrogate_key(dim_counterparty, "counterparty_id", "counterparty_sk")
dim_counterparty = apply_bitemporal_columns(dim_counterparty.withColumn("effective_from_dt", F.current_date()), "effective_from_dt")
dim_counterparty.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_counterparty")

dim_currency = spark.table(f"{BRONZE}.currencies_raw").select("currency_code", "currency_name", "decimal_precision").dropDuplicates()
dim_currency = add_surrogate_key(dim_currency, "currency_code", "currency_sk")
dim_currency = apply_bitemporal_columns(dim_currency.withColumn("effective_from_dt", F.current_date()), "effective_from_dt")
dim_currency.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_currency")

dim_asset_class = spark.table(f"{BRONZE}.asset_classes_raw").select("asset_class_code", "asset_class_name", "risk_bucket").dropDuplicates()
dim_asset_class = add_surrogate_key(dim_asset_class, "asset_class_code", "asset_class_sk")
dim_asset_class = apply_bitemporal_columns(dim_asset_class.withColumn("effective_from_dt", F.current_date()), "effective_from_dt")
dim_asset_class.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_asset_class")

dim_market_data_source = (
    spark.table(f"{BRONZE}.market_data_sources_raw")
    .select("source_system_code", "source_system_name", "vendor_name")
    .dropDuplicates()
)
dim_market_data_source = add_surrogate_key(dim_market_data_source, "source_system_code", "market_data_source_sk")
dim_market_data_source = apply_bitemporal_columns(
    dim_market_data_source.withColumn("effective_from_dt", F.current_date()),
    "effective_from_dt",
)
dim_market_data_source.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_market_data_source")

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
dim_date = apply_bitemporal_columns(dim_date, "calendar_dt")
dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.dim_date")


# COMMAND ----------

portfolio_ref = spark.table(f"{SILVER}.dim_portfolio").select("portfolio_id", "portfolio_sk")
instrument_ref = spark.table(f"{SILVER}.dim_instrument").select("instrument_id", "instrument_sk", "asset_class_code")
counterparty_ref = spark.table(f"{SILVER}.dim_counterparty").select("counterparty_id", "counterparty_sk")
currency_ref = spark.table(f"{SILVER}.dim_currency").select("currency_code", "currency_sk")
market_data_source_ref = spark.table(f"{SILVER}.dim_market_data_source").select("source_system_code", "market_data_source_sk")

fact_transactions = (
    spark.table(f"{BRONZE}.transactions_raw")
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
    )
)
fact_transactions = apply_bitemporal_columns(fact_transactions, "trade_dt")
fact_transactions.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_transactions")

fact_positions_daily = (
    spark.table(f"{BRONZE}.positions_daily_raw")
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
    )
)
fact_cashflows = apply_bitemporal_columns(fact_cashflows, "cashflow_dt")
fact_cashflows.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_cashflows")

print("Silver dimensions and facts published.")
