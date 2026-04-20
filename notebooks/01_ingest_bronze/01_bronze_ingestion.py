# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Quant Core - Bronze Ingestion

# COMMAND ----------

# DBTITLE 1,Bronze Layer Tables - Business Usage
# MAGIC %md
# MAGIC ## Bronze Layer Tables
# MAGIC
# MAGIC This notebook ingests raw data into the bronze layer of the quant core data platform. Below are the tables created and their business usage:
# MAGIC
# MAGIC ### Fact Tables (Time-Series Data)
# MAGIC
# MAGIC **`transactions_raw`**
# MAGIC * **Description**: Records all buy/sell transactions executed across portfolios
# MAGIC * **Business Usage**: Trade reconciliation, transaction cost analysis, P&L attribution, regulatory reporting (MiFID II, EMIR), audit trails
# MAGIC * **Key Fields**: transaction_id, portfolio_id, instrument_id, counterparty_id, trade_dt, settlement_dt, quantity, price, net_amount
# MAGIC
# MAGIC **`positions_daily_raw`**
# MAGIC * **Description**: End-of-day portfolio positions snapshot for each instrument
# MAGIC * **Business Usage**: Position monitoring, portfolio composition analysis, risk exposure calculation, NAV computation, performance measurement
# MAGIC * **Key Fields**: position_id, portfolio_id, instrument_id, position_dt, quantity, end_of_day_price, market_value, unrealized_pnl
# MAGIC
# MAGIC **`market_prices_daily_raw`**
# MAGIC * **Description**: Daily closing prices and returns for all traded instruments
# MAGIC * **Business Usage**: Mark-to-market valuation, return calculation, volatility estimation, risk metrics (VaR, CVaR), backtesting strategies
# MAGIC * **Key Fields**: price_id, instrument_id, price_dt, close_price, return_pct, volatility_proxy
# MAGIC
# MAGIC **`cashflows_raw`**
# MAGIC * **Description**: All cash movements including dividends, coupons, fees, and capital flows
# MAGIC * **Business Usage**: Cash management, income tracking, dividend forecasting, liquidity planning, cash drag analysis
# MAGIC * **Key Fields**: cashflow_id, portfolio_id, instrument_id, cashflow_dt, cashflow_type, amount
# MAGIC
# MAGIC ### Dimension Tables (Reference Data)
# MAGIC
# MAGIC **`portfolios_raw`**
# MAGIC * **Description**: Master data for investment portfolios/funds
# MAGIC * **Business Usage**: Portfolio classification, strategy assignment, benchmark mapping, reporting hierarchies
# MAGIC * **Key Fields**: portfolio_id, portfolio_name, strategy, base_currency, inception_dt, status
# MAGIC
# MAGIC **`instruments_raw`**
# MAGIC * **Description**: Master data for all tradeable financial instruments
# MAGIC * **Business Usage**: Security master reference, instrument classification, risk factor mapping, regulatory classification
# MAGIC * **Key Fields**: instrument_id, instrument_name, ticker, isin, asset_class_id, currency_code
# MAGIC
# MAGIC **`counterparties_raw`**
# MAGIC * **Description**: Reference data for trading counterparties and brokers
# MAGIC * **Business Usage**: Counterparty risk management, credit exposure monitoring, broker selection, execution quality analysis
# MAGIC * **Key Fields**: counterparty_id, counterparty_name, counterparty_type, country_code, credit_rating
# MAGIC
# MAGIC **`currencies_raw`**
# MAGIC * **Description**: Reference data for currencies used in trading and valuation
# MAGIC * **Business Usage**: FX conversion, multi-currency reporting, currency risk exposure, hedging decisions
# MAGIC * **Key Fields**: currency_code, currency_name, minor_unit, is_active
# MAGIC
# MAGIC **`asset_classes_raw`**
# MAGIC * **Description**: Classification hierarchy for financial asset types
# MAGIC * **Business Usage**: Asset allocation analysis, risk budgeting, portfolio construction, performance attribution by asset class
# MAGIC * **Key Fields**: asset_class_id, asset_class_name, asset_class_description
# MAGIC
# MAGIC **`market_data_sources_raw`**
# MAGIC * **Description**: Reference data for market data vendors and feeds
# MAGIC * **Business Usage**: Data lineage tracking, vendor management, data quality assessment, regulatory audit trails
# MAGIC * **Key Fields**: source_code, source_name, data_type, is_primary

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F

CATALOG = "quant_core"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
LANDING_BASE = "/Volumes/quant_core/landing/mock_data"
OPEN_ENDED_TS = "9999-12-31 23:59:59"

if "dbutils" in globals():
    dbutils.widgets.text("target_yyyymm", "202601")
    TARGET_YYYYMM = dbutils.widgets.get("target_yyyymm")
else:
    TARGET_YYYYMM = "202601"


def resolve_monthly_path(dataset: str) -> str:
    return f"{LANDING_BASE}/{dataset}/{TARGET_YYYYMM}/*.csv"


def resolve_dimension_path(table_name: str) -> str:
    return f"{LANDING_BASE}/dimensions/{TARGET_YYYYMM}/{table_name}.csv"


def ingest_csv_to_bronze(source_path: str, target_table: str) -> None:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("source_file_name", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
        .withColumn("load_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
        .withColumn("record_hash", F.sha2(F.to_json(F.struct("*")), 256))
        .withColumn("system_from_ts", F.current_timestamp())
        .withColumn("system_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
        .withColumn("record_version", F.lit(1))
        .withColumn("change_type", F.lit("INSERT"))
        .withColumn("is_current_system", F.lit(True))
        .withColumn("source_yyyymm", F.lit(TARGET_YYYYMM))
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")

# COMMAND ----------

ingest_csv_to_bronze(resolve_monthly_path("transactions"), "transactions_raw")
ingest_csv_to_bronze(resolve_monthly_path("positions_daily"), "positions_daily_raw")
ingest_csv_to_bronze(resolve_monthly_path("market_prices_daily"), "market_prices_daily_raw")
ingest_csv_to_bronze(resolve_monthly_path("cashflows"), "cashflows_raw")
ingest_csv_to_bronze(resolve_dimension_path("portfolios"), "portfolios_raw")
ingest_csv_to_bronze(resolve_dimension_path("instruments"), "instruments_raw")
ingest_csv_to_bronze(resolve_dimension_path("counterparties"), "counterparties_raw")
ingest_csv_to_bronze(resolve_dimension_path("currencies"), "currencies_raw")
ingest_csv_to_bronze(resolve_dimension_path("asset_classes"), "asset_classes_raw")
ingest_csv_to_bronze(resolve_dimension_path("market_data_sources"), "market_data_sources_raw")

print(f"Bronze ingestion complete for source month {TARGET_YYYYMM}.")

# COMMAND ----------

# DBTITLE 1,View Bronze Data
# View sample data from bronze tables
print("=== Transactions ===")
display(spark.table(f"{BRONZE_SCHEMA}.transactions_raw").limit(5))

print("\n=== Positions Daily ===")
display(spark.table(f"{BRONZE_SCHEMA}.positions_daily_raw").limit(5))

print("\n=== Market Prices ===")
display(spark.table(f"{BRONZE_SCHEMA}.market_prices_daily_raw").limit(5))

print("\n=== Record Counts ===")
tables = [
    "transactions_raw", "positions_daily_raw", "market_prices_daily_raw", 
    "cashflows_raw", "portfolios_raw", "instruments_raw", 
    "counterparties_raw", "currencies_raw", "asset_classes_raw", 
    "market_data_sources_raw"
]

counts = [(t, spark.table(f"{BRONZE_SCHEMA}.{t}").count()) for t in tables]
display(spark.createDataFrame(counts, ["table_name", "record_count"]))
