# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Bronze Ingestion

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "quant_core"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
LANDING_BASE = "/Volumes/quant_core/landing/mock_data"
OPEN_ENDED_TS = "9999-12-31 23:59:59"

if "dbutils" in globals():
    dbutils.widgets.text("target_yyyymm", "202604")
    TARGET_YYYYMM = dbutils.widgets.get("target_yyyymm")
else:
    TARGET_YYYYMM = "202604"


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
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("source_file_path", F.input_file_name())
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
