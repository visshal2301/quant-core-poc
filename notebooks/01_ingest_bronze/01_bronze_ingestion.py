# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Bronze Ingestion

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "quant_core"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
LANDING_BASE = "/Volumes/quant_core/landing/mock_data"
OPEN_ENDED_TS = "9999-12-31 23:59:59"


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
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")


# COMMAND ----------

ingest_csv_to_bronze(f"{LANDING_BASE}/transactions/*.csv", "transactions_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/positions_daily/*.csv", "positions_daily_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/market_prices_daily/*.csv", "market_prices_daily_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/cashflows/*.csv", "cashflows_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/dimensions/portfolios.csv", "portfolios_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/dimensions/instruments.csv", "instruments_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/dimensions/counterparties.csv", "counterparties_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/dimensions/currencies.csv", "currencies_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/dimensions/asset_classes.csv", "asset_classes_raw")
ingest_csv_to_bronze(f"{LANDING_BASE}/dimensions/market_data_sources.csv", "market_data_sources_raw")

print("Bronze ingestion complete.")
