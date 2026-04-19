# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Environment Setup

# COMMAND ----------

CATALOG = "quant_core"
SCHEMAS = ["landing", "bronze", "silver", "gold", "audit"]
VOLUME_PATHS = [
    "/Volumes/quant_core/landing/mock_data/dimensions",
    "/Volumes/quant_core/landing/mock_data/transactions",
    "/Volumes/quant_core/landing/mock_data/positions_daily",
    "/Volumes/quant_core/landing/mock_data/market_prices_daily",
    "/Volumes/quant_core/landing/mock_data/cashflows",
]

# COMMAND ----------

for schema_name in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")

for volume_path in VOLUME_PATHS:
    dbutils.fs.mkdirs(volume_path)

print("Quant Core schemas and volume folders are ready.")

