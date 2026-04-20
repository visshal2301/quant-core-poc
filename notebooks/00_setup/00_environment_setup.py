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

# DBTITLE 1,Create Catalog
# Create the catalog first
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"Catalog '{CATALOG}' created successfully.")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Step 1: Create schemas
for schema_name in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")
    print(f"Schema '{schema_name}' created.")

# Step 2: Create volumes (mock_data volume in landing schema)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.mock_data")
print("Volume 'mock_data' created in 'landing' schema.")

# Step 3: Create subdirectories within the volume
for volume_path in VOLUME_PATHS:
    dbutils.fs.mkdirs(volume_path)
    print(f"Directory created: {volume_path}")

print("\nQuant Core schemas, volumes, and folders are ready.")
print("Fact landing folders expect month subfolders such as YYYYMM, for example /transactions/202604/.")
