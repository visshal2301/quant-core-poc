# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Quant Core - Gold Calculations

# COMMAND ----------

# DBTITLE 1,Setup and Utility Functions
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
SRC_ROOT = os.path.join(REPO_ROOT, "src")
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from quant_core.calculations.runtime import run_gold_calculations

CATALOG = "quant_core"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
CONFIDENCE_LEVELS = [0.95, 0.99]
STRESS_WINDOW_START = "2024-09-01"
STRESS_WINDOW_END = "2024-11-30"

if "dbutils" in globals():
    dbutils.widgets.text("target_yyyymm", "202601")
    TARGET_YYYYMM = dbutils.widgets.get("target_yyyymm")
else:
    TARGET_YYYYMM = "202601"

run_gold_calculations(spark=spark, target_yyyymm=TARGET_YYYYMM, catalog=CATALOG)

print(f"Gold calculations published for target month {TARGET_YYYYMM}.")
