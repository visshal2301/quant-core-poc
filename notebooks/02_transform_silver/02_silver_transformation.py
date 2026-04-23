# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Silver Transformation

# COMMAND ----------

# DBTITLE 1,Cell 2
import os
import sys

from pyspark.sql import functions as F

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
SRC_ROOT = os.path.join(REPO_ROOT, "src")
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from quant_core.transforms.runtime import run_silver_transformation

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

print(f"\n{'='*80}")
print(f"Processing source_yyyymm = {TARGET_YYYYMM}")
print("Using SMART PARTITION REPLACEMENT - will replace only affected partitions")
print(f"{'='*80}\n")

run_silver_transformation(spark=spark, target_yyyymm=TARGET_YYYYMM, catalog=CATALOG)

print(f"\n{'='*80}")
print(f"✅ Silver dimensions and facts published for source_yyyymm = {TARGET_YYYYMM}")
print(f"✅ SMART PARTITION REPLACEMENT: Only affected partitions were replaced")
print(f"✅ Historical partitions preserved automatically")
print(f"\nHybrid approach: Fact tables store both surrogate keys (point-in-time) and natural keys (current hierarchy)")
print(f"\n⚠️  SCHEMA EVOLUTION POLICY: All schema changes must use explicit ALTER TABLE statements")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Dynamic Partition Overwrite - Why It Matters
# MAGIC %md
# MAGIC ## Smart Partition Replacement vs Static replaceWhere
# MAGIC
# MAGIC ### The Problem with Static `replaceWhere`
# MAGIC
# MAGIC **Old approach** (BROKEN):
# MAGIC ```python
# MAGIC # Filters bronze by source month
# MAGIC .where(F.col("source_yyyymm") == "202602")
# MAGIC
# MAGIC # But hard-codes replacement of the SAME month
# MAGIC .option("replaceWhere", "trade_yyyymm = '202602'")
# MAGIC ```
# MAGIC
# MAGIC **What breaks:**
# MAGIC 1. **Late-arriving data**: February load contains January corrections → January data becomes duplicates
# MAGIC 2. **Historical corrections**: Reload January data in March → Wrong partition targeted
# MAGIC 3. **Backfilling**: Load 6 months of historical data → Only current month partition replaced
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Solution: Smart Partition Replacement
# MAGIC
# MAGIC **New approach** (ROBUST):
# MAGIC ```python
# MAGIC # 1. Filter bronze by source month (when loaded)
# MAGIC df = spark.table("bronze.transactions").where(F.col("source_yyyymm") == "202602")
# MAGIC
# MAGIC # 2. Calculate actual event months from the data
# MAGIC df = df.withColumn("trade_yyyymm", F.date_format(F.to_date("trade_dt"), "yyyyMM"))
# MAGIC
# MAGIC # 3. Extract which partitions actually exist in this batch
# MAGIC affected_partitions = [row.trade_yyyymm for row in df.select("trade_yyyymm").distinct().collect()]
# MAGIC # Result: ['202601', '202602'] if batch contains both January and February transactions
# MAGIC
# MAGIC # 4. Build dynamic replaceWhere condition
# MAGIC replace_condition = f"trade_yyyymm IN ({','.join([repr(p) for p in affected_partitions])})"
# MAGIC # Result: "trade_yyyymm IN ('202601','202602')"
# MAGIC
# MAGIC # 5. Replace only the affected partitions
# MAGIC df.write.format("delta").mode("overwrite").partitionBy("trade_yyyymm") \
# MAGIC     .option("replaceWhere", replace_condition) \
# MAGIC     .saveAsTable("silver.fact_transactions")
# MAGIC ```
# MAGIC
# MAGIC **How it works:**
# MAGIC * Dynamically calculates which partition values are in the source data
# MAGIC * Replaces ONLY those partitions in the target table
# MAGIC * Preserves all other partitions untouched
# MAGIC * **Works on all compute types** (classic, serverless, SQL warehouses)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example Scenarios
# MAGIC
# MAGIC #### Scenario 1: Late-Arriving January Data in February Load
# MAGIC ```
# MAGIC Bronze (source_yyyymm = 202602):
# MAGIC   - 180 transactions from Feb 2026 (trade_dt = 2026-02-XX)
# MAGIC   - 20 transactions from Jan 2026 (trade_dt = 2026-01-XX) ← late arrivals
# MAGIC
# MAGIC Calculated affected_partitions: ['202601', '202602']
# MAGIC replaceWhere: "trade_yyyymm IN ('202601','202602')"
# MAGIC
# MAGIC Result:
# MAGIC   ✅ Replaces partition trade_yyyymm=202601 (20 records) - late data properly corrects January
# MAGIC   ✅ Replaces partition trade_yyyymm=202602 (180 records)
# MAGIC   ✅ Preserves all other historical months (Dec, Nov, Oct...)
# MAGIC
# MAGIC Old static approach (replaceWhere="trade_yyyymm='202602'"):
# MAGIC   ❌ Only replaces 202602 partition
# MAGIC   ❌ 20 January records written to 202601 partition as DUPLICATES
# MAGIC   ❌ Data quality issue: duplicate transaction_ids in silver layer
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 2: Historical Correction
# MAGIC ```
# MAGIC Bronze (source_yyyymm = 202603):
# MAGIC   - Correction file contains 50 January transactions with updated prices
# MAGIC   - All transactions have trade_dt in Jan 2026
# MAGIC
# MAGIC Calculated affected_partitions: ['202601']
# MAGIC replaceWhere: "trade_yyyymm IN ('202601')"
# MAGIC
# MAGIC Result:
# MAGIC   ✅ Replaces ONLY partition trade_yyyymm=202601 (50 corrected records)
# MAGIC   ✅ Feb and March partitions completely untouched
# MAGIC   ✅ Corrections applied precisely where needed
# MAGIC
# MAGIC Old static approach:
# MAGIC   ❌ Tries to replace partition 202603 (wrong month)
# MAGIC   ❌ Corrections written to 202601 but become duplicates
# MAGIC   ❌ Original wrong data still present
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 3: Multi-Month Backfill
# MAGIC ```
# MAGIC Bronze (source_yyyymm = 202604):
# MAGIC   - Backfill load contains 6 months of historical data
# MAGIC   - Transactions span Nov 2025 through Apr 2026
# MAGIC
# MAGIC Calculated affected_partitions: ['202511', '202512', '202601', '202602', '202603', '202604']
# MAGIC replaceWhere: "trade_yyyymm IN ('202511','202512','202601','202602','202603','202604')"
# MAGIC
# MAGIC Result:
# MAGIC   ✅ Replaces all 6 affected partitions in one operation
# MAGIC   ✅ Historical months properly populated
# MAGIC   ✅ Earlier months (Oct 2025 and before) untouched
# MAGIC
# MAGIC Old static approach:
# MAGIC   ❌ Only replaces partition 202604
# MAGIC   ❌ 5 months of historical data written but not properly replaced
# MAGIC   ❌ Massive data duplication across 5 partitions
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance & Safety
# MAGIC
# MAGIC **Performance:**
# MAGIC * One additional lightweight `.distinct().collect()` per fact table (~milliseconds)
# MAGIC * No full data scan - only reads partition column
# MAGIC * Same Delta write performance as static replaceWhere
# MAGIC * Typical overhead: <1% of total ETL time
# MAGIC
# MAGIC **Safety:**
# MAGIC * ✅ **Prevents data loss** from mismatched source/event months
# MAGIC * ✅ **Prevents duplicates** from late-arriving data
# MAGIC * ✅ **Enables corrections** without manual partition management
# MAGIC * ✅ **Supports backfilling** multiple months in one run
# MAGIC * ✅ **Works everywhere** - no special cluster configuration needed
# MAGIC
# MAGIC **When calculation is free:**
# MAGIC * If source data fits in memory (~millions of rows), `.collect()` is nearly instant
# MAGIC * Partition column is always small (just YYYYMM strings)
# MAGIC * Trade-off: microseconds of calculation vs. hours debugging data quality issues

# COMMAND ----------

# DBTITLE 1,Schema Evolution Policy & Examples
# MAGIC %md
# MAGIC ## Schema Evolution Policy for Silver Layer
# MAGIC
# MAGIC ### **Strict Schema Control**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use explicit `ALTER TABLE` statements for all schema changes
# MAGIC * Document schema changes in version control
# MAGIC * Review schema changes through PR process
# MAGIC * Test schema changes in development first
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Use `.option("mergeSchema", "true")` in production
# MAGIC * Use `.option("overwriteSchema", "true")` in production
# MAGIC * Allow automatic schema evolution
# MAGIC * Make schema changes during regular ETL runs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Common Schema Change Scenarios**
# MAGIC
# MAGIC #### **Scenario 1: Adding a New Column**
# MAGIC ```python
# MAGIC # Add a new column to fact_transactions
# MAGIC spark.sql("""
# MAGIC     ALTER TABLE quant_core.silver.fact_transactions 
# MAGIC     ADD COLUMNS (
# MAGIC         broker_id STRING COMMENT 'Broker identifier for transaction routing',
# MAGIC         broker_sk BIGINT COMMENT 'Surrogate key for broker dimension'
# MAGIC     )
# MAGIC """)
# MAGIC
# MAGIC # Then run your ETL with the new columns
# MAGIC fact_transactions_updated.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_transactions")
# MAGIC ```
# MAGIC
# MAGIC #### **Scenario 2: Adding Multiple Columns**
# MAGIC ```python
# MAGIC # Add risk metrics to fact_positions_daily
# MAGIC spark.sql("""
# MAGIC     ALTER TABLE quant_core.silver.fact_positions_daily 
# MAGIC     ADD COLUMNS (
# MAGIC         value_at_risk DOUBLE COMMENT 'VaR at 95% confidence',
# MAGIC         expected_shortfall DOUBLE COMMENT 'Expected shortfall beyond VaR',
# MAGIC         beta DOUBLE COMMENT 'Beta relative to market benchmark'
# MAGIC     )
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC #### **Scenario 3: Changing Column Comment/Metadata**
# MAGIC ```python
# MAGIC # Update column comment
# MAGIC spark.sql("""
# MAGIC     ALTER TABLE quant_core.silver.fact_transactions 
# MAGIC     ALTER COLUMN net_amount COMMENT 'Net transaction amount after fees and adjustments (audited)'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC #### **Scenario 4: Adding Table Properties**
# MAGIC ```python
# MAGIC # Set table properties for retention policy
# MAGIC spark.sql("""
# MAGIC     ALTER TABLE quant_core.silver.fact_transactions 
# MAGIC     SET TBLPROPERTIES (
# MAGIC         'delta.logRetentionDuration' = '365 days',
# MAGIC         'delta.deletedFileRetentionDuration' = '90 days',
# MAGIC         'data_classification' = 'CONFIDENTIAL',
# MAGIC         'retention_policy' = 'REGULATORY_7_YEARS'
# MAGIC     )
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Column Removal (Rare - Requires Migration)**
# MAGIC
# MAGIC ⚠️ **Dropping columns is a BREAKING CHANGE**
# MAGIC
# MAGIC If you must remove a column:
# MAGIC
# MAGIC ```python
# MAGIC # Step 1: Verify no downstream dependencies
# MAGIC # Check dashboards, reports, gold layer queries
# MAGIC
# MAGIC # Step 2: Create new version of table
# MAGIC spark.sql("""
# MAGIC     CREATE TABLE quant_core.silver.fact_transactions_v2 
# MAGIC     AS SELECT 
# MAGIC         transaction_id,
# MAGIC         portfolio_id,
# MAGIC         -- (list only columns you want to keep)
# MAGIC     FROM quant_core.silver.fact_transactions
# MAGIC """)
# MAGIC
# MAGIC # Step 3: Update all ETL pipelines to write to new table
# MAGIC # Step 4: Migrate downstream consumers
# MAGIC # Step 5: After validation period, drop old table
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Schema Validation Pattern (Recommended)**
# MAGIC
# MAGIC ```python
# MAGIC def validate_schema_match(df, target_table: str):
# MAGIC     """
# MAGIC     Validate that DataFrame schema matches target table schema.
# MAGIC     Raises error if schemas don't match exactly.
# MAGIC     """
# MAGIC     if not spark.catalog.tableExists(target_table):
# MAGIC         return True  # First write, no validation needed
# MAGIC     
# MAGIC     existing_schema = spark.table(target_table).schema
# MAGIC     new_schema = df.schema
# MAGIC     
# MAGIC     existing_cols = {f.name: f.dataType for f in existing_schema.fields}
# MAGIC     new_cols = {f.name: f.dataType for f in new_schema.fields}
# MAGIC     
# MAGIC     # Check for missing columns
# MAGIC     missing_cols = set(existing_cols.keys()) - set(new_cols.keys())
# MAGIC     if missing_cols:
# MAGIC         raise ValueError(f"DataFrame is missing columns: {missing_cols}. Use ALTER TABLE to add/remove columns.")
# MAGIC     
# MAGIC     # Check for extra columns
# MAGIC     extra_cols = set(new_cols.keys()) - set(existing_cols.keys())
# MAGIC     if extra_cols:
# MAGIC         raise ValueError(f"DataFrame has unexpected columns: {extra_cols}. Use ALTER TABLE to add columns first.")
# MAGIC     
# MAGIC     # Check for type mismatches
# MAGIC     for col_name in existing_cols:
# MAGIC         if existing_cols[col_name] != new_cols[col_name]:
# MAGIC             raise ValueError(f"Column {col_name} type mismatch: table={existing_cols[col_name]}, df={new_cols[col_name]}")
# MAGIC     
# MAGIC     return True
# MAGIC
# MAGIC # Usage:
# MAGIC validate_schema_match(fact_transactions, f"{SILVER}.fact_transactions")
# MAGIC fact_transactions.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER}.fact_transactions")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Benefits of This Approach**
# MAGIC
# MAGIC 1. **Auditability**: All schema changes are version controlled and reviewable
# MAGIC 2. **Safety**: No accidental column drops or type changes
# MAGIC 3. **Compliance**: Clear documentation trail for regulatory requirements
# MAGIC 4. **Predictability**: ETL runs cannot unexpectedly modify schemas
# MAGIC 5. **Testing**: Schema changes can be tested independently before ETL runs

# COMMAND ----------

# DBTITLE 1,Hybrid Approach Query Examples
# ========================================
# HYBRID APPROACH DEMONSTRATION
# ========================================

print("\n" + "="*80)
print("EXAMPLE 1: Point-in-Time Query Using Surrogate Keys")
print("Use Case: Historical P&L attribution with as-of-date portfolio attributes")
print("="*80)

# Query using surrogate keys - captures attributes as they were at transaction time
point_in_time_query = spark.sql(f"""
    SELECT 
        t.transaction_id,
        t.trade_dt,
        t.portfolio_id,                    -- Natural key (for identification)
        p.portfolio_name,
        p.risk_policy_name,                 -- Attribute as of transaction date
        p.effective_from_dt,                -- Shows when this version became effective
        t.net_amount,
        c.currency_name
    FROM {SILVER}.fact_transactions t
    JOIN {SILVER}.dim_portfolio p ON t.portfolio_sk = p.portfolio_sk  -- Using SK for point-in-time
    JOIN {SILVER}.dim_currency c ON t.currency_sk = c.currency_sk
    WHERE t.portfolio_id = 'PORT001'
    ORDER BY t.trade_dt
    LIMIT 5
""")

print("\nResults: Shows portfolio attributes as they were at the transaction date")
display(point_in_time_query)

print("\n" + "="*80)
print("EXAMPLE 2: Current Hierarchy Query Using Natural Keys")
print("Use Case: Transaction summary with current portfolio attributes")
print("="*80)

# Query using natural keys - always gets current attributes
current_hierarchy_query = spark.sql(f"""
    SELECT 
        t.portfolio_id,
        p.portfolio_name,
        p.risk_policy_name,                 -- Current risk policy
        p.base_currency_code,
        COUNT(t.transaction_id) as transaction_count,
        SUM(t.net_amount) as total_net_amount
    FROM {SILVER}.fact_transactions t
    JOIN {SILVER}.dim_portfolio p 
        ON t.portfolio_id = p.portfolio_id  -- Using natural key for current attributes
        AND p.is_current = true             -- Get only current version
    GROUP BY t.portfolio_id, p.portfolio_name, p.risk_policy_name, p.base_currency_code
    ORDER BY transaction_count DESC
""")

print("\nResults: Shows transaction summary with CURRENT portfolio attributes")
display(current_hierarchy_query)

print("\n" + "="*80)
print("EXAMPLE 3: Verify Natural Keys Are Present in Fact Tables")
print("="*80)

print("\nfact_transactions schema:")
spark.table(f"{SILVER}.fact_transactions").printSchema()

print("\n" + "="*80)
print("KEY BENEFITS OF HYBRID APPROACH:")
print("="*80)
print("""
1. POINT-IN-TIME ACCURACY (using surrogate keys):
   - Regulatory reporting with historical attributes
   - Performance attribution as-of specific dates
   - Audit trails showing exactly what was known at transaction time
   
2. CURRENT HIERARCHY REPORTING (using natural keys):
   - Dashboards showing latest organizational structure
   - Simplified ad-hoc analysis without complex joins
   - Portfolio rebalancing with current classifications
   
3. QUERY FLEXIBILITY:
   - Same fact table serves both use cases
   - Minimal storage overhead (just natural key columns)
   - No need for separate bridge tables or complex subqueries
""")

print("\n" + "="*80)
print("STORAGE OVERHEAD: Minimal")
print("="*80)
print("""
- Added columns per fact table: 3-4 string columns (natural keys)
- Storage impact: ~5-10% increase in fact table size
- Query performance: Negligible impact, often faster for natural key queries
- Maintenance: No additional ETL complexity
""")

# COMMAND ----------

# DBTITLE 1,View Dimension Schemas
# Display schemas and record counts for all dimension tables
dim_tables = [
    "dim_portfolio", "dim_instrument", "dim_counterparty", 
    "dim_currency", "dim_asset_class", "dim_market_data_source", "dim_date"
]

for table_name in dim_tables:
    full_table_name = f"{SILVER}.{table_name}"
    df = spark.table(full_table_name)
    count = df.count()
    
    print(f"\n{'='*80}")
    print(f"Table: {table_name} (Records: {count})")
    print(f"{'='*80}")
    df.printSchema()
    
    # Show sample records
    print(f"\nSample records from {table_name}:")
    display(df.limit(3))
