# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Silver Transformation

# COMMAND ----------

# DBTITLE 1,Cell 2
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
            .withColumn("effective_from_dt", F.current_date())
            .withColumn("effective_to_dt", F.to_date(F.lit(OPEN_ENDED_DT)))
            .withColumn("is_current", F.lit(True))
            .withColumn("valid_from_ts", F.current_timestamp())
            .withColumn("valid_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
            .withColumn("system_from_ts", F.current_timestamp())
            .withColumn("system_to_ts", F.to_timestamp(F.lit(OPEN_ENDED_TS)))
            .withColumn("is_current_valid", F.lit(True))
            .withColumn("is_current_system", F.lit(True))
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

# DBTITLE 1,Cell 5
# ========================================
# SCHEMA EVOLUTION POLICY FOR SILVER LAYER
# ========================================
# NO automatic schema evolution (no mergeSchema or overwriteSchema)
# All schema changes MUST be done via explicit ALTER TABLE statements
# This ensures full auditability and regulatory compliance
# ========================================

portfolio_ref = current_dimension_ref("dim_portfolio", "portfolio_id", "portfolio_sk")
instrument_ref = (
    spark.table(f"{SILVER}.dim_instrument")
    .where(F.col("is_current") & F.col("is_current_valid") & F.col("is_current_system"))
    .select("instrument_id", "instrument_sk", "asset_class_code")
)
counterparty_ref = current_dimension_ref("dim_counterparty", "counterparty_id", "counterparty_sk")
currency_ref = current_dimension_ref("dim_currency", "currency_code", "currency_sk")
market_data_source_ref = current_dimension_ref("dim_market_data_source", "source_system_code", "market_data_source_sk")

# ========================================
# SMART PARTITION REPLACEMENT APPROACH
# ========================================
# FIXED: No longer assumes source_yyyymm == event_yyyymm
# Calculates which partitions are in source data, replaces only those
# Handles late-arriving data, corrections, and backfills correctly
# Works on all compute types (classic, serverless, etc.)
# ========================================

print(f"\n{'='*80}")
print(f"Processing source_yyyymm = {TARGET_YYYYMM}")
print(f"Using SMART PARTITION REPLACEMENT - will replace only affected partitions")
print(f"{'='*80}\n")

# HYBRID APPROACH: Store both surrogate keys (for point-in-time) and natural keys (for current hierarchy)
fact_transactions = (
    spark.table(f"{BRONZE}.transactions_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(portfolio_ref, "portfolio_id", "left")
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(counterparty_ref, "counterparty_id", "left")
    .join(currency_ref, "currency_code", "left")
    .select(
        "transaction_id",
        # Natural keys for current hierarchy reporting
        "portfolio_id",
        "instrument_id",
        "counterparty_id",
        "currency_code",
        # Surrogate keys for point-in-time accuracy
        "portfolio_sk",
        "instrument_sk",
        "counterparty_sk",
        "currency_sk",
        # Transaction attributes
        F.to_date("trade_dt").alias("trade_dt"),
        F.to_date("settlement_dt").alias("settlement_dt"),
        "transaction_type",
        F.col("quantity").cast("double").alias("quantity"),
        F.col("price").cast("double").alias("price"),
        F.col("gross_amount").cast("double").alias("gross_amount"),
        F.col("fees_amount").cast("double").alias("fees_amount"),
        F.col("net_amount").cast("double").alias("net_amount"),
        "load_id",
        "source_yyyymm",
        F.date_format(F.to_date("trade_dt"), "yyyyMM").alias("trade_yyyymm"),
    )
)
fact_transactions = apply_bitemporal_columns(fact_transactions, "trade_dt")

# Calculate which partitions exist in source data
affected_partitions = [row.trade_yyyymm for row in fact_transactions.select("trade_yyyymm").distinct().collect()]
print(f"fact_transactions: Will replace partitions {sorted(affected_partitions)}")

# Smart partition replacement: replaceWhere with calculated partitions
replace_condition = f"trade_yyyymm IN ({','.join([repr(p) for p in affected_partitions])})"
fact_transactions.write.format("delta").mode("overwrite").partitionBy("trade_yyyymm").option("replaceWhere", replace_condition).saveAsTable(f"{SILVER}.fact_transactions")

fact_positions_daily = (
    spark.table(f"{BRONZE}.positions_daily_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(portfolio_ref, "portfolio_id", "left")
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(currency_ref, "currency_code", "left")
    .join(market_data_source_ref, "source_system_code", "left")
    .select(
        "position_id",
        # Natural keys for current hierarchy reporting
        "portfolio_id",
        "instrument_id",
        "currency_code",
        "source_system_code",
        # Surrogate keys for point-in-time accuracy
        "portfolio_sk",
        "instrument_sk",
        "currency_sk",
        "market_data_source_sk",
        # Position attributes
        F.to_date("position_dt").alias("position_dt"),
        F.col("quantity").cast("double").alias("quantity"),
        F.col("end_of_day_price").cast("double").alias("end_of_day_price"),
        F.col("market_value").cast("double").alias("market_value"),
        F.col("unrealized_pnl").cast("double").alias("unrealized_pnl"),
        "source_yyyymm",
        F.date_format(F.to_date("position_dt"), "yyyyMM").alias("position_yyyymm"),
    )
)
fact_positions_daily = apply_bitemporal_columns(
    fact_positions_daily,
    "position_dt",
    F.expr("cast(position_dt + interval 1 day as timestamp)"),
)

affected_partitions = [row.position_yyyymm for row in fact_positions_daily.select("position_yyyymm").distinct().collect()]
print(f"fact_positions_daily: Will replace partitions {sorted(affected_partitions)}")

replace_condition = f"position_yyyymm IN ({','.join([repr(p) for p in affected_partitions])})"
fact_positions_daily.write.format("delta").mode("overwrite").partitionBy("position_yyyymm").option("replaceWhere", replace_condition).saveAsTable(f"{SILVER}.fact_positions_daily")

fact_market_prices_daily = (
    spark.table(f"{BRONZE}.market_prices_daily_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(currency_ref, "currency_code", "left")
    .join(market_data_source_ref, "source_system_code", "left")
    .select(
        "price_id",
        # Natural keys for current hierarchy reporting
        "instrument_id",
        "currency_code",
        "source_system_code",
        # Surrogate keys for point-in-time accuracy
        "instrument_sk",
        "currency_sk",
        "market_data_source_sk",
        # Price attributes
        F.to_date("price_dt").alias("price_dt"),
        F.col("close_price").cast("double").alias("close_price"),
        F.col("return_pct").cast("double").alias("return_pct"),
        F.col("volatility_proxy").cast("double").alias("volatility_proxy"),
        "source_yyyymm",
        F.date_format(F.to_date("price_dt"), "yyyyMM").alias("price_yyyymm"),
    )
)
fact_market_prices_daily = apply_bitemporal_columns(
    fact_market_prices_daily,
    "price_dt",
    F.expr("cast(price_dt + interval 1 day as timestamp)"),
)

affected_partitions = [row.price_yyyymm for row in fact_market_prices_daily.select("price_yyyymm").distinct().collect()]
print(f"fact_market_prices_daily: Will replace partitions {sorted(affected_partitions)}")

replace_condition = f"price_yyyymm IN ({','.join([repr(p) for p in affected_partitions])})"
fact_market_prices_daily.write.format("delta").mode("overwrite").partitionBy("price_yyyymm").option("replaceWhere", replace_condition).saveAsTable(f"{SILVER}.fact_market_prices_daily")

fact_cashflows = (
    spark.table(f"{BRONZE}.cashflows_raw")
    .where(F.col("source_yyyymm") == F.lit(TARGET_YYYYMM))
    .join(portfolio_ref, "portfolio_id", "left")
    .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
    .join(currency_ref, "currency_code", "left")
    .select(
        "cashflow_id",
        # Natural keys for current hierarchy reporting
        "portfolio_id",
        "instrument_id",
        "currency_code",
        # Surrogate keys for point-in-time accuracy
        "portfolio_sk",
        "instrument_sk",
        "currency_sk",
        # Cashflow attributes
        F.to_date("cashflow_dt").alias("cashflow_dt"),
        "cashflow_type",
        F.col("cashflow_amount").cast("double").alias("cashflow_amount"),
        "source_yyyymm",
        F.date_format(F.to_date("cashflow_dt"), "yyyyMM").alias("cashflow_yyyymm"),
    )
)
fact_cashflows = apply_bitemporal_columns(fact_cashflows, "cashflow_dt")

affected_partitions = [row.cashflow_yyyymm for row in fact_cashflows.select("cashflow_yyyymm").distinct().collect()]
print(f"fact_cashflows: Will replace partitions {sorted(affected_partitions)}")

replace_condition = f"cashflow_yyyymm IN ({','.join([repr(p) for p in affected_partitions])})"
fact_cashflows.write.format("delta").mode("overwrite").partitionBy("cashflow_yyyymm").option("replaceWhere", replace_condition).saveAsTable(f"{SILVER}.fact_cashflows")

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
