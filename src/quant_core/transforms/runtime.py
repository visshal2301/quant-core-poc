"""Runtime entry points for Silver transformations."""

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from quant_core.ingestion.bronze import build_replace_where
from quant_core.transforms.dimensions import DIMENSION_SPECS
from quant_core.transforms.facts import FACT_CASHFLOWS_SPEC
from quant_core.transforms.facts import FACT_MARKET_PRICES_DAILY_SPEC
from quant_core.transforms.facts import FACT_POSITIONS_DAILY_SPEC
from quant_core.transforms.facts import FACT_TRANSACTIONS_SPEC
from quant_core.transforms.silver import OPEN_ENDED_DT, OPEN_ENDED_TS, add_surrogate_key, apply_bitemporal_columns, build_tracking_hash, current_dimension_ref


def ensure_dimension_table(spark, target_table_name, initial_df):
    if not spark.catalog.tableExists(target_table_name):
        initial_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)


def scd2_dimension_merge(spark, source_df, target_table_name, business_key, surrogate_key, tracked_columns):
    source_with_hash = build_tracking_hash(source_df.dropDuplicates([business_key]), tracked_columns)

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
        ensure_dimension_table(spark, target_table_name, initial_df)
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
        .select(
            "src.*",
            F.col("tgt.{0}".format(surrogate_key)).alias("existing_surrogate_key"),
            F.col("tgt.record_version").alias("existing_version"),
        )
    )

    if changes_df.limit(1).count() == 0:
        return

    changed_existing_keys = changes_df.where(F.col("existing_surrogate_key").isNotNull()).select(business_key).dropDuplicates()
    if changed_existing_keys.limit(1).count() > 0:
        (
            DeltaTable.forName(spark, target_table_name)
            .alias("tgt")
            .merge(changed_existing_keys.alias("chg"), "tgt.{0} = chg.{0} AND tgt.is_current = true".format(business_key))
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
        .withColumn("change_type", F.when(F.col("existing_surrogate_key").isNull(), F.lit("INSERT")).otherwise(F.lit("UPDATE")))
        .drop("existing_surrogate_key", "existing_version")
    )
    new_versions.select(*target_df.columns).write.format("delta").mode("append").saveAsTable(target_table_name)


def overwrite_static_dimension(spark, source_df, target_table_name, business_key, surrogate_key):
    """
    Overwrite static dimension with validation to prevent empty writes.
    
    Args:
        spark: SparkSession
        source_df: Source DataFrame with dimension data
        target_table_name: Full table name (catalog.schema.table)
        business_key: Natural business key column
        surrogate_key: Surrogate key column name
        
    Raises:
        ValueError: If source_df is empty and would result in empty dimension table
    """
    # Count source records BEFORE any transformations
    source_count = source_df.count()
    
    # If no source data and table doesn't exist, this is likely an error
    if source_count == 0:
        if not spark.catalog.tableExists(target_table_name):
            raise ValueError(
                f"Cannot create dimension table {target_table_name} with empty source data. "
                f"Check bronze table exists and has data for this period."
            )
        else:
            # Table exists but no new data - skip overwrite to preserve existing data
            print(f"⚠️  Warning: No source data for {target_table_name}, preserving existing data")
            existing_count = spark.table(target_table_name).count()
            print(f"   Existing records: {existing_count}")
            return
    
    # Proceed with transformation
    df = add_surrogate_key(source_df.dropDuplicates([business_key]), business_key, surrogate_key)
    df = (
        df.withColumn("effective_from_dt", F.current_date())
        .withColumn("effective_to_dt", F.to_date(F.lit(OPEN_ENDED_DT)))
        .withColumn("is_current", F.lit(True))
    )
    df = apply_bitemporal_columns(df, "effective_from_dt").withColumn("attribute_hash", F.lit(None).cast("string"))
    
    # Final validation before write
    final_count = df.count()
    if final_count == 0:
        raise ValueError(
            f"Transformation produced empty DataFrame for {target_table_name}. "
            f"Started with {source_count} records but ended with 0 after deduplication."
        )
    
    print(f"✓ Writing {final_count} records to {target_table_name}")
    df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)


def _source_df(spark, bronze_table_name, target_yyyymm, source_columns):
    return spark.table(bronze_table_name).where(F.col("source_yyyymm") == F.lit(target_yyyymm)).select(*source_columns)


def _write_partitioned_fact(df, target_table_name, partition_column):
    """
    Write partitioned fact table with validation for empty DataFrames.
    
    Args:
        df: DataFrame to write
        target_table_name: Target table name
        partition_column: Column to partition by
        
    Raises:
        ValueError: If DataFrame is empty (would cause partition_values error)
    """
    row_count = df.count()
    if row_count == 0:
        raise ValueError(
            f"Cannot write {target_table_name}: DataFrame is empty. "
            f"This usually means dimension joins resulted in no matching records. "
            f"Check that all required dimension tables are populated."
        )
    
    affected_partitions = [getattr(row, partition_column) for row in df.select(partition_column).distinct().collect()]
    
    if not affected_partitions:
        raise ValueError(
            f"Cannot write {target_table_name}: partition_values is empty "
            f"even though DataFrame has {row_count} rows. "
            f"Check that partition_column '{partition_column}' is not null."
        )
    
    replace_condition = build_replace_where(partition_column, affected_partitions)
    print(f"✓ Writing {row_count} records to {target_table_name} (partitions: {affected_partitions})")
    
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy(partition_column)
        .option("replaceWhere", replace_condition)
        .saveAsTable(target_table_name)
    )


def run_silver_transformation(spark, target_yyyymm, catalog="quant_core"):
    bronze = "{0}.bronze".format(catalog)
    silver = "{0}.silver".format(catalog)
    
    print(f"\n{'='*80}")
    print(f"Starting Silver Transformation for period: {target_yyyymm}")
    print(f"{'='*80}\n")

    # Process dimensions with error handling
    print("Processing Dimensions:")
    print("-" * 80)
    
    for spec in DIMENSION_SPECS:
        try:
            source_df = _source_df(spark, "{0}.{1}_raw".format(bronze, spec.table_name.replace("dim_", "" if spec.table_name != "dim_market_data_source" else "")), target_yyyymm, spec.source_columns)
            # Handle naming mismatches between silver dimensions and bronze raw tables.
            if spec.table_name == "dim_portfolio":
                source_df = _source_df(spark, "{0}.portfolios_raw".format(bronze), target_yyyymm, spec.source_columns)
            elif spec.table_name == "dim_instrument":
                source_df = _source_df(spark, "{0}.instruments_raw".format(bronze), target_yyyymm, spec.source_columns)
            elif spec.table_name == "dim_counterparty":
                source_df = _source_df(spark, "{0}.counterparties_raw".format(bronze), target_yyyymm, spec.source_columns)
            elif spec.table_name == "dim_currency":
                source_df = _source_df(spark, "{0}.currencies_raw".format(bronze), target_yyyymm, spec.source_columns)
            elif spec.table_name == "dim_asset_class":
                source_df = _source_df(spark, "{0}.asset_classes_raw".format(bronze), target_yyyymm, spec.source_columns)
            elif spec.table_name == "dim_market_data_source":
                source_df = _source_df(spark, "{0}.market_data_sources_raw".format(bronze), target_yyyymm, spec.source_columns)

            target_table_name = "{0}.{1}".format(silver, spec.table_name)
            
            if spec.scd2_enabled:
                print(f"  Processing {spec.table_name} (SCD2)...")
                scd2_dimension_merge(spark, source_df, target_table_name, spec.business_key, spec.surrogate_key, spec.tracked_columns)
            else:
                print(f"  Processing {spec.table_name} (static)...")
                overwrite_static_dimension(spark, source_df, target_table_name, spec.business_key, spec.surrogate_key)
                
        except Exception as e:
            print(f"\n❌ ERROR processing {spec.table_name}: {str(e)}")
            raise RuntimeError(f"Dimension {spec.table_name} failed: {str(e)}") from e
    
    print("\n✓ All dimensions processed successfully\n")

    # Process dim_date
    print("Processing dim_date...")
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
    dim_date.write.format("delta").mode("overwrite").saveAsTable("{0}.dim_date".format(silver))
    print("✓ dim_date processed\n")

    # Build dimension reference lookups
    print("Building dimension references...")
    print("-" * 80)
    
    portfolio_ref = current_dimension_ref(spark, "{0}.dim_portfolio".format(silver), "portfolio_id", "portfolio_sk")
    instrument_ref = (
        spark.table("{0}.dim_instrument".format(silver))
        .where(F.col("is_current") & F.col("is_current_valid") & F.col("is_current_system"))
        .select("instrument_id", "instrument_sk", "asset_class_code")
    )
    counterparty_ref = current_dimension_ref(spark, "{0}.dim_counterparty".format(silver), "counterparty_id", "counterparty_sk")
    currency_ref = current_dimension_ref(spark, "{0}.dim_currency".format(silver), "currency_code", "currency_sk")
    market_data_source_ref = current_dimension_ref(spark, "{0}.dim_market_data_source".format(silver), "source_system_code", "market_data_source_sk")
    
    # Validate critical dimensions
    for ref_name, ref_df in [
        ("portfolio_ref", portfolio_ref),
        ("instrument_ref", instrument_ref),
        ("currency_ref", currency_ref),
        ("counterparty_ref", counterparty_ref),
        ("market_data_source_ref", market_data_source_ref)
    ]:
        ref_count = ref_df.count()
        if ref_count == 0:
            raise ValueError(f"Critical dimension reference {ref_name} is empty! Cannot proceed with fact transformation.")
        print(f"  ✓ {ref_name}: {ref_count} records")
    
    print("\n✓ All dimension references validated\n")

    # Process fact tables
    print("Processing Fact Tables:")
    print("-" * 80)
    
    try:
        print("  Processing fact_transactions...")
        fact_transactions = (
            spark.table("{0}.transactions_raw".format(bronze))
            .where(F.col("source_yyyymm") == F.lit(target_yyyymm))
            .join(portfolio_ref, "portfolio_id", "left")
            .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
            .join(counterparty_ref, "counterparty_id", "left")
            .join(currency_ref, "currency_code", "left")
            .select(
                "transaction_id",
                "portfolio_id",
                "instrument_id",
                "counterparty_id",
                "currency_code",
                "portfolio_sk",
                "instrument_sk",
                "counterparty_sk",
                "currency_sk",
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
                F.date_format(F.to_date("trade_dt"), "yyyyMM").alias(FACT_TRANSACTIONS_SPEC.partition_column),
            )
        )
        fact_transactions = apply_bitemporal_columns(fact_transactions, FACT_TRANSACTIONS_SPEC.event_date_column)
        _write_partitioned_fact(fact_transactions, "{0}.{1}".format(silver, FACT_TRANSACTIONS_SPEC.table_name), FACT_TRANSACTIONS_SPEC.partition_column)
    except Exception as e:
        print(f"\n❌ ERROR processing fact_transactions: {str(e)}")
        raise

    try:
        print("  Processing fact_positions_daily...")
        fact_positions_daily = (
            spark.table("{0}.positions_daily_raw".format(bronze))
            .where(F.col("source_yyyymm") == F.lit(target_yyyymm))
            .join(portfolio_ref, "portfolio_id", "left")
            .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
            .join(currency_ref, "currency_code", "left")
            .join(market_data_source_ref, "source_system_code", "left")
            .select(
                "position_id",
                "portfolio_id",
                "instrument_id",
                "currency_code",
                "source_system_code",
                "portfolio_sk",
                "instrument_sk",
                "currency_sk",
                "market_data_source_sk",
                F.to_date("position_dt").alias("position_dt"),
                F.col("quantity").cast("double").alias("quantity"),
                F.col("end_of_day_price").cast("double").alias("end_of_day_price"),
                F.col("market_value").cast("double").alias("market_value"),
                F.col("unrealized_pnl").cast("double").alias("unrealized_pnl"),
                "source_yyyymm",
                F.date_format(F.to_date("position_dt"), "yyyyMM").alias(FACT_POSITIONS_DAILY_SPEC.partition_column),
            )
        )
        fact_positions_daily = apply_bitemporal_columns(
            fact_positions_daily,
            FACT_POSITIONS_DAILY_SPEC.event_date_column,
            F.expr("cast(position_dt + interval 1 day as timestamp)"),
        )
        _write_partitioned_fact(
            fact_positions_daily,
            "{0}.{1}".format(silver, FACT_POSITIONS_DAILY_SPEC.table_name),
            FACT_POSITIONS_DAILY_SPEC.partition_column,
        )
    except Exception as e:
        print(f"\n❌ ERROR processing fact_positions_daily: {str(e)}")
        raise

    try:
        print("  Processing fact_market_prices_daily...")
        fact_market_prices_daily = (
            spark.table("{0}.market_prices_daily_raw".format(bronze))
            .where(F.col("source_yyyymm") == F.lit(target_yyyymm))
            .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
            .join(currency_ref, "currency_code", "left")
            .join(market_data_source_ref, "source_system_code", "left")
            .select(
                "price_id",
                "instrument_id",
                "currency_code",
                "source_system_code",
                "instrument_sk",
                "currency_sk",
                "market_data_source_sk",
                F.to_date("price_dt").alias("price_dt"),
                F.col("close_price").cast("double").alias("close_price"),
                F.col("return_pct").cast("double").alias("return_pct"),
                F.col("volatility_proxy").cast("double").alias("volatility_proxy"),
                "source_yyyymm",
                F.date_format(F.to_date("price_dt"), "yyyyMM").alias(FACT_MARKET_PRICES_DAILY_SPEC.partition_column),
            )
        )
        fact_market_prices_daily = apply_bitemporal_columns(
            fact_market_prices_daily,
            FACT_MARKET_PRICES_DAILY_SPEC.event_date_column,
            F.expr("cast(price_dt + interval 1 day as timestamp)"),
        )
        _write_partitioned_fact(
            fact_market_prices_daily,
            "{0}.{1}".format(silver, FACT_MARKET_PRICES_DAILY_SPEC.table_name),
            FACT_MARKET_PRICES_DAILY_SPEC.partition_column,
        )
    except Exception as e:
        print(f"\n❌ ERROR processing fact_market_prices_daily: {str(e)}")
        raise

    try:
        print("  Processing fact_cashflows...")
        fact_cashflows = (
            spark.table("{0}.cashflows_raw".format(bronze))
            .where(F.col("source_yyyymm") == F.lit(target_yyyymm))
            .join(portfolio_ref, "portfolio_id", "left")
            .join(instrument_ref.select("instrument_id", "instrument_sk"), "instrument_id", "left")
            .join(currency_ref, "currency_code", "left")
            .select(
                "cashflow_id",
                "portfolio_id",
                "instrument_id",
                "currency_code",
                "portfolio_sk",
                "instrument_sk",
                "currency_sk",
                F.to_date("cashflow_dt").alias("cashflow_dt"),
                "cashflow_type",
                F.col("cashflow_amount").cast("double").alias("cashflow_amount"),
                "source_yyyymm",
                F.date_format(F.to_date("cashflow_dt"), "yyyyMM").alias(FACT_CASHFLOWS_SPEC.partition_column),
            )
        )
        fact_cashflows = apply_bitemporal_columns(fact_cashflows, FACT_CASHFLOWS_SPEC.event_date_column)
        _write_partitioned_fact(fact_cashflows, "{0}.{1}".format(silver, FACT_CASHFLOWS_SPEC.table_name), FACT_CASHFLOWS_SPEC.partition_column)
    except Exception as e:
        print(f"\n❌ ERROR processing fact_cashflows: {str(e)}")
        raise
    
    print(f"\n{'='*80}")
    print(f"✅ Silver Transformation Complete for period: {target_yyyymm}")
    print(f"{'='*80}\n")
