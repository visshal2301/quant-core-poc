# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Quant Core - Gold Calculations

# COMMAND ----------

# DBTITLE 1,Setup and Utility Functions
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

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


def current_bitemporal(df, business_col: str, system_col: str = "system_from_ts"):
    now_ts = F.current_timestamp()
    return df.where(
        (now_ts >= F.col("valid_from_ts"))
        & (now_ts < F.col("valid_to_ts"))
        & (now_ts >= F.col(system_col))
        & (now_ts < F.col("system_to_ts"))
    )


def irr(values, guess=0.10, max_iterations=100, tolerance=1e-7):
    """Calculate IRR with overflow protection and validation."""
    try:
        # Validate input
        if not values or len(values) < 2:
            return None
        
        # Check for invalid patterns (all same sign)
        positive_count = sum(1 for v in values if v > 0)
        negative_count = sum(1 for v in values if v < 0)
        
        if positive_count == 0 or negative_count == 0:
            return None  # IRR undefined for all same-sign cashflows
        
        rate = guess
        # Rate bounds to prevent overflow: -99% to 10,000%
        MIN_RATE = -0.99
        MAX_RATE = 100.0
        
        for iteration in range(max_iterations):
            # Bounds checking to prevent divergence
            if rate < MIN_RATE or rate > MAX_RATE:
                return None  # Rate out of reasonable bounds
            
            npv = 0.0
            derivative = 0.0
            
            for period_index, amount in enumerate(values):
                try:
                    # Check if calculation will overflow before computing
                    if period_index > 0 and abs(1 + rate) > 1e10:
                        return None  # Prevent overflow
                    
                    power_term = (1 + rate) ** period_index
                    npv += amount / power_term
                    
                    if period_index > 0:
                        derivative -= period_index * amount / ((1 + rate) ** (period_index + 1))
                except (OverflowError, ZeroDivisionError):
                    return None  # Calculation overflow or division by zero
            
            if abs(npv) < tolerance:
                return float(rate)
            
            if derivative == 0 or abs(derivative) < 1e-10:
                break  # Derivative too small, cannot continue
            
            # Newton-Raphson step with damping for stability
            rate_delta = npv / derivative
            
            # Limit step size to prevent wild jumps
            max_step = 0.5
            if abs(rate_delta) > max_step:
                rate_delta = max_step if rate_delta > 0 else -max_step
            
            rate -= rate_delta
        
        return None  # Did not converge
    except Exception:
        return None  # Any other error


def xirr(cashflow_series, guess=0.10, max_iterations=100, tolerance=1e-7):
    """Calculate XIRR with overflow protection and validation."""
    try:
        parsed = sorted(
            [(item["cashflow_dt"], float(item["cashflow_amount"])) for item in cashflow_series if item["cashflow_dt"] is not None],
            key=lambda x: x[0],
        )
        if len(parsed) < 2:
            return None
        
        # Check for invalid patterns (all same sign)
        positive_count = sum(1 for _, amount in parsed if amount > 0)
        negative_count = sum(1 for _, amount in parsed if amount < 0)
        
        if positive_count == 0 or negative_count == 0:
            return None  # XIRR undefined for all same-sign cashflows

        start_dt = parsed[0][0]
        if isinstance(start_dt, str):
            start_dt = date.fromisoformat(start_dt)

        def xnpv(rate):
            total = 0.0
            for cashflow_dt, amount in parsed:
                dt = cashflow_dt if not isinstance(cashflow_dt, str) else date.fromisoformat(cashflow_dt)
                year_fraction = (dt - start_dt).days / 365.25
                try:
                    # Prevent overflow in power calculation
                    if abs(1 + rate) > 1e10 or year_fraction * abs(rate) > 50:
                        raise OverflowError
                    total += amount / ((1 + rate) ** year_fraction)
                except (OverflowError, ZeroDivisionError):
                    return float('inf') if rate > 0 else float('-inf')
            return total

        rate = guess
        # Rate bounds to prevent overflow
        MIN_RATE = -0.99
        MAX_RATE = 100.0
        
        for _ in range(max_iterations):
            if rate < MIN_RATE or rate > MAX_RATE:
                return None  # Rate out of reasonable bounds
            
            value = xnpv(rate)
            
            if not (-1e15 < value < 1e15):  # Check for infinity or extreme values
                return None
            
            if abs(value) < tolerance:
                return float(rate)
            
            # Numerical derivative
            derivative = (xnpv(rate + tolerance) - value) / tolerance
            
            if derivative == 0 or abs(derivative) < 1e-10 or not (-1e15 < derivative < 1e15):
                break
            
            # Newton-Raphson step with damping
            rate_delta = value / derivative
            max_step = 0.5
            if abs(rate_delta) > max_step:
                rate_delta = max_step if rate_delta > 0 else -max_step
            
            rate -= rate_delta
        
        return None  # Did not converge
    except Exception:
        return None  # Any other error


irr_udf = F.udf(lambda arr: irr([float(x) for x in arr]) if arr else None, DoubleType())
xirr_udf = F.udf(lambda arr: xirr(arr) if arr else None, DoubleType())


def publish_var_results() -> None:
    positions = current_bitemporal(spark.table(f"{SILVER}.fact_positions_daily"), "position_dt").where(
        F.col("position_yyyymm") == F.lit(TARGET_YYYYMM)
    )
    prices = current_bitemporal(spark.table(f"{SILVER}.fact_market_prices_daily"), "system_from_ts").where(
        F.col("price_yyyymm") == F.lit(TARGET_YYYYMM)
    )

    pnl_distribution = (
        positions.alias("p")
        .join(prices.alias("m"), F.col("p.instrument_sk") == F.col("m.instrument_sk"), "inner")
        .where(F.col("m.price_dt") <= F.col("p.position_dt"))
        .select(
            F.col("p.portfolio_sk"),
            F.col("p.position_dt").alias("as_of_date"),
            F.col("p.valid_from_ts").alias("business_as_of_ts"),
            F.current_timestamp().alias("system_as_of_ts"),
            F.col("m.price_dt").alias("historical_dt"),
            (F.col("p.market_value") * F.col("m.return_pct")).alias("simulated_pnl"),
            F.col("p.market_value"),
        )
    )
    pnl_distribution.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.risk_pnl_distribution")

    for confidence_level in CONFIDENCE_LEVELS:
        results = (
            pnl_distribution.groupBy("portfolio_sk", "as_of_date", "business_as_of_ts", "system_as_of_ts")
            .agg(
                F.expr(f"percentile_approx(simulated_pnl, {1 - confidence_level})").alias("loss_threshold"),
                F.sum("market_value").alias("portfolio_market_value"),
                F.count("*").alias("simulations_count"),
            )
            .withColumn("confidence_level", F.lit(confidence_level))
            .withColumn("holding_period_days", F.lit(1))
            .withColumn("result_yyyymm", F.lit(TARGET_YYYYMM))
            .withColumn("var_amount", -1 * F.col("loss_threshold"))
            .withColumn("var_pct_market_value", F.col("var_amount") / F.col("portfolio_market_value"))
            .select(
                "portfolio_sk",
                "as_of_date",
                "business_as_of_ts",
                "system_as_of_ts",
                "confidence_level",
                "holding_period_days",
                "result_yyyymm",
                "simulations_count",
                "var_amount",
                "var_pct_market_value",
            )
        )
        write_mode = "overwrite" if confidence_level == CONFIDENCE_LEVELS[0] else "append"
        results.write.format("delta").mode(write_mode).saveAsTable(f"{GOLD}.risk_var_results")


def publish_svar_results() -> None:
    pnl_distribution = spark.table(f"{GOLD}.risk_pnl_distribution").where(
        (F.col("historical_dt") >= F.to_date(F.lit(STRESS_WINDOW_START)))
        & (F.col("historical_dt") <= F.to_date(F.lit(STRESS_WINDOW_END)))
    )

    for confidence_level in CONFIDENCE_LEVELS:
        results = (
            pnl_distribution.groupBy("portfolio_sk", "as_of_date", "business_as_of_ts", "system_as_of_ts")
            .agg(
                F.expr(f"percentile_approx(simulated_pnl, {1 - confidence_level})").alias("loss_threshold"),
                F.sum("market_value").alias("portfolio_market_value"),
            )
            .withColumn("confidence_level", F.lit(confidence_level))
            .withColumn("stress_window_start_dt", F.to_date(F.lit(STRESS_WINDOW_START)))
            .withColumn("stress_window_end_dt", F.to_date(F.lit(STRESS_WINDOW_END)))
            .withColumn("result_yyyymm", F.lit(TARGET_YYYYMM))
            .withColumn("svar_amount", -1 * F.col("loss_threshold"))
            .withColumn("svar_pct_market_value", F.col("svar_amount") / F.col("portfolio_market_value"))
            .select(
                "portfolio_sk",
                "as_of_date",
                "business_as_of_ts",
                "system_as_of_ts",
                "stress_window_start_dt",
                "stress_window_end_dt",
                "confidence_level",
                "result_yyyymm",
                "svar_amount",
                "svar_pct_market_value",
            )
        )
        write_mode = "overwrite" if confidence_level == CONFIDENCE_LEVELS[0] else "append"
        results.write.format("delta").mode(write_mode).saveAsTable(f"{GOLD}.risk_svar_results")


def publish_return_results() -> None:
    cashflows = current_bitemporal(spark.table(f"{SILVER}.fact_cashflows"), "system_from_ts").where(
        F.col("cashflow_yyyymm") == F.lit(TARGET_YYYYMM)
    )
    positions = current_bitemporal(spark.table(f"{SILVER}.fact_positions_daily"), "system_from_ts").where(
        F.col("position_yyyymm") == F.lit(TARGET_YYYYMM)
    )

    bounds = positions.groupBy("portfolio_sk").agg(
        F.min("position_dt").alias("beginning_dt"),
        F.max("position_dt").alias("valuation_dt"),
    )

    beginning_value = (
        positions.join(bounds, "portfolio_sk", "inner")
        .where(F.col("position_dt") == F.col("beginning_dt"))
        .groupBy("portfolio_sk", "beginning_dt")
        .agg(F.sum("market_value").alias("beginning_value"))
    )

    terminal_value = (
        positions.join(bounds, "portfolio_sk", "inner")
        .where(F.col("position_dt") == F.col("valuation_dt"))
        .groupBy("portfolio_sk", "valuation_dt")
        .agg(F.sum("market_value").alias("ending_value"))
    )

    cagr_results = (
        beginning_value.join(terminal_value, "portfolio_sk", "inner")
        .withColumn("years_held", F.datediff("valuation_dt", "beginning_dt") / F.lit(365.25))
        .withColumn("cagr", F.pow(F.col("ending_value") / F.col("beginning_value"), 1 / F.col("years_held")) - 1)
        .withColumn("result_yyyymm", F.lit(TARGET_YYYYMM))
        .select("portfolio_sk", "beginning_dt", "valuation_dt", "beginning_value", "ending_value", "years_held", "cagr", "result_yyyymm")
    )
    cagr_results.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.performance_cagr_results")

    terminal_cashflow = terminal_value.select(
        "portfolio_sk",
        F.col("valuation_dt").alias("cashflow_dt"),
        F.col("ending_value").alias("cashflow_amount"),
    )

    cashflow_union = cashflows.select("portfolio_sk", "cashflow_dt", "cashflow_amount").unionByName(terminal_cashflow)

    irr_base = (
        cashflow_union.groupBy("portfolio_sk")
        .agg(
            F.sort_array(F.collect_list(F.struct("cashflow_dt", "cashflow_amount"))).alias("cashflow_series"),
            F.sum("cashflow_amount").alias("net_cashflow"),
        )
        .join(terminal_value.select("portfolio_sk", "ending_value"), "portfolio_sk", "left")
        .withColumn("ordered_amounts", F.expr("transform(cashflow_series, x -> x.cashflow_amount)"))
    )

    irr_results = irr_base.withColumn("irr", irr_udf(F.col("ordered_amounts"))).withColumn("result_yyyymm", F.lit(TARGET_YYYYMM)).select("portfolio_sk", "irr", "result_yyyymm")
    xirr_results = irr_base.withColumn("xirr", xirr_udf(F.col("cashflow_series"))).withColumn("result_yyyymm", F.lit(TARGET_YYYYMM)).select("portfolio_sk", "xirr", "result_yyyymm")

    irr_results.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD}.performance_irr_results"
    )
    xirr_results.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD}.performance_xirr_results"
    )


# COMMAND ----------

publish_var_results()
publish_svar_results()
publish_return_results()

print(f"Gold calculations published for target month {TARGET_YYYYMM}.")
