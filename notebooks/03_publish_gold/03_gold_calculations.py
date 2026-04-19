# Databricks notebook source
# MAGIC %md
# MAGIC # Quant Core - Gold Calculations

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

CATALOG = "quant_core"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
CONFIDENCE_LEVELS = [0.95, 0.99]
STRESS_WINDOW_START = "2024-09-01"
STRESS_WINDOW_END = "2024-11-30"


def current_bitemporal(df, business_col: str, system_col: str = "system_from_ts"):
    now_ts = F.current_timestamp()
    return df.where(
        (now_ts >= F.col("valid_from_ts"))
        & (now_ts < F.col("valid_to_ts"))
        & (now_ts >= F.col(system_col))
        & (now_ts < F.col("system_to_ts"))
    )


def irr(values, guess=0.10, max_iterations=100, tolerance=1e-7):
    rate = guess
    for _ in range(max_iterations):
        npv = 0.0
        derivative = 0.0
        for period_index, amount in enumerate(values):
            npv += amount / ((1 + rate) ** period_index)
            if period_index > 0:
                derivative -= period_index * amount / ((1 + rate) ** (period_index + 1))
        if abs(npv) < tolerance:
            return float(rate)
        if derivative == 0:
            break
        rate -= npv / derivative
    return None


def xirr(cashflow_series, guess=0.10, max_iterations=100, tolerance=1e-7):
    parsed = sorted(
        [(item["cashflow_dt"], float(item["cashflow_amount"])) for item in cashflow_series if item["cashflow_dt"] is not None],
        key=lambda x: x[0],
    )
    if len(parsed) < 2:
        return None

    start_dt = parsed[0][0]
    if isinstance(start_dt, str):
        start_dt = date.fromisoformat(start_dt)

    def xnpv(rate):
        total = 0.0
        for cashflow_dt, amount in parsed:
            dt = cashflow_dt if not isinstance(cashflow_dt, str) else date.fromisoformat(cashflow_dt)
            year_fraction = (dt - start_dt).days / 365.25
            total += amount / ((1 + rate) ** year_fraction)
        return total

    rate = guess
    for _ in range(max_iterations):
        value = xnpv(rate)
        if abs(value) < tolerance:
            return float(rate)
        derivative = (xnpv(rate + tolerance) - value) / tolerance
        if derivative == 0:
            break
        rate -= value / derivative
    return None


irr_udf = F.udf(lambda arr: irr([float(x) for x in arr]) if arr else None, DoubleType())
xirr_udf = F.udf(lambda arr: xirr(arr) if arr else None, DoubleType())


def publish_var_results() -> None:
    positions = current_bitemporal(spark.table(f"{SILVER}.fact_positions_daily"), "position_dt")
    prices = current_bitemporal(spark.table(f"{SILVER}.fact_market_prices_daily"), "system_from_ts")

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
            .withColumn("var_amount", -1 * F.col("loss_threshold"))
            .withColumn("var_pct_market_value", F.col("var_amount") / F.col("portfolio_market_value"))
            .select(
                "portfolio_sk",
                "as_of_date",
                "business_as_of_ts",
                "system_as_of_ts",
                "confidence_level",
                "holding_period_days",
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
                "svar_amount",
                "svar_pct_market_value",
            )
        )
        write_mode = "overwrite" if confidence_level == CONFIDENCE_LEVELS[0] else "append"
        results.write.format("delta").mode(write_mode).saveAsTable(f"{GOLD}.risk_svar_results")


def publish_return_results() -> None:
    cashflows = current_bitemporal(spark.table(f"{SILVER}.fact_cashflows"), "system_from_ts")
    positions = current_bitemporal(spark.table(f"{SILVER}.fact_positions_daily"), "system_from_ts")

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
        .select("portfolio_sk", "beginning_dt", "valuation_dt", "beginning_value", "ending_value", "years_held", "cagr")
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

    irr_results = irr_base.withColumn("irr", irr_udf(F.col("ordered_amounts"))).select("portfolio_sk", "irr")
    xirr_results = irr_base.withColumn("xirr", xirr_udf(F.col("cashflow_series"))).select("portfolio_sk", "xirr")

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

print("Gold calculations published.")
