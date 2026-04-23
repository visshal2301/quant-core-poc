"""Runtime entry points for Gold calculations."""

from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from quant_core.calculations.finance import irr, xirr
from quant_core.calculations.performance.cagr import cagr_replace_condition
from quant_core.calculations.performance.irr import irr_replace_condition
from quant_core.calculations.performance.xirr import xirr_replace_condition
from quant_core.calculations.risk.svar import svar_replace_condition
from quant_core.calculations.risk.var import pnl_replace_condition, var_replace_condition

CONFIDENCE_LEVELS = [0.95, 0.99]
STRESS_WINDOW_START = "2024-09-01"
STRESS_WINDOW_END = "2024-11-30"


def publish_var_results(spark, target_yyyymm, silver="quant_core.silver", gold="quant_core.gold"):
    irr_udf = F.udf(lambda arr: irr([float(x) for x in arr]) if arr else None, DoubleType())
    xirr_udf = F.udf(lambda arr: xirr(arr) if arr else None, DoubleType())
    # Keep UDFs defined in this runtime for notebook compatibility; returned below for reuse.
    _ = (irr_udf, xirr_udf)

    positions = spark.table("{0}.fact_positions_daily".format(silver)).where(F.col("position_yyyymm") == F.lit(target_yyyymm))
    prices = spark.table("{0}.fact_market_prices_daily".format(silver))

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
            F.col("p.position_yyyymm"),
        )
    )
    (
        pnl_distribution.write.format("delta")
        .mode("overwrite")
        .partitionBy("position_yyyymm")
        .option("replaceWhere", pnl_replace_condition(target_yyyymm))
        .saveAsTable("{0}.risk_pnl_distribution".format(gold))
    )

    all_var_results = []
    for confidence_level in CONFIDENCE_LEVELS:
        results = (
            pnl_distribution.groupBy("portfolio_sk", "as_of_date", "business_as_of_ts", "system_as_of_ts")
            .agg(
                F.expr("percentile_approx(simulated_pnl, {0})".format(1 - confidence_level)).alias("loss_threshold"),
                F.sum("market_value").alias("portfolio_market_value"),
                F.count("*").alias("simulations_count"),
            )
            .withColumn("confidence_level", F.lit(confidence_level))
            .withColumn("holding_period_days", F.lit(1))
            .withColumn("result_yyyymm", F.lit(target_yyyymm))
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
        all_var_results.append(results)
    combined_var_results = reduce(lambda df1, df2: df1.unionByName(df2), all_var_results)
    (
        combined_var_results.write.format("delta")
        .mode("overwrite")
        .partitionBy("result_yyyymm")
        .option("replaceWhere", var_replace_condition(target_yyyymm))
        .saveAsTable("{0}.risk_var_results".format(gold))
    )


def publish_svar_results(spark, target_yyyymm, gold="quant_core.gold"):
    pnl_distribution = (
        spark.table("{0}.risk_pnl_distribution".format(gold))
        .where(F.col("position_yyyymm") == F.lit(target_yyyymm))
        .where((F.col("historical_dt") >= F.to_date(F.lit(STRESS_WINDOW_START))) & (F.col("historical_dt") <= F.to_date(F.lit(STRESS_WINDOW_END))))
    )
    all_svar_results = []
    for confidence_level in CONFIDENCE_LEVELS:
        results = (
            pnl_distribution.groupBy("portfolio_sk", "as_of_date", "business_as_of_ts", "system_as_of_ts")
            .agg(
                F.expr("percentile_approx(simulated_pnl, {0})".format(1 - confidence_level)).alias("loss_threshold"),
                F.sum("market_value").alias("portfolio_market_value"),
            )
            .withColumn("confidence_level", F.lit(confidence_level))
            .withColumn("stress_window_start_dt", F.to_date(F.lit(STRESS_WINDOW_START)))
            .withColumn("stress_window_end_dt", F.to_date(F.lit(STRESS_WINDOW_END)))
            .withColumn("result_yyyymm", F.lit(target_yyyymm))
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
        all_svar_results.append(results)
    combined_svar_results = reduce(lambda df1, df2: df1.unionByName(df2), all_svar_results)
    (
        combined_svar_results.write.format("delta")
        .mode("overwrite")
        .partitionBy("result_yyyymm")
        .option("replaceWhere", svar_replace_condition(target_yyyymm))
        .saveAsTable("{0}.risk_svar_results".format(gold))
    )


def publish_return_results(spark, target_yyyymm, silver="quant_core.silver", gold="quant_core.gold"):
    irr_udf = F.udf(lambda arr: irr([float(x) for x in arr]) if arr else None, DoubleType())
    xirr_udf = F.udf(lambda arr: xirr(arr) if arr else None, DoubleType())

    cashflows = spark.table("{0}.fact_cashflows".format(silver)).where(F.col("cashflow_yyyymm") == F.lit(target_yyyymm))
    positions = spark.table("{0}.fact_positions_daily".format(silver)).where(F.col("position_yyyymm") == F.lit(target_yyyymm))

    bounds = positions.groupBy("portfolio_sk").agg(F.min("position_dt").alias("beginning_dt"), F.max("position_dt").alias("valuation_dt"))
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
        .withColumn("result_yyyymm", F.lit(target_yyyymm))
        .select("portfolio_sk", "beginning_dt", "valuation_dt", "beginning_value", "ending_value", "years_held", "cagr", "result_yyyymm")
    )
    (
        cagr_results.write.format("delta")
        .mode("overwrite")
        .partitionBy("result_yyyymm")
        .option("replaceWhere", cagr_replace_condition(target_yyyymm))
        .saveAsTable("{0}.performance_cagr_results".format(gold))
    )

    terminal_cashflow = terminal_value.select("portfolio_sk", F.col("valuation_dt").alias("cashflow_dt"), F.col("ending_value").alias("cashflow_amount"))
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

    irr_results = irr_base.withColumn("irr", irr_udf(F.col("ordered_amounts"))).withColumn("result_yyyymm", F.lit(target_yyyymm)).select("portfolio_sk", "irr", "result_yyyymm")
    xirr_results = irr_base.withColumn("xirr", xirr_udf(F.col("cashflow_series"))).withColumn("result_yyyymm", F.lit(target_yyyymm)).select("portfolio_sk", "xirr", "result_yyyymm")

    (
        irr_results.write.format("delta")
        .mode("overwrite")
        .partitionBy("result_yyyymm")
        .option("replaceWhere", irr_replace_condition(target_yyyymm))
        .saveAsTable("{0}.performance_irr_results".format(gold))
    )
    (
        xirr_results.write.format("delta")
        .mode("overwrite")
        .partitionBy("result_yyyymm")
        .option("replaceWhere", xirr_replace_condition(target_yyyymm))
        .saveAsTable("{0}.performance_xirr_results".format(gold))
    )


def run_gold_calculations(spark, target_yyyymm, catalog="quant_core"):
    silver = "{0}.silver".format(catalog)
    gold = "{0}.gold".format(catalog)
    publish_var_results(spark, target_yyyymm, silver=silver, gold=gold)
    publish_svar_results(spark, target_yyyymm, gold=gold)
    publish_return_results(spark, target_yyyymm, silver=silver, gold=gold)

