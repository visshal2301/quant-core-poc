"""VaR-specific reusable metadata."""

from quant_core.ingestion.bronze import build_replace_where

PNL_PARTITION_COLUMN = "position_yyyymm"
VAR_RESULT_PARTITION_COLUMN = "result_yyyymm"


def pnl_replace_condition(target_yyyymm):
    return build_replace_where(PNL_PARTITION_COLUMN, [target_yyyymm])


def var_replace_condition(target_yyyymm):
    return build_replace_where(VAR_RESULT_PARTITION_COLUMN, [target_yyyymm])

