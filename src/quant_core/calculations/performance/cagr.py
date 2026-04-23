"""CAGR-specific reusable metadata."""

from quant_core.ingestion.bronze import build_replace_where

CAGR_RESULT_PARTITION_COLUMN = "result_yyyymm"


def cagr_replace_condition(target_yyyymm):
    return build_replace_where(CAGR_RESULT_PARTITION_COLUMN, [target_yyyymm])

