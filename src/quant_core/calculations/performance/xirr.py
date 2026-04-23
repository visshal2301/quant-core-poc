"""XIRR-specific reusable metadata."""

from quant_core.ingestion.bronze import build_replace_where

XIRR_RESULT_PARTITION_COLUMN = "result_yyyymm"


def xirr_replace_condition(target_yyyymm):
    return build_replace_where(XIRR_RESULT_PARTITION_COLUMN, [target_yyyymm])

