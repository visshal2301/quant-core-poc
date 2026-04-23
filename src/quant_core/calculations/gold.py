"""Reusable Gold-layer helpers."""

from quant_core.calculations.finance import irr, xirr
from quant_core.ingestion.bronze import build_replace_where


def build_month_replace_condition(partition_column, partition_value):
    return build_replace_where(partition_column, [partition_value])


__all__ = ["irr", "xirr", "build_month_replace_condition"]

