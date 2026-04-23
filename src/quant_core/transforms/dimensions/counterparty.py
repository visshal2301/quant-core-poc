"""Counterparty dimension spec."""

from quant_core.models import DimensionSpec


DIM_COUNTERPARTY_SPEC = DimensionSpec(
    table_name="dim_counterparty",
    business_key="counterparty_id",
    surrogate_key="counterparty_sk",
    tracked_columns=["counterparty_name", "counterparty_type", "country_code", "credit_rating"],
    source_columns=["counterparty_id", "counterparty_name", "counterparty_type", "country_code", "credit_rating"],
    scd2_enabled=True,
)

