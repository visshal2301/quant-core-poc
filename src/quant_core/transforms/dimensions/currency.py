"""Currency dimension spec."""

from quant_core.models import DimensionSpec


DIM_CURRENCY_SPEC = DimensionSpec(
    table_name="dim_currency",
    business_key="currency_code",
    surrogate_key="currency_sk",
    tracked_columns=["currency_name", "decimal_precision"],
    source_columns=["currency_code", "currency_name", "decimal_precision"],
    scd2_enabled=False,
)

