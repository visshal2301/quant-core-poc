"""Market data source dimension spec."""

from quant_core.models import DimensionSpec


DIM_MARKET_DATA_SOURCE_SPEC = DimensionSpec(
    table_name="dim_market_data_source",
    business_key="source_system_code",
    surrogate_key="market_data_source_sk",
    tracked_columns=["source_system_name", "vendor_name"],
    source_columns=["source_system_code", "source_system_name", "vendor_name"],
    scd2_enabled=False,
)

