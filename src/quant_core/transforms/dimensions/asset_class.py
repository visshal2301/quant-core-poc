"""Asset class dimension spec."""

from quant_core.models import DimensionSpec


DIM_ASSET_CLASS_SPEC = DimensionSpec(
    table_name="dim_asset_class",
    business_key="asset_class_code",
    surrogate_key="asset_class_sk",
    tracked_columns=["asset_class_name", "risk_bucket"],
    source_columns=["asset_class_code", "asset_class_name", "risk_bucket"],
    scd2_enabled=False,
)

