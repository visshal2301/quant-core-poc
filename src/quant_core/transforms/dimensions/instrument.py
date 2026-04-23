"""Instrument dimension spec."""

from quant_core.models import DimensionSpec


DIM_INSTRUMENT_SPEC = DimensionSpec(
    table_name="dim_instrument",
    business_key="instrument_id",
    surrogate_key="instrument_sk",
    tracked_columns=[
        "instrument_name",
        "ticker",
        "isin",
        "asset_class_code",
        "currency_code",
        "issuer_name",
        "coupon_rate",
        "maturity_dt",
    ],
    source_columns=[
        "instrument_id",
        "instrument_name",
        "ticker",
        "isin",
        "asset_class_code",
        "currency_code",
        "issuer_name",
        "coupon_rate",
        "maturity_dt",
    ],
    scd2_enabled=True,
)

