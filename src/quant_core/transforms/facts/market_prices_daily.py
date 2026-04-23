"""Daily market price fact spec."""

from quant_core.models import FactSpec


FACT_MARKET_PRICES_DAILY_SPEC = FactSpec(
    table_name="fact_market_prices_daily",
    event_date_column="price_dt",
    partition_column="price_yyyymm",
    natural_keys=["instrument_id", "currency_code", "source_system_code"],
    numeric_columns=["close_price", "return_pct", "volatility_proxy"],
    reference_keys=["instrument_sk", "currency_sk", "market_data_source_sk"],
    extra_columns=["price_id", "source_yyyymm"],
)

