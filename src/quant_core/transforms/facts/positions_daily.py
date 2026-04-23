"""Daily position fact spec."""

from quant_core.models import FactSpec


FACT_POSITIONS_DAILY_SPEC = FactSpec(
    table_name="fact_positions_daily",
    event_date_column="position_dt",
    partition_column="position_yyyymm",
    natural_keys=["portfolio_id", "instrument_id", "currency_code", "source_system_code"],
    numeric_columns=["quantity", "end_of_day_price", "market_value", "unrealized_pnl"],
    reference_keys=["portfolio_sk", "instrument_sk", "currency_sk", "market_data_source_sk"],
    extra_columns=["position_id", "source_yyyymm"],
)

