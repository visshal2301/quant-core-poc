"""Bronze ingestion specs for fact datasets."""

from quant_core.models import BronzeTableSpec


FACT_BRONZE_SPECS = [
    BronzeTableSpec(dataset="transactions", table_name="transactions_raw", expected_grain="transaction"),
    BronzeTableSpec(dataset="positions_daily", table_name="positions_daily_raw", expected_grain="daily_position"),
    BronzeTableSpec(dataset="market_prices_daily", table_name="market_prices_daily_raw", expected_grain="daily_market_price"),
    BronzeTableSpec(dataset="cashflows", table_name="cashflows_raw", expected_grain="cashflow"),
]

