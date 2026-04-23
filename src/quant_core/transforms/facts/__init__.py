"""Fact-level transformation specs."""

from quant_core.transforms.facts.cashflows import FACT_CASHFLOWS_SPEC
from quant_core.transforms.facts.market_prices_daily import FACT_MARKET_PRICES_DAILY_SPEC
from quant_core.transforms.facts.positions_daily import FACT_POSITIONS_DAILY_SPEC
from quant_core.transforms.facts.transactions import FACT_TRANSACTIONS_SPEC

FACT_SPECS = [
    FACT_TRANSACTIONS_SPEC,
    FACT_POSITIONS_DAILY_SPEC,
    FACT_MARKET_PRICES_DAILY_SPEC,
    FACT_CASHFLOWS_SPEC,
]

