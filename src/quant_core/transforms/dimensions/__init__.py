"""Dimension-level transformation specs."""

from quant_core.transforms.dimensions.asset_class import DIM_ASSET_CLASS_SPEC
from quant_core.transforms.dimensions.counterparty import DIM_COUNTERPARTY_SPEC
from quant_core.transforms.dimensions.currency import DIM_CURRENCY_SPEC
from quant_core.transforms.dimensions.instrument import DIM_INSTRUMENT_SPEC
from quant_core.transforms.dimensions.market_data_source import DIM_MARKET_DATA_SOURCE_SPEC
from quant_core.transforms.dimensions.portfolio import DIM_PORTFOLIO_SPEC

DIMENSION_SPECS = [
    DIM_PORTFOLIO_SPEC,
    DIM_INSTRUMENT_SPEC,
    DIM_COUNTERPARTY_SPEC,
    DIM_CURRENCY_SPEC,
    DIM_ASSET_CLASS_SPEC,
    DIM_MARKET_DATA_SOURCE_SPEC,
]

