"""Portfolio dimension spec."""

from quant_core.models import DimensionSpec


DIM_PORTFOLIO_SPEC = DimensionSpec(
    table_name="dim_portfolio",
    business_key="portfolio_id",
    surrogate_key="portfolio_sk",
    tracked_columns=["portfolio_name", "portfolio_type", "base_currency_code", "risk_policy_name"],
    source_columns=["portfolio_id", "portfolio_name", "portfolio_type", "base_currency_code", "risk_policy_name"],
    scd2_enabled=True,
)

