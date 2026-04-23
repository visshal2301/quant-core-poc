from quant_core.calculations.finance import irr, xirr
from quant_core.calculations.performance.cagr import cagr_replace_condition
from quant_core.calculations.risk.var import pnl_replace_condition
from quant_core.ingestion.bronze import build_replace_where
from quant_core.transforms.dimensions.portfolio import DIM_PORTFOLIO_SPEC


def test_irr_returns_none_for_same_sign_cashflows():
    assert irr([100.0, 200.0, 300.0]) is None


def test_irr_returns_value_for_basic_series():
    result = irr([-1000.0, 600.0, 600.0])
    assert result is not None
    assert result > 0


def test_xirr_returns_none_for_same_sign_cashflows():
    cashflows = [
        {"cashflow_dt": "2026-01-01", "cashflow_amount": 1000.0},
        {"cashflow_dt": "2026-02-01", "cashflow_amount": 250.0},
    ]
    assert xirr(cashflows) is None


def test_build_replace_where_orders_values():
    condition = build_replace_where("trade_yyyymm", ["202602", "202601"])
    assert condition == "trade_yyyymm IN ('202601','202602')"


def test_repo_dimension_spec_exposes_business_key():
    assert DIM_PORTFOLIO_SPEC.business_key == "portfolio_id"
    assert DIM_PORTFOLIO_SPEC.scd2_enabled is True


def test_domain_replace_condition_helpers_delegate_correctly():
    assert pnl_replace_condition("202601") == "position_yyyymm IN ('202601')"
    assert cagr_replace_condition("202601") == "result_yyyymm IN ('202601')"
