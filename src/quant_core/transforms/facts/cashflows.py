"""Cashflow fact spec."""

from quant_core.models import FactSpec


FACT_CASHFLOWS_SPEC = FactSpec(
    table_name="fact_cashflows",
    event_date_column="cashflow_dt",
    partition_column="cashflow_yyyymm",
    natural_keys=["portfolio_id", "instrument_id", "currency_code"],
    numeric_columns=["cashflow_amount"],
    reference_keys=["portfolio_sk", "instrument_sk", "currency_sk"],
    extra_columns=["cashflow_id", "cashflow_type", "source_yyyymm"],
)

