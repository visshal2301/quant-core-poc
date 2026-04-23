"""Transaction fact spec."""

from quant_core.models import FactSpec


FACT_TRANSACTIONS_SPEC = FactSpec(
    table_name="fact_transactions",
    event_date_column="trade_dt",
    partition_column="trade_yyyymm",
    natural_keys=["portfolio_id", "instrument_id", "counterparty_id", "currency_code"],
    numeric_columns=["quantity", "price", "gross_amount", "fees_amount", "net_amount"],
    reference_keys=["portfolio_sk", "instrument_sk", "counterparty_sk", "currency_sk"],
    extra_columns=["transaction_id", "settlement_dt", "transaction_type", "load_id", "source_yyyymm"],
)

