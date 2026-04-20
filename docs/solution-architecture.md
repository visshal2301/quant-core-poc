# Quant Core POC Solution Architecture

## 1. Objective

Build an end-to-end Databricks proof of concept for enterprise-style financial calculations using Medallion architecture. The platform will ingest generated source files from Databricks Volumes, curate and enrich the data through Bronze and Silver, and perform calculations in Gold for:
- `VaR`
- `SVaR`
- `IRR`
- `XIRR`
- `CAGR`

The POC is notebook-first, but structured so it can later be refactored into a production-grade code repository.
The design is upgraded to a bi-temporal model so that both business validity and system history are preserved.

## 2. Scope and assumptions

- Scope: data platform POC with analytics-ready financial calculations
- Asset coverage: mixed portfolios
- Source systems: generated mock files only
- Fact grain: daily plus transaction-level detail
- Risk method: historical simulation
- Gold focus: both risk analytics and investment performance analytics
- Deployment style: start in notebooks, then refactor to enterprise code modules
- Temporal model: bi-temporal across curated facts and dimensions

## 3. Target architecture

### Logical layers

1. Source simulation layer
- Python-based generators create CSV files for facts and dimensions.
- Files are written to Databricks Volumes under a controlled folder layout.
- Fact datasets are organized as `dataset/YYYYMM/<dataset>_YYYYMMDD.csv`.
- Dimension snapshots are organized as `dimensions/YYYYMM/<dimension>.csv`.

2. Bronze layer
- Raw files are ingested with minimal transformations.
- Schema capture, ingestion timestamps, source file names, load IDs, and raw lineage are preserved.
- Bronze tables are append-only and auditable.
- Bronze establishes `system time` entry points through ingestion metadata.

3. Silver layer
- Standardize data types, date handling, null rules, and business keys.
- Create conformed dimensions and curated fact tables.
- Apply validations, deduplication, survivorship rules, and business enrichment.
- Stamp each curated record with `valid time` and `system time` windows.
- Preserve prior versions for corrections and restatements instead of overwriting history.

4. Gold layer
- Publish finance-ready marts and calculation outputs.
- Separate subject areas:
  - risk analytics
  - performance analytics
- Expose final Delta tables and SQL views for dashboards and downstream consumption.
- Support point-in-time analytical queries such as:
  - what was true on a business date
  - what the platform knew on a reporting date
  - what was known then about a past business date

## 3A. Bi-temporal design principles

Each curated record carries two timelines:

1. Valid time
- when the business fact or attribute is true in the domain
- example: an instrument classification becomes effective on a certain date

2. System time
- when the platform first stored or corrected that version
- example: a late file loaded on April 15 states that a business change actually became valid on April 1

Standard temporal columns:
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

Temporal query examples:
- business as-of: filter where `report_ts` falls between `valid_from_ts` and `valid_to_ts`
- system as-known: filter where `report_ts` falls between `system_from_ts` and `system_to_ts`
- full bi-temporal: apply both filters together

## 4. Medallion design

### Bronze

Raw Delta tables:
- `bronze.transactions_raw`
- `bronze.positions_daily_raw`
- `bronze.market_prices_daily_raw`
- `bronze.cashflows_raw`
- `bronze.portfolios_raw`
- `bronze.instruments_raw`
- `bronze.counterparties_raw`
- `bronze.currencies_raw`

Mandatory Bronze control columns:
- `ingestion_ts`
- `load_id`
- `source_file_name`
- `source_file_path`
- `record_hash`
- `is_corrupt_record`
- `system_from_ts`
- `system_to_ts`
- `record_version`
- `change_type`

### Silver

Curated dimensions:
- `silver.dim_portfolio`
- `silver.dim_instrument`
- `silver.dim_counterparty`
- `silver.dim_date`
- `silver.dim_currency`
- `silver.dim_asset_class`
- `silver.dim_market_data_source`

Curated facts:
- `silver.fact_transactions`
- `silver.fact_positions_daily`
- `silver.fact_market_prices_daily`
- `silver.fact_cashflows`

Silver enrichment examples:
- derive holding currency and reporting currency
- classify instrument to asset class
- calculate market value per position
- align transaction and cashflow records to conformed dimensions
- enforce date and business key standards
- persist prior versions for corrected records
- allow late-arriving and backdated effective changes

### Gold

Risk marts:
- `gold.risk_var_results`
- `gold.risk_svar_results`
- `gold.risk_pnl_distribution`

Performance marts:
- `gold.performance_irr_results`
- `gold.performance_xirr_results`
- `gold.performance_cagr_results`

Common access views:
- `gold.vw_portfolio_risk_summary`
- `gold.vw_portfolio_performance_summary`
- `gold.vw_portfolio_risk_asof`
- `gold.vw_portfolio_performance_asof`

## 5. Data model

### Dimension model

#### `dim_portfolio`
- `portfolio_sk`
- `portfolio_id`
- `portfolio_name`
- `portfolio_type`
- `base_currency_code`
- `risk_policy_name`
- `effective_from_dt`
- `effective_to_dt`
- `is_current`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

#### `dim_instrument`
- `instrument_sk`
- `instrument_id`
- `instrument_name`
- `ticker`
- `isin`
- `asset_class_code`
- `currency_code`
- `issuer_name`
- `coupon_rate`
- `maturity_dt`
- `effective_from_dt`
- `effective_to_dt`
- `is_current`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

#### `dim_counterparty`
- `counterparty_sk`
- `counterparty_id`
- `counterparty_name`
- `counterparty_type`
- `country_code`
- `credit_rating`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

#### `dim_currency`
- `currency_sk`
- `currency_code`
- `currency_name`
- `decimal_precision`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`

#### `dim_asset_class`
- `asset_class_sk`
- `asset_class_code`
- `asset_class_name`
- `risk_bucket`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`

#### `dim_market_data_source`
- `market_data_source_sk`
- `source_system_code`
- `source_system_name`
- `vendor_name`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`

#### `dim_date`
- `date_sk`
- `calendar_dt`
- `year_num`
- `quarter_num`
- `month_num`
- `day_num`
- `is_month_end`
- `is_business_day`

### Fact model

#### `fact_transactions`
- `transaction_id`
- `portfolio_sk`
- `instrument_sk`
- `counterparty_sk`
- `trade_dt`
- `settlement_dt`
- `transaction_type`
- `quantity`
- `price`
- `gross_amount`
- `fees_amount`
- `net_amount`
- `currency_sk`
- `load_id`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

#### `fact_positions_daily`
- `position_id`
- `portfolio_sk`
- `instrument_sk`
- `position_dt`
- `quantity`
- `end_of_day_price`
- `market_value`
- `unrealized_pnl`
- `currency_sk`
- `market_data_source_sk`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

#### `fact_market_prices_daily`
- `price_id`
- `instrument_sk`
- `price_dt`
- `close_price`
- `return_pct`
- `volatility_proxy`
- `currency_sk`
- `market_data_source_sk`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

#### `fact_cashflows`
- `cashflow_id`
- `portfolio_sk`
- `instrument_sk`
- `cashflow_dt`
- `cashflow_type`
- `cashflow_amount`
- `currency_sk`
- `valid_from_ts`
- `valid_to_ts`
- `system_from_ts`
- `system_to_ts`
- `is_current_valid`
- `is_current_system`
- `record_version`
- `change_type`

## 6. Calculation design

### `VaR`

Method:
- historical simulation
- portfolio-level daily PnL distribution from historical market returns
- confidence levels: `95%` and `99%`
- horizon: `1 day`

High-level logic:
1. derive historical returns by instrument
2. map positions to return series
3. calculate simulated portfolio PnL for each historical day
4. sort PnL distribution
5. take percentile loss threshold as VaR

Output columns:
- `portfolio_id`
- `as_of_date`
- `business_as_of_ts`
- `system_as_of_ts`
- `confidence_level`
- `holding_period_days`
- `simulations_count`
- `var_amount`
- `var_pct_market_value`

### `SVaR`

Method:
- same as historical simulation VaR
- restricted to a stressed historical window in sample data

High-level logic:
1. identify stressed date window
2. run the same PnL simulation only on the stressed subset
3. compute stressed percentile loss

Output columns:
- `portfolio_id`
- `as_of_date`
- `business_as_of_ts`
- `system_as_of_ts`
- `stress_window_start_dt`
- `stress_window_end_dt`
- `confidence_level`
- `svar_amount`
- `svar_pct_market_value`

### `IRR`

Method:
- regular-period internal rate of return for evenly spaced cashflows

Use case:
- useful for synthetic investment schedules where period spacing is consistent

### `XIRR`

Method:
- annualized internal rate of return using dated, irregular cashflows
- recommended for realistic investment portfolio cashflows

### `CAGR`

Method:
- annualized growth between start and end valuation

Formula:
- `CAGR = (Ending Value / Beginning Value) ^ (1 / Years) - 1`

## 7. Sample data design

### Generated entities

Dimensions:
- portfolios
- instruments
- counterparties
- currencies
- asset classes
- market data sources

Facts:
- daily prices for 3 years
- daily positions snapshots
- transactions for 1 year
- irregular cashflows for return calculations

### Synthetic stress scenario

The generator will introduce a stressed period to support `SVaR`:
- abrupt price shocks
- increased negative volatility clustering
- cross-asset drawdown behavior

This gives the POC a controlled but realistic historical stress window.

## 8. Enterprise controls

### Data governance

- consistent catalog and schema naming
- Delta tables with audit columns
- business key and surrogate key discipline
- lineage from volume file to gold output
- bi-temporal history retention for audit and replay

### Data quality

- schema validation
- null threshold checks
- duplicate transaction detection
- invalid date and currency checks
- rejected-record handling

### Operational controls

- parameterized execution by run date and environment
- run-level `load_id`
- logging and exception handling
- rerunnable notebook design
- partition strategy for daily data
- correction handling without destructive overwrites
- temporal point-in-time reproducibility

### Security design for future state

- Unity Catalog-managed access
- role-based schema access
- volume ACLs
- separation between raw, curated, and published zones

## 9. Folder structure

```text
quant-core-poc/
  config/
    quant_core_config.yaml
  docs/
    solution-architecture.md
    presentation-outline.md
  notebooks/
    00_setup/
      00_environment_setup.py
    01_ingest_bronze/
      01_bronze_ingestion.py
    02_transform_silver/
      02_silver_transformation.py
    03_publish_gold/
      03_gold_calculations.py
  scripts/
    generate_mock_data.py
  sql/
    gold_views.sql
```

## 10. Recommended Databricks object layout

- Catalog: `quant_core`
- Schemas:
  - `landing`
  - `bronze`
  - `silver`
  - `gold`
  - `audit`
- Volume:
  - `/Volumes/quant_core/landing/mock_data/`

Example landing folders:
- `/Volumes/quant_core/landing/mock_data/transactions/202604/`
- `/Volumes/quant_core/landing/mock_data/positions_daily/202604/`
- `/Volumes/quant_core/landing/mock_data/market_prices_daily/202604/`
- `/Volumes/quant_core/landing/mock_data/cashflows/202604/`
- `/Volumes/quant_core/landing/mock_data/dimensions/202604/`

## 11. Execution flow

1. Provision base schemas and volume paths.
2. Run data generator for a target `YYYYMM` to create daily files under the month folder.
3. Ingest raw files for the same `YYYYMM` into Bronze with audit metadata.
4. Transform Bronze to Silver dimensions and facts.
5. Publish Gold risk and performance outputs.
6. Create SQL views for reporting.
7. Demo outputs through Databricks SQL or notebook visualizations.

## 11A. Point-in-time query pattern

Example bi-temporal filter:

```sql
SELECT *
FROM quant_core.silver.fact_positions_daily
WHERE TIMESTAMP '2026-03-31 00:00:00' >= valid_from_ts
  AND TIMESTAMP '2026-03-31 00:00:00' < valid_to_ts
  AND TIMESTAMP '2026-04-05 12:00:00' >= system_from_ts
  AND TIMESTAMP '2026-04-05 12:00:00' < system_to_ts;
```

This answers:
- what positions were valid for March 31, 2026
- based on what the platform knew on April 5, 2026

## 12. Notebook-first to enterprise roadmap

### Phase 1: POC
- Databricks notebooks
- mock data generator
- manual execution order
- Delta outputs and simple SQL views

### Phase 2: Enterprise hardening
- move business logic into reusable Python packages
- orchestrate with Databricks Workflows
- add unit and integration tests
- add CI/CD and environment promotion
- implement DLT or Lakeflow where beneficial
- add monitoring and alerting

## 13. Suggested demo narrative

1. Show raw generated files landing in Volumes.
2. Show Bronze tables with file lineage and ingestion metadata.
3. Show Silver conformed facts and dimensions.
4. Show Gold portfolio risk summary and return summary.
5. Walk through `VaR`, `SVaR`, `IRR`, `XIRR`, and `CAGR` outputs for a selected portfolio.
6. Show a late correction and rerun a point-in-time query to demonstrate bi-temporal auditability.
