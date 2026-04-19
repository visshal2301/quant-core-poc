# Quant Core POC Presentation Outline

## Slide 1: Executive summary
- Databricks-based financial calculation platform POC
- Enterprise Medallion architecture
- Bi-temporal schema design for auditability and corrections
- End-to-end flow from generated source files to Gold calculations
- In-scope analytics: `VaR`, `SVaR`, `IRR`, `XIRR`, `CAGR`

## Slide 2: Business problem
- Risk and performance calculations often live in disconnected tools
- Lack of governed data lineage creates audit and trust issues
- The POC shows a scalable path from raw market and transaction data to analytics-ready outputs

## Slide 3: Architecture
- Source file generation in Volumes
- Bronze raw ingestion
- Silver standardization and enrichment
- Gold risk and performance marts
- valid time and system time preserved across curated data

## Slide 4: Why bi-temporal
- answers both business truth and platform knowledge questions
- supports late-arriving corrections without losing prior history
- improves audit readiness for financial reporting

## Slide 5: Data model
- conformed dimensions for portfolio, instrument, counterparty, date, currency, and asset class
- daily and transaction-level facts supporting both risk and returns
- temporal attributes on curated facts and dimensions

## Slide 6: Key calculations
- `VaR`: historical simulation at `95%` and `99%`
- `SVaR`: stressed historical window
- `IRR`: regular period returns
- `XIRR`: irregular dated cashflows
- `CAGR`: annualized growth

## Slide 7: Enterprise controls
- audit columns
- lineage
- validation
- rerunnable loads
- bi-temporal history retention
- future CI/CD and RBAC path

## Slide 8: Demo flow
- generate files
- ingest to Bronze
- curate to Silver
- calculate Gold metrics
- show summary views
- demonstrate as-of queries after a correction

## Slide 9: Next steps
- refactor notebook logic into Python modules
- add workflow orchestration
- add tests and monitoring
- onboard real upstream datasets
