# Quant Core Databricks POC

`Quant Core` is a notebook-first Databricks proof of concept for enterprise-grade financial analytics across risk and return calculations.

The POC uses:
- Medallion architecture (`bronze`, `silver`, `gold`)
- bi-temporal schema design (`valid time` and `system time`)
- `SCD Type 2` for core dimensions
- schema-drift-aware Bronze ingestion
- Databricks Volumes for mock source files
- monthly landing folders using `YYYYMM` plus daily fact files
- Delta tables for curated layers
- Mixed portfolio sample data
- Historical simulation for `VaR` and `SVaR`
- Gold-layer calculations for `VaR`, `SVaR`, `IRR`, `XIRR`, and `CAGR`

## Deliverables

- Architecture and solution design: [docs/solution-architecture.md]
- Presentation-ready summary: [docs/presentation-outline.md]
- Runtime configuration: [config/quant_core_config.yaml]
- Mock data generator: [scripts/generate_mock_data.py]
- Landing-folder and enterprise-pattern note: [docs/landing-pattern.md]
- Databricks notebooks:
  - [notebooks/00_setup/00_environment_setup.py]
  - [notebooks/01_ingest_bronze/01_bronze_ingestion.py]
  - [notebooks/02_transform_silver/02_silver_transformation.py]
  - [notebooks/03_publish_gold/03_gold_calculations.py])

## Proposed execution flow

1. Run environment setup notebook.
2. Generate mock source files for a target `YYYYMM` in Databricks Volume paths.
3. Run Bronze ingestion notebook with the same `target_yyyymm` to land one source month into Delta Bronze tables with ingestion-time lineage.
4. Run Silver transformation notebook with the same `target_yyyymm` to standardize, validate, enrich, and stamp bi-temporal validity windows on facts and dimensions.
5. Run Gold calculations notebook with the same `target_yyyymm` to publish risk and performance marts plus point-in-time query views.

## Bi-temporal design

Each curated record is modeled with two timelines:
- `valid_from_ts` and `valid_to_ts`: when the record is true in the business domain
- `system_from_ts` and `system_to_ts`: when the platform knew and stored that version

This enables:
- backdated corrections
- late-arriving changes
- as-of reporting for business time
- as-known reporting for audit and traceability

## History and drift handling

- Bronze accepts monthly source files with light schema drift handling and writes the latest discovered schema into Delta.
- Silver implements `SCD2` for `dim_portfolio`, `dim_instrument`, and `dim_counterparty`.
- Static reference dimensions are still rebuilt as curated snapshots in this POC.

## Enterprise refactor path

This starter is notebook-first by design. The next step is to move reusable logic from notebooks into a repo structure:
- `src/quant_core/ingestion`
- `src/quant_core/transforms`
- `src/quant_core/calculations`
- `tests/unit`
- `tests/integration`
- CI/CD deployment assets
