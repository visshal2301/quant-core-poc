# Quant Core Databricks POC

`Quant Core` is a notebook-first Databricks proof of concept for enterprise-grade financial analytics across risk and return calculations.

The POC uses:
- Medallion architecture (`bronze`, `silver`, `gold`)
- bi-temporal schema design (`valid time` and `system time`)
- Databricks Volumes for mock source files
- Delta tables for curated layers
- Mixed portfolio sample data
- Historical simulation for `VaR` and `SVaR`
- Gold-layer calculations for `VaR`, `SVaR`, `IRR`, `XIRR`, and `CAGR`

## Deliverables

- Architecture and solution design: [docs/solution-architecture.md](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\docs\solution-architecture.md)
- Presentation-ready summary: [docs/presentation-outline.md](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\docs\presentation-outline.md)
- Runtime configuration: [config/quant_core_config.yaml](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\config\quant_core_config.yaml)
- Mock data generator: [scripts/generate_mock_data.py](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\scripts\generate_mock_data.py)
- Databricks notebooks:
  - [notebooks/00_setup/00_environment_setup.py](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\notebooks\00_setup\00_environment_setup.py)
  - [notebooks/01_ingest_bronze/01_bronze_ingestion.py](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\notebooks\01_ingest_bronze\01_bronze_ingestion.py)
  - [notebooks/02_transform_silver/02_silver_transformation.py](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\notebooks\02_transform_silver\02_silver_transformation.py)
  - [notebooks/03_publish_gold/03_gold_calculations.py](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\notebooks\03_publish_gold\03_gold_calculations.py)

## Proposed execution flow

1. Run environment setup notebook.
2. Upload or generate mock source files in Databricks Volume paths.
3. Run Bronze ingestion notebook to land raw files into Delta Bronze tables with ingestion-time lineage.
4. Run Silver transformation notebook to standardize, validate, enrich, and stamp bi-temporal validity windows on facts and dimensions.
5. Run Gold calculations notebook to publish risk and performance marts plus point-in-time query views.

## Bi-temporal design

Each curated record is modeled with two timelines:
- `valid_from_ts` and `valid_to_ts`: when the record is true in the business domain
- `system_from_ts` and `system_to_ts`: when the platform knew and stored that version

This enables:
- backdated corrections
- late-arriving changes
- as-of reporting for business time
- as-known reporting for audit and traceability

## Enterprise refactor path

This starter is notebook-first by design. The next step is to move reusable logic from notebooks into a repo structure:
- `src/quant_core/ingestion`
- `src/quant_core/transforms`
- `src/quant_core/calculations`
- `tests/unit`
- `tests/integration`
- CI/CD deployment assets
