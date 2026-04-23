# Quant Core Landing Pattern

## Purpose

This note explains how landing data is organized in the Databricks Volume so the mock data generator, Bronze ingestion, and downstream fact tables stay aligned.

## Proposed landing structure

```text
/Volumes/quant_core/landing/mock_data/
  dimensions/
    202604/
      portfolios.csv
      instruments.csv
      counterparties.csv
      currencies.csv
      asset_classes.csv
      market_data_sources.csv
  transactions/
    202604/
      transactions_20260401.csv
      transactions_20260403.csv
      ...
  positions_daily/
    202604/
      positions_daily_20260401.csv
      positions_daily_20260402.csv
      ...
  market_prices_daily/
    202604/
      market_prices_daily_20260401.csv
      market_prices_daily_20260402.csv
      ...
  cashflows/
    202604/
      cashflows_20260401.csv
      cashflows_20260415.csv
      ...
```

## Why this is useful

- easy to run a single monthly backfill
- simple to pass one `YYYYMM` parameter through generator and Bronze
- daily fact files still preserve business granularity
- month folders are easy to audit and demo

## POC behavior

The generator accepts `--yyyymm`.

Example:

```bash
python scripts/generate_mock_data.py --yyyymm 202604
```

Default POC behavior:
- create landing folders under the requested `YYYYMM`
- generate daily fact files inside that month folder
- if the month folder already exists, delete it and recreate it

This is good for a deterministic POC because it lets us rerun the same month and avoid stale mixed data.

The same `YYYYMM` should be passed through:
- mock data generation
- Bronze ingestion
- Silver transformation
- Gold publishing

This keeps scheduled workflows deterministic and makes monthly backfills easier to operate.

## Enterprise recommendation

The `YYYYMM` folder pattern is a good idea.

The delete-and-recreate behavior is usually not how raw enterprise landing is handled in production.

Enterprise patterns are usually one of these:

1. Append-only raw landing
- keep every delivered file
- use dataset and delivery timestamps or run IDs
- never delete raw arrivals automatically

2. Replay-friendly monthly partitioning
- keep `dataset/YYYYMM/`
- write each load into a unique subfolder such as `run_id=20260420_083000`
- promote only validated data downstream

3. Controlled reprocessing zone
- allow overwrite in a dedicated non-authoritative sandbox or POC zone
- keep authoritative raw landing immutable

## Recommended compromise for Quant Core

For this POC:
- keep `dataset/YYYYMM/`
- allow overwrite of the target month from the generator
- parameterize Bronze ingestion with `target_yyyymm`

For enterprise hardening later:
- change the generator to write under `dataset/YYYYMM/run_id=<timestamp>/`
- ingest using Auto Loader or parameterized batch reads
- retain prior files for audit and replay

## Relation to Silver and Gold

This landing pattern does not change the logical fact-table grain:
- `fact_transactions` remains transaction-level
- `fact_positions_daily` remains daily snapshot level
- `fact_market_prices_daily` remains daily price level
- `fact_cashflows` remains dated cashflow level

The landing folder only organizes delivery and ingestion. The Silver and Gold data model stays business-oriented.
