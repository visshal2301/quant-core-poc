# Quant Core Scheduling and CI/CD

## Purpose

This note explains how the refactored repo can be scheduled in Databricks and validated through CI/CD.

## Scheduling approach

The repo now supports a month-parameterized workflow based on `target_yyyymm`.

The same parameter is passed through:
- Bronze ingestion
- Silver transformation
- Gold calculations

This keeps reruns and monthly backfills deterministic.

## Databricks workflow asset

The project includes a Databricks Asset Bundle:
- [databricks.yml](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\databricks.yml)
- [resources/quant_core_job.yml](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\resources\quant_core_job.yml)

The workflow runs:
1. environment setup
2. Bronze ingestion
3. Silver transformation
4. Gold calculations

## Example scheduling pattern

For production-style execution, schedule the workflow monthly and pass the processing month explicitly.

Recommended pattern:
- run near the start of the next month
- pass the completed business month as `target_yyyymm`

Example:
- run on February 1, 2026
- process `202601`

## CI/CD approach

The repo includes a GitHub Actions workflow:
- [.github/workflows/ci.yml](C:\Users\DELL\Documents\Codex\2026-04-19-i-am-createing-a-poc-on\quant-core-poc\.github\workflows\ci.yml)

Current CI steps:
- install package and dev dependencies
- compile Python files
- run unit and integration tests

## Recommended promotion path

Suggested environments:
- `dev`
- `uat`
- `prod`

Suggested release flow:
1. developer opens PR
2. CI runs compile and tests
3. reviewer approves
4. bundle is deployed to target Databricks workspace
5. workflow is triggered with the intended `target_yyyymm`

## Next hardening steps

- add separate deployment workflow for Databricks bundle promotion
- add secrets management for workspace authentication
- add environment-specific variables for catalog/schema/workspace paths
- add test coverage for runtime modules
- add data-quality and audit checks as pipeline gates

