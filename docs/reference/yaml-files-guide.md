# YAML Files Guide - Quant Core POC

## Overview

The Quant Core POC uses **4 YAML files** organized across different purposes:

1. **`databricks.yml`** - Main Databricks Asset Bundle (DAB) configuration
2. **`resources/quant_core_job.yml`** - Job workflow definition
3. **`config/quant_core_config.yaml`** - Runtime application configuration
4. **`.github/workflows/ci.yml`** - GitHub Actions CI/CD pipeline

---

## 1. databricks.yml

**Location:** `/quant-core-poc/databricks.yml`  
**Purpose:** Root configuration for Databricks Asset Bundles (DABs)

### What It Does

This is the **entry point** for deploying the entire project to Databricks. It defines:
* Bundle name and identity
* Environment-specific deployment targets (dev, prod)
* Global variables shared across resources
* Resource includes (jobs, pipelines, etc.)

### File Structure

```yaml
bundle:
  name: quant-core-poc                  # Bundle identifier

include:
  - resources/*.yml                     # Auto-includes all resource definitions

variables:
  target_yyyymm:                        # Global parameter for month processing
    description: Month to process in YYYYMM format
    default: "202601"
  catalog_name:                         # Unity Catalog name
    description: Unity Catalog catalog name
    default: "quant_core"

targets:
  dev:                                  # Development environment
    default: true
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

  prod:                                 # Production environment
    workspace:
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
```

### Key Concepts

#### Bundle
* **Name:** Unique identifier for the project
* **Include:** Automatically loads all `.yml` files from `resources/` folder
* **Variables:** Shared parameters accessible in all included resources via `${var.variable_name}`

#### Variables
* **`target_yyyymm`**: Controls which month to process (e.g., "202601")
  * Referenced in jobs: `${var.target_yyyymm}`
  * Can be overridden at deployment time
* **`catalog_name`**: Unity Catalog for data storage
  * Used for table references in notebooks

#### Targets (Environments)
* **`dev`**: Development environment
  * Default target (set with `default: true`)
  * Deploys to user's personal workspace
  * Path includes username for isolation
* **`prod`**: Production environment
  * Deploys to shared workspace
  * Used for production runs

### Usage Commands

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to dev (default target)
databricks bundle deploy

# Deploy to prod
databricks bundle deploy --target prod

# Run the job in dev
databricks bundle run quant_core_monthly_pipeline

# Deploy with custom variable
databricks bundle deploy --var="target_yyyymm=202602"
```

### Variable Interpolation

Throughout the bundle, you can use:
* `${bundle.name}` → "quant-core-poc"
* `${bundle.target}` → "dev" or "prod"
* `${var.target_yyyymm}` → "202601" (or custom value)
* `${workspace.current_user.userName}` → Current user's email

---

## 2. resources/quant_core_job.yml

**Location:** `/quant-core-poc/resources/quant_core_job.yml`  
**Purpose:** Defines the ETL workflow job structure

### What It Does

Defines the **monthly pipeline job** as a multi-task DAG (Directed Acyclic Graph):
* Task dependencies (Bronze → Silver → Gold)
* Notebook paths and parameters
* Execution order and parallelism

### File Structure

```yaml
resources:
  jobs:
    quant_core_monthly_pipeline:                    # Job identifier
      name: quant_core_monthly_pipeline_${bundle.target}  # Job name in UI
      max_concurrent_runs: 1                        # Prevent parallel execution
      
      tasks:
        - task_key: environment_setup               # Task 1: Setup
          notebook_task:
            notebook_path: ./notebooks/00_setup/00_environment_setup.py

        - task_key: bronze_ingestion                # Task 2: Bronze layer
          depends_on:
            - task_key: environment_setup
          notebook_task:
            notebook_path: ./notebooks/01_ingest_bronze/01_bronze_ingestion.py
            base_parameters:
              target_yyyymm: ${var.target_yyyymm}   # Pass month parameter

        - task_key: silver_transformation           # Task 3: Silver layer
          depends_on:
            - task_key: bronze_ingestion
          notebook_task:
            notebook_path: ./notebooks/02_transform_silver/02_silver_transformation.py
            base_parameters:
              target_yyyymm: ${var.target_yyyymm}

        - task_key: gold_calculations               # Task 4: Gold layer
          depends_on:
            - task_key: silver_transformation
          notebook_task:
            notebook_path: ./notebooks/03_publish_gold/03_gold_calculations.py
            base_parameters:
              target_yyyymm: ${var.target_yyyymm}

      parameters:                                   # Job-level parameters
        - name: target_yyyymm
          default: ${var.target_yyyymm}
```

### Execution Flow

```
environment_setup
       ↓
bronze_ingestion (target_yyyymm)
       ↓
silver_transformation (target_yyyymm)
       ↓
gold_calculations (target_yyyymm)
```

### Key Features

#### Task Dependencies
* **`depends_on`**: Ensures tasks run in correct order
* **Serial execution**: Each task waits for its predecessor
* **Automatic failure handling**: If one task fails, downstream tasks are skipped

#### Parameters
* **`base_parameters`**: Notebook widgets/parameters
* **`target_yyyymm`**: Passed to Bronze, Silver, and Gold notebooks
* **Not passed to setup**: Setup is environment-agnostic

#### Concurrent Runs
* **`max_concurrent_runs: 1`**: Prevents data race conditions
* **Why?**: Silver layer uses partition replacement—concurrent runs could corrupt data
* **Production consideration**: Keep this setting for data integrity

### Notebook Path Resolution

Paths are **relative to bundle root**:
* `./notebooks/00_setup/00_environment_setup.py`
* Deployed notebooks accessible at runtime
* Bundle deployment syncs notebooks to workspace

### Usage

```bash
# Run the entire pipeline
databricks bundle run quant_core_monthly_pipeline

# Run with custom month
databricks bundle run quant_core_monthly_pipeline --params target_yyyymm=202603

# Monitor job execution
databricks jobs get-run <run_id>

# View job history
databricks jobs list-runs --job-id <job_id>
```

### Production Enhancements (Future)

Consider adding:

```yaml
# Cluster configuration
new_cluster:
  spark_version: "14.3.x-scala2.12"
  node_type_id: "i3.xlarge"
  num_workers: 2
  
# Email notifications
email_notifications:
  on_failure:
    - data-team@company.com
  on_success:
    - data-team@company.com

# Timeout settings
timeout_seconds: 7200  # 2 hours

# Retry configuration
max_retries: 2
retry_on_timeout: true
```

---

## 3. config/quant_core_config.yaml

**Location:** `/quant-core-poc/config/quant_core_config.yaml`  
**Purpose:** Application-level runtime configuration

### What It Does

Stores **business logic parameters** and **data generation settings**:
* Schema and catalog names
* Volume paths and landing patterns
* Risk calculation parameters (VaR confidence levels, stress windows)
* Bi-temporal strategy configuration
* Mock data generation settings

### File Structure

```yaml
# Project identity
project_name: quant_core
catalog: quant_core

# Schema names
schemas:
  landing: landing
  bronze: bronze
  silver: silver
  gold: gold
  audit: audit

# Data storage paths
volume_base_path: /Volumes/quant_core/landing/mock_data
paths:
  dimensions: /Volumes/quant_core/landing/mock_data/dimensions/{yyyymm}
  transactions: /Volumes/quant_core/landing/mock_data/transactions/{yyyymm}
  positions_daily: /Volumes/quant_core/landing/mock_data/positions_daily/{yyyymm}
  market_prices_daily: /Volumes/quant_core/landing/mock_data/market_prices_daily/{yyyymm}
  cashflows: /Volumes/quant_core/landing/mock_data/cashflows/{yyyymm}

# Landing zone patterns
landing:
  folder_pattern: dataset/YYYYMM
  fact_file_pattern: "<dataset>_YYYYMMDD.csv"
  dimension_snapshot_pattern: dimensions/YYYYMM/<table>.csv
  overwrite_existing_month_for_poc: true

# Runtime parameters
runtime:
  lookback_years_prices: 3                # Historical price data to load
  lookback_years_transactions: 1           # Transaction history to load
  open_ended_timestamp: 9999-12-31 23:59:59  # Bi-temporal open-ended marker
  
  # Risk calculation parameters
  var_confidence_levels:
    - 0.95                                 # 95% VaR
    - 0.99                                 # 99% VaR
  var_horizon_days: 1                      # 1-day VaR
  stress_window_start: 2024-09-01          # SVaR stress period start
  stress_window_end: 2024-11-30            # SVaR stress period end

# Bi-temporal strategy
temporal:
  valid_time_strategy:
    transactions: trade_dt_to_open_end
    positions_daily: position_dt_day_window
    market_prices_daily: price_dt_day_window
    cashflows: cashflow_dt_to_open_end
    dimensions: effective_start_to_open_end
  
  system_time_strategy:
    source: ingestion_timestamp
    close_previous_version_on_change: true
  
  query_defaults:
    use_current_valid_only: true
    use_current_system_only: true

# Mock data generation settings
dimensions:
  portfolios: 10                           # Number of portfolios
  instruments: 50                          # Number of instruments
  counterparties: 12                       # Number of counterparties
  currencies:
    - USD
    - EUR
    - GBP
    - INR
  asset_classes:
    - EQUITY
    - BOND
    - ETF
    - MUTUAL_FUND
    - DERIVATIVE
```

### Usage in Code

**Load configuration:**

```python
import yaml

with open("config/quant_core_config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Access values
catalog = config["catalog"]
var_levels = config["runtime"]["var_confidence_levels"]
stress_start = config["runtime"]["stress_window_start"]
```

**Using in notebooks:**

```python
# Example: Gold calculations notebook
CONFIDENCE_LEVELS = [0.95, 0.99]  # From config
STRESS_WINDOW_START = "2024-09-01"  # From config
STRESS_WINDOW_END = "2024-11-30"    # From config
```

### Key Sections Explained

#### Paths
* **`{yyyymm}`**: Placeholder replaced at runtime
* **Volumes**: Databricks Volumes for file storage
* **Partitioned by month**: `transactions/202601/`, `transactions/202602/`

#### Runtime Parameters
* **VaR confidence levels**: `[0.95, 0.99]`
  * Calculates both 95% and 99% VaR
  * Used in `gold_calculations` notebook
* **Stress window**: Crisis period for SVaR calculation
  * Sep-Nov 2024 in this example
  * Filters market prices to stress period only

#### Temporal Strategies
* **Valid time strategy**: How to set `valid_from_ts` and `valid_to_ts`
  * `trade_dt_to_open_end`: Transaction valid from trade date forever
  * `position_dt_day_window`: Position valid for that day only
* **System time strategy**: How to track data versions
  * `ingestion_timestamp`: Use when data was loaded
  * `close_previous_version_on_change`: Update old version's `system_to_ts`

#### Mock Data Settings
* **Dimensions counts**: Controls mock data generation
  * 10 portfolios, 50 instruments, etc.
* **Used by**: `scripts/generate_mock_data.py`

---

## 4. .github/workflows/ci.yml

**Location:** `/quant-core-poc/.github/workflows/ci.yml`  
**Purpose:** GitHub Actions CI/CD pipeline

### What It Does

Automates **testing and validation** when code is pushed to GitHub:
* Runs on every push to `main`/`master` or pull request
* Sets up Python environment
* Installs dependencies
* Compiles Python files (syntax check)
* Runs unit and integration tests

### File Structure

```yaml
name: CI                                 # Workflow name

on:
  push:
    branches: ["main", "master"]         # Trigger on push to main
  pull_request:                          # Trigger on any PR

jobs:
  test:
    runs-on: ubuntu-latest               # Use Ubuntu runner
    
    steps:
      - name: Checkout                   # Step 1: Get code
        uses: actions/checkout@v4
      
      - name: Set up Python              # Step 2: Install Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      
      - name: Install dependencies       # Step 3: Install packages
        run: |
          python -m pip install --upgrade pip
          pip install -e .[dev]
      
      - name: Compile Python files       # Step 4: Syntax check
        run: |
          python -m py_compile scripts/generate_mock_data.py
          python -m compileall src
      
      - name: Run tests                  # Step 5: Execute tests
        run: |
          pytest tests/unit tests/integration
```

### Execution Flow

```
1. Code pushed to GitHub
   ↓
2. GitHub Actions triggered
   ↓
3. Checkout code
   ↓
4. Install Python 3.11
   ↓
5. Install dependencies (pip install -e .[dev])
   ↓
6. Compile Python files (syntax check)
   ↓
7. Run pytest (unit + integration tests)
   ↓
8. Report success/failure
```

### Key Features

#### Triggers
* **Push to main/master**: Validates production branches
* **Pull requests**: Runs on all PRs before merge
* **Automatic**: No manual intervention needed

#### Python Setup
* **Version**: 3.11 (matches Databricks Runtime)
* **Package installation**: `pip install -e .[dev]`
  * `-e`: Editable install (development mode)
  * `[dev]`: Includes dev dependencies (pytest, linters)

#### Compilation Check
* **`py_compile`**: Checks syntax without running
* **`compileall src`**: Validates entire `src/` directory
* **Fast fail**: Catches syntax errors before tests run

#### Test Execution
* **`pytest tests/unit tests/integration`**: Runs all tests
* **Requires**: Tests to be implemented in `tests/` folders
* **Current status**: Test structure exists but needs implementation

### Required Dependencies

Must be defined in `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov",
    "black",
    "ruff",
]
```

### Future Enhancements

Consider adding:

```yaml
# Code quality checks
- name: Run linter
  run: ruff check src

- name: Check formatting
  run: black --check src

# Coverage reporting
- name: Run tests with coverage
  run: pytest --cov=src --cov-report=xml tests/

# Databricks deployment
- name: Deploy to dev
  if: github.ref == 'refs/heads/main'
  run: |
    databricks bundle deploy --target dev
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

---

## Relationship Between YAML Files

```
databricks.yml (Root)
    │
    ├─ includes ───> resources/quant_core_job.yml (Job definition)
    │                    │
    │                    └─ references ───> notebooks/ (Execution)
    │                                           │
    │                                           └─ uses ───> config/quant_core_config.yaml (Runtime params)
    │
    └─ deployed via ───> Databricks CLI (databricks bundle deploy)
                              │
                              └─ validated by ───> .github/workflows/ci.yml (CI/CD)
```

### Data Flow

1. **Developer** modifies code and pushes to GitHub
2. **CI/CD** (`.github/workflows/ci.yml`) validates and tests
3. **Developer** runs `databricks bundle deploy`
4. **DABs** (`databricks.yml`) reads bundle configuration
5. **DABs** includes job definition (`resources/quant_core_job.yml`)
6. **Job** executes notebooks with parameters
7. **Notebooks** read runtime config (`config/quant_core_config.yaml`)
8. **ETL** processes data through Bronze → Silver → Gold

---

## When to Edit Each File

### databricks.yml
**Edit when:**
* Adding new deployment targets (e.g., `staging`, `qa`)
* Adding global variables
* Changing bundle name
* Including new resource files

**Example:**
```yaml
targets:
  staging:
    workspace:
      root_path: /Workspace/Shared/.bundle/${bundle.name}/staging
```

### resources/quant_core_job.yml
**Edit when:**
* Adding new ETL tasks
* Changing task dependencies
* Modifying notebook paths
* Adding cluster configurations
* Setting up notifications

**Example:**
```yaml
- task_key: data_quality_checks
  depends_on:
    - task_key: gold_calculations
  notebook_task:
    notebook_path: ./notebooks/04_quality/data_quality_checks.py
```

### config/quant_core_config.yaml
**Edit when:**
* Changing VaR confidence levels
* Updating stress test windows
* Modifying data paths
* Adjusting mock data generation parameters
* Changing catalog/schema names

**Example:**
```yaml
runtime:
  var_confidence_levels:
    - 0.90  # Add 90% VaR
    - 0.95
    - 0.99
```

### .github/workflows/ci.yml
**Edit when:**
* Adding code quality checks (linting, formatting)
* Adding security scans
* Setting up deployment automation
* Changing Python version
* Adding coverage reporting

**Example:**
```yaml
- name: Security scan
  run: bandit -r src/
```

---

## Best Practices

### 1. Version Control
✅ **DO:**
* Commit all YAML files to Git
* Use descriptive commit messages when changing config
* Review YAML changes in pull requests

❌ **DON'T:**
* Store secrets in YAML files
* Commit local overrides

### 2. Variable Management
✅ **DO:**
* Use variables for environment-specific values
* Document variable purposes in comments
* Validate required variables before deployment

❌ **DON'T:**
* Hard-code environment names
* Use absolute paths

### 3. Documentation
✅ **DO:**
* Add comments explaining non-obvious settings
* Document variable interpolation syntax
* Keep this guide updated when adding files

### 4. Testing
✅ **DO:**
* Validate YAML syntax before committing
* Test bundle deployment in dev before prod
* Run `databricks bundle validate` locally

---

## Troubleshooting

### Common Issues

#### 1. Bundle Validation Fails
```bash
Error: variable 'target_yyyymm' is not defined
```
**Solution:** Check `databricks.yml` variables section

#### 2. Job Fails to Find Notebook
```
Error: Notebook not found: ./notebooks/01_ingest_bronze/01_bronze_ingestion.py
```
**Solution:** Verify notebook path in `quant_core_job.yml` is relative to bundle root

#### 3. Configuration Not Loading
```python
KeyError: 'var_confidence_levels'
```
**Solution:** Check YAML syntax in `quant_core_config.yaml` (indentation matters!)

#### 4. CI Fails on pytest
```
ERROR: No module named 'pytest'
```
**Solution:** Add pytest to `pyproject.toml` dev dependencies

---

## References

* **Databricks Asset Bundles**: https://docs.databricks.com/dev-tools/bundles/
* **Jobs API**: https://docs.databricks.com/workflows/jobs/jobs-api.html
* **GitHub Actions**: https://docs.github.com/en/actions
* **YAML Syntax**: https://yaml.org/spec/1.2/spec.html

---

**Document Version:** 1.0  
**Last Updated:** April 2026  
**Author:** Quant Core Team  
**Related Documents:**
* [Project Analysis & Recommendations](../reviews/project-analysis-and-recommendations.md)
* [Scheduling and CI/CD](../operations/scheduling-and-cicd.md)
* [Solution Architecture](../architecture/solution-architecture.md)
