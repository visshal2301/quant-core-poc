# 🚀 Complete CI/CD Implementation Guide

## 📋 Overview

This guide walks through implementing a production-grade CI/CD pipeline for your Quant Core POC using:
- **Databricks Asset Bundles (DAB)** for infrastructure as code
- **GitHub Actions** for CI/CD automation
- **Multi-environment deployment** (dev → staging → prod)
- **Automated testing** and validation

---

## 🏗️ Architecture

```
Developer → Git Push → GitHub Actions → Databricks Environments
                            ↓
                    ┌───────┴────────┐
                    │                │
                CI Pipeline    CD Pipeline
                    │                │
              ┌─────┴─────┐    ┌─────┴─────┐
              │           │    │           │
           Lint/Test   Build  Dev    Staging → Prod
```

---

## ✅ What's Already Set Up

Your project already has:
1. ✅ Git repository initialized
2. ✅ Databricks Asset Bundle configuration
3. ✅ Basic CI workflow
4. ✅ Job resource definitions
5. ✅ Test directory structure
6. ✅ Configuration management

What we just created:
1. ✅ Enhanced CI/CD workflows
2. ✅ Multi-environment bundle config
3. ✅ Sample tests (unit & integration)
4. ✅ Updated job configuration

---

## 🎯 Step-by-Step Implementation

### Step 1: Set Up Git Remote Repository

#### Option A: GitHub (Recommended)

1. **Create GitHub repository:**
   ```bash
   # Go to https://github.com/new
   # Create repository: quant-core-poc
   # Don't initialize with README (you already have code)
   ```

2. **Connect local repo to GitHub:**
   ```bash
   cd /Workspace/Users/padaialgo@gmail.com/quant-core-poc
   
   # Add remote
   git remote add origin https://github.com/YOUR-ORG/quant-core-poc.git
   
   # Push code
   git add .
   git commit -m "Initial commit with CI/CD setup"
   git push -u origin main
   ```

#### Option B: Azure DevOps

```bash
# Create project in Azure DevOps
# Then connect:
git remote add origin https://dev.azure.com/YOUR-ORG/quant-core/_git/quant-core-poc
git push -u origin main
```

---

### Step 2: Configure Databricks Authentication

#### 2.1 Create Service Principal

1. **In Databricks:**
   - Admin Console → Service Principals
   - Click "Add Service Principal"
   - Name: `cicd-quant-core`
   - Copy the **Application ID**

2. **Generate Token:**
   - Settings → Developer → Access Tokens
   - Generate new token for service principal
   - **Save the token securely** (you won't see it again!)

3. **Grant Permissions:**
   ```sql
   -- In SQL workspace
   GRANT USE CATALOG ON CATALOG quant_core TO `cicd-quant-core`;
   GRANT CREATE SCHEMA ON CATALOG quant_core TO `cicd-quant-core`;
   GRANT USE SCHEMA ON SCHEMA quant_core.bronze TO `cicd-quant-core`;
   GRANT USE SCHEMA ON SCHEMA quant_core.silver TO `cicd-quant-core`;
   GRANT SELECT, MODIFY ON SCHEMA quant_core.bronze TO `cicd-quant-core`;
   GRANT SELECT, MODIFY ON SCHEMA quant_core.silver TO `cicd-quant-core`;
   ```

#### 2.2 Store Secrets in GitHub

**GitHub Repository → Settings → Secrets and variables → Actions**

Add these secrets:

| Secret Name | Value | Description |
|-------------|-------|-------------|
| `DATABRICKS_HOST` | `https://dbc-7c64d739-3c54.cloud.databricks.com` | Workspace URL |
| `DATABRICKS_TOKEN` | `<token-from-step-2.2>` | Service principal token |
| `DATABRICKS_DEV_HOST` | Same as above | Dev environment |
| `DATABRICKS_DEV_TOKEN` | Same token or separate | Dev token |
| `DATABRICKS_STAGING_HOST` | Staging workspace URL (if separate) | Staging environment |
| `DATABRICKS_STAGING_TOKEN` | Staging token | Staging token |
| `DATABRICKS_PROD_HOST` | Production workspace URL (if separate) | Prod environment |
| `DATABRICKS_PROD_TOKEN` | Production token | Prod token |

**Note:** For single workspace setup, use the same host/token for all environments. Environments are separated by deployment paths.

---

### Step 3: Update pyproject.toml

Ensure your `pyproject.toml` has all dependencies:

```toml
[project]
name = "quant-core"
version = "0.1.0"
dependencies = [
    "pyspark>=3.5.0",
    "delta-spark>=3.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "black>=23.7.0",
    "ruff>=0.0.285",
    "mypy>=1.5.0",
]

[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"

[tool.black]
line-length = 120
target-version = ["py311"]

[tool.ruff]
line-length = 120
target-version = "py311"
```

---

### Step 4: Test Local Deployment

Before pushing to GitHub, test deployment locally:

```bash
cd /Workspace/Users/padaialgo@gmail.com/quant-core-poc

# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Configure authentication
export DATABRICKS_HOST="https://dbc-7c64d739-3c54.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"

# Validate bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Check deployment
databricks bundle run quant_core_monthly_pipeline_dev
```

---

### Step 5: Enable GitHub Actions

1. **Push your code:**
   ```bash
   git add .
   git commit -m "Add CI/CD workflows"
   git push origin main
   ```

2. **Check workflow runs:**
   - Go to GitHub → Actions tab
   - You should see "CI - Test and Validate" running

3. **First run will fail** (expected!) because:
   - Tests need to be updated with real implementations
   - Databricks secrets not configured yet

---

### Step 6: Configure GitHub Environments

**For production deployments with approvals:**

1. **GitHub Repository → Settings → Environments**

2. **Create environments:**
   - `dev` (no protection rules)
   - `staging` (optional: require 1 reviewer)
   - `prod` (require 2 reviewers + wait timer)

3. **Configure protection rules for `prod`:**
   - Required reviewers: 2
   - Wait timer: 5 minutes
   - Restrict to main branch only

---

### Step 7: Deploy to Each Environment

#### Deploy to Dev (Automatic)

Every push to `main` automatically deploys to dev:
```bash
git push origin main
# Watch GitHub Actions → CD - Deploy to Databricks
```

#### Deploy to Staging (Manual)

```bash
# Go to GitHub → Actions → CD - Deploy to Databricks
# Click "Run workflow"
# Select environment: staging
# Click "Run workflow"
```

#### Deploy to Production (Manual + Approval)

```bash
# Go to GitHub → Actions → CD - Deploy to Databricks
# Click "Run workflow"
# Select environment: prod
# Click "Run workflow"
# → Requires approval from designated reviewers
```

---

### Step 8: Implement Tests

#### Update Unit Tests

Edit `tests/unit/test_silver_transformations.py`:
- Add tests for your transformation functions
- Test data quality rules
- Test business logic

#### Update Integration Tests

Edit `tests/integration/test_pipeline.py`:
- Create test catalog: `quant_core_test`
- Run pipeline against test data
- Validate results

#### Run Tests Locally

```bash
cd /Workspace/Users/padaialgo@gmail.com/quant-core-poc

# Install test dependencies
pip install -e .[dev]

# Run unit tests
pytest tests/unit -v

# Run integration tests
pytest tests/integration -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

---

### Step 9: Set Up Monitoring & Alerting

#### Databricks Job Monitoring

1. **Configure email alerts** (already in bundle config):
   ```yaml
   email_notifications:
     on_failure:
       - data-team@company.com
     on_success:
       - data-team@company.com
   ```

2. **Set up Slack webhooks** (optional):
   ```yaml
   webhook_notifications:
     on_failure:
       - id: <slack-webhook-id>
   ```

#### GitHub Actions Notifications

1. **Slack integration:**
   - Add Slack app to GitHub
   - Configure notifications for workflow failures

2. **Email notifications:**
   - GitHub → Settings → Notifications
   - Enable workflow failure emails

---

### Step 10: Create Deployment Checklist

#### Pre-Deployment Checklist

- [ ] All tests passing locally
- [ ] Code reviewed and approved
- [ ] No merge conflicts
- [ ] Updated version number (if applicable)
- [ ] Updated documentation
- [ ] Breaking changes documented

#### Production Deployment Checklist

- [ ] Tested in staging environment
- [ ] Data backup completed
- [ ] Rollback plan documented
- [ ] Stakeholders notified
- [ ] Monitoring dashboards ready
- [ ] On-call engineer assigned

---

## 📚 Common Workflows

### Making a Change

```bash
# 1. Create feature branch
git checkout -b feature/add-new-metric

# 2. Make changes
# Edit files...

# 3. Run tests locally
pytest tests/

# 4. Commit and push
git add .
git commit -m "Add new risk metric calculation"
git push origin feature/add-new-metric

# 5. Create Pull Request
# GitHub → Pull requests → New pull request

# 6. Wait for CI checks
# CI will run automatically

# 7. Get code review and merge
# After approval, merge to main

# 8. Automatic dev deployment
# CD pipeline deploys to dev automatically

# 9. Test in dev
# Verify changes work in dev environment

# 10. Deploy to production
# Use GitHub Actions workflow dispatch
```

### Hotfix to Production

```bash
# 1. Create hotfix branch from main
git checkout main
git pull
git checkout -b hotfix/fix-dimension-bug

# 2. Make fix
# Edit files...

# 3. Fast-track testing
pytest tests/unit

# 4. Commit and create PR
git add .
git commit -m "Fix: Handle null values in dim_currency"
git push origin hotfix/fix-dimension-bug

# 5. Get expedited review
# Mark PR as urgent

# 6. Merge to main
# After approval

# 7. Deploy directly to prod
# Use workflow dispatch with prod environment
```

### Rollback

```bash
# Option 1: Redeploy previous version
databricks bundle deploy -t prod --var="version=previous"

# Option 2: Revert git commit
git revert <commit-hash>
git push origin main

# Option 3: Manual rollback
# Disable job in Databricks UI
# Restore previous bundle
```

---

## 🔍 Monitoring & Troubleshooting

### Check Deployment Status

```bash
# View deployed resources
databricks bundle resources -t prod

# Check job status
databricks jobs list --name "Bronze Ingestion Pipeline - prod"

# View recent runs
databricks jobs runs list --job-id <job-id> --limit 10
```

### View Logs

```bash
# Get run output
databricks jobs runs get-output --run-id <run-id>

# For specific task
databricks jobs runs get-output --run-id <run-id> --task-key silver_transformation
```

### Debug Failed Workflow

1. **GitHub Actions:**
   - Actions tab → Click failed workflow
   - Expand failed step
   - Check logs

2. **Databricks:**
   - Jobs → Click failed run
   - View task output
   - Check cluster logs

---

## 📖 Best Practices

### Code Quality

1. **Always run tests before committing:**
   ```bash
   pytest tests/
   black src/ tests/
   ruff check src/ tests/
   ```

2. **Write meaningful commit messages:**
   ```
   ✅ Good: "Fix: Handle empty dimension tables in silver transformation"
   ❌ Bad: "fix bug"
   ```

3. **Keep PRs focused:**
   - One feature/fix per PR
   - Small, reviewable changes
   - Clear description

### Deployment Safety

1. **Test in dev first:**
   - Always deploy to dev
   - Run smoke tests
   - Verify results

2. **Staged rollout:**
   - dev → staging → prod
   - Wait 24hrs in staging
   - Monitor metrics

3. **Rollback plan:**
   - Document how to rollback
   - Test rollback in staging
   - Keep previous version available

### Security

1. **Never commit secrets:**
   - Use GitHub Secrets
   - Rotate tokens regularly
   - Use service principals

2. **Least privilege:**
   - Grant minimum permissions
   - Separate dev/prod access
   - Audit access regularly

3. **Code review:**
   - Require 1+ approver
   - Review dependencies
   - Check for vulnerabilities

---

## 🆘 Troubleshooting

### Issue: Bundle validation fails

```bash
# Check bundle syntax
databricks bundle validate -t dev

# Common issues:
# - Invalid YAML syntax
# - Missing variables
# - Invalid resource references
```

### Issue: Deployment succeeds but job fails

```bash
# Check job configuration
databricks jobs get --job-id <job-id>

# Verify parameters are passed correctly
# Check notebook paths exist
# Verify permissions
```

### Issue: Tests fail in CI but pass locally

```bash
# Check Python version matches
# Verify dependencies in pyproject.toml
# Check for environment-specific issues
# Review GitHub Actions logs
```

### Issue: Workspace path not found

```bash
# Bundle deploys to:
# Dev: /Workspace/Users/<user>/.bundle/quant-core-poc/dev
# Prod: /Workspace/Shared/.bundle/quant-core-poc/prod

# Check workspace path in bundle config
# Verify sync configuration
```

---

## 📞 Support

- **Databricks Documentation:** https://docs.databricks.com/
- **Asset Bundles Guide:** https://docs.databricks.com/dev-tools/bundles/
- **GitHub Actions:** https://docs.github.com/actions

---

## ✅ Next Steps

1. [ ] Set up Git remote repository
2. [ ] Configure Databricks service principal
3. [ ] Add secrets to GitHub
4. [ ] Test local deployment
5. [ ] Push code to GitHub
6. [ ] Enable GitHub Actions
7. [ ] Configure environments
8. [ ] Deploy to dev
9. [ ] Write/update tests
10. [ ] Deploy to staging
11. [ ] Deploy to production

---

**Congratulations! 🎉** You now have a production-ready CI/CD pipeline for your Databricks jobs!
