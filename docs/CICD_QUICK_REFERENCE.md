# 🚀 CI/CD Quick Reference

## 📦 Repository Structure

```
quant-core-poc/
├── .github/
│   └── workflows/
│       ├── ci.yml              # Runs on every push
│       ├── cd.yml              # Deploys to environments
│       └── pr-validation.yml   # Validates PRs
├── config/
│   └── quant_core_config.yaml # Configuration
├── notebooks/
│   ├── 01_ingest_bronze/
│   └── 02_transform_silver/
├── resources/
│   └── quant_core_job.yml     # Job definition
├── scripts/
│   └── generate_mock_data.py
├── src/
│   └── quant_core/            # Python package
├── tests/
│   ├── unit/                  # Unit tests
│   └── integration/           # Integration tests
├── databricks.yml             # Bundle configuration
└── pyproject.toml            # Python dependencies
```

## 🔑 Essential Commands

### Local Development

```bash
# Install dependencies
pip install -e .[dev]

# Run tests
pytest tests/ -v

# Format code
black src/ tests/ scripts/

# Lint code
ruff check src/ tests/ scripts/

# Type check
mypy src/
```

### Databricks CLI

```bash
# Authenticate
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

# Validate bundle
databricks bundle validate -t dev

# Deploy to environment
databricks bundle deploy -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod

# Run job
databricks bundle run quant_core_monthly_pipeline_dev

# View resources
databricks bundle resources -t dev

# Destroy (careful!)
databricks bundle destroy -t dev
```

### Git Workflow

```bash
# Start new feature
git checkout -b feature/my-feature

# Stage changes
git add .

# Commit
git commit -m "feat: Add new feature"

# Push
git push origin feature/my-feature

# After PR approval, merge deletes branch
# Pull latest main
git checkout main
git pull
```

## 🌍 Environments

| Environment | Branch | Approval | Auto Deploy | Use Case |
|-------------|--------|----------|-------------|----------|
| **dev** | any | No | Yes | Development & testing |
| **staging** | main | Optional | Manual | Pre-production validation |
| **prod** | main | Required | Manual | Production workloads |

## 🔄 CI/CD Triggers

| Workflow | Trigger | Actions |
|----------|---------|---------|
| **CI** | Push to any branch | Lint, test, validate |
| **PR Validation** | Open/update PR | Test, comment results |
| **CD (dev)** | Push to main | Deploy to dev |
| **CD (staging/prod)** | Manual dispatch | Deploy to selected env |

## 📊 Deployment Paths

| Environment | Workspace Path | Catalog |
|-------------|----------------|---------|
| dev | `/Workspace/Users/<user>/.bundle/quant-core-poc/dev` | `quant_core_dev` |
| staging | `/Workspace/Shared/.bundle/quant-core-poc/staging` | `quant_core_staging` |
| prod | `/Workspace/Shared/.bundle/quant-core-poc/prod` | `quant_core` |

## 🔐 Required Secrets (GitHub)

```yaml
DATABRICKS_HOST           # Workspace URL
DATABRICKS_TOKEN          # Service principal token
DATABRICKS_DEV_HOST       # Dev workspace (or same)
DATABRICKS_DEV_TOKEN      # Dev token
DATABRICKS_STAGING_HOST   # Staging workspace
DATABRICKS_STAGING_TOKEN  # Staging token
DATABRICKS_PROD_HOST      # Prod workspace
DATABRICKS_PROD_TOKEN     # Prod token
```

## ⚡ Quick Actions

### Deploy to Dev

```bash
git push origin main
# Wait for GitHub Actions to complete
```

### Deploy to Production

1. Go to GitHub → Actions
2. Click "CD - Deploy to Databricks"
3. Click "Run workflow"
4. Select `prod` environment
5. Wait for approvals
6. Workflow deploys

### Rollback

```bash
# Option 1: Revert commit
git revert <commit-hash>
git push origin main

# Option 2: Redeploy previous version
git checkout <previous-commit>
databricks bundle deploy -t prod

# Option 3: Disable job in Databricks UI
```

### Check Job Status

```bash
# List jobs
databricks jobs list --name "Bronze Ingestion Pipeline"

# Get job details
databricks jobs get --job-id <job-id>

# List recent runs
databricks jobs runs list --job-id <job-id> --limit 5

# Get run output
databricks jobs runs get-output --run-id <run-id>
```

## 🧪 Testing Checklist

- [ ] Unit tests pass locally
- [ ] Integration tests pass
- [ ] Code formatted (black)
- [ ] Linter passes (ruff)
- [ ] No type errors (mypy)
- [ ] Documentation updated
- [ ] PR reviewed
- [ ] CI passes on GitHub

## 📈 Monitoring

### Job Monitoring

- Databricks Jobs UI
- Email notifications
- Slack webhooks (if configured)

### CI/CD Monitoring

- GitHub Actions tab
- Email notifications
- Workflow run history

## 🆘 Emergency Procedures

### Production Issue

1. **Immediate:** Disable job in Databricks UI
2. **Identify:** Check run logs and error messages
3. **Fix:** Create hotfix branch
4. **Test:** Run tests in dev
5. **Deploy:** Fast-track to prod with approvals
6. **Verify:** Monitor first prod run

### Rollback Required

1. Identify last good commit
2. Revert or redeploy
3. Verify rollback successful
4. Document incident
5. Post-mortem analysis

## 📞 Support Resources

- **Databricks Docs:** https://docs.databricks.com/
- **Asset Bundles:** https://docs.databricks.com/dev-tools/bundles/
- **GitHub Actions:** https://docs.github.com/actions
- **pytest:** https://docs.pytest.org/

## 🎯 Next Steps After Setup

1. [ ] Update tests with real implementations
2. [ ] Set up monitoring dashboards
3. [ ] Configure alert policies
4. [ ] Document runbook procedures
5. [ ] Schedule first production run
6. [ ] Train team on CI/CD workflow

---

**Pro Tip:** Keep this reference handy! Bookmark or print it for quick access.
