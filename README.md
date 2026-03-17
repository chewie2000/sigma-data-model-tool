# Sigma Data Models - Repository Template

This folder contains everything you need to set up a GitHub repository for managing Sigma data models as code.

## Quick Setup

1. **Create a new private GitHub repository**

2. **Copy these files to your repository:**
   ```
   your-repo/
   ├── .github/
   │   └── workflows/
   │       ├── sync-to-sigma.yml      # Syncs changes to Sigma on merge
   │       └── pull-from-sigma.yml    # Pulls changes from Sigma (scheduled)
   ├── scripts/
   │   ├── sync_to_sigma.py           # Push to Sigma
   │   ├── pull_from_sigma.py         # Pull from Sigma
   │   └── generate_diff_report.py    # PR diff comments
   ├── data-models/
   │   └── _template.json             # Example data model template
   ├── config.yml                     # Configuration file
   └── .gitignore
   ```

3. **Configure GitHub Secrets** (Settings → Secrets and variables → Actions → Secrets):
   - `SIGMA_CLIENT_ID` - Your Sigma API Client ID
   - `SIGMA_SECRET` - Your Sigma API Client Secret

4. **Configure GitHub Variables** (Settings → Secrets and variables → Actions → Variables):
   - `SIGMA_CLOUD` - Your cloud: `aws`, `azure`, or `gcp`
   - `SIGMA_FOLDER_ID` - Folder ID where new data models will be created

5. **Enable Workflow Permissions** (Settings → Actions → General):
   - Select "Read and write permissions"
   - Check "Allow GitHub Actions to create and approve pull requests"

6. **Use the [Data Model Manager Tool](https://twells89.github.io/sigma-data-model-tool/)** to edit and commit changes!

## Files Included

| File | Purpose |
|------|---------|
| `sync-to-sigma.yml` | GitHub Action that syncs data models to Sigma when PRs are merged |
| `pull-from-sigma.yml` | GitHub Action that pulls changes from Sigma (runs daily + manual) |
| `sync_to_sigma.py` | Python script to push changes to Sigma API |
| `pull_from_sigma.py` | Python script to pull data models from Sigma |
| `generate_diff_report.py` | Generates PR comments showing what changed |
| `config.yml` | Stores data model ID mappings |
| `_template.json` | Example data model JSON structure |

## How It Works

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   Web Tool or   │  ──PR──▶│     GitHub      │──Merge─▶│     Sigma       │
│   Direct Edit   │         │  Review Changes │         │  Auto-deployed  │
└─────────────────┘         └─────────────────┘         └─────────────────┘
```

1. Edit data models in the web tool or directly in JSON
2. Commit creates a Pull Request
3. Review changes (diff report posted as comment)
4. Merge to deploy to Sigma automatically
