# Databricks project

A Databricks project (pipeline, tables and Genie space for a business problem), built with the concepts and plan in `docs/databricks-project-playbook.md`. Start with section 4 of the playbook (the build plan).

## Setup (once per clone)

1. Python 3.12 virtual environment (newer Python versions break `databricks-sql-connector`):

```
python3.12 -m venv .venv
source .venv/bin/activate
```

2. `.env` (gitignored) for any local connection settings, for example the workspace host and warehouse id. Never commit secrets.

3. Databricks CLI and login:

```
brew tap databricks/tap && brew install databricks
databricks auth login --host https://<workspace-host>
```

4. Databricks agent skills, installed in project scope and not committed. From the repo root:

```
databricks aitools install --agents claude-code --scope project --skills-only \
  --skills databricks-core,databricks-pipelines,databricks-dabs,databricks-serverless,databricks-jobs,databricks-unity-catalog,databricks-metric-views,databricks-genie-agents
```

The version, the update steps and how the skills relate to CLAUDE.md are in `docs/databricks-project-playbook.md` section 2.8.
