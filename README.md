# Databricks project

A 4-hour Databricks prototype on Free Edition for a business problem: files landed in a volume, a pipeline from bronze to silver to gold, and a Genie agent and AI/BI dashboard to answer the stakeholder's questions. Built with the concepts and plan in `docs/databricks-project-playbook.md`. Start with section 4 of the playbook (the build plan).

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

5. The catalog, once per workspace. Free Edition only creates catalogs through SQL or the UI (the bundle can't), so run this on the SQL warehouse:

```
databricks experimental aitools tools query --warehouse <warehouse-id> \
  "CREATE CATALOG IF NOT EXISTS databricks_interview"
```

## Deploy and run

```
databricks bundle validate --strict -t prod
databricks bundle deploy -t prod                       # schemas, landing volume, pipeline
databricks fs mkdir dbfs:/Volumes/databricks_interview/raw/landing/<source>
databricks fs cp <file> dbfs:/Volumes/databricks_interview/raw/landing/<source>/
databricks bundle run databricks_interview_pipeline -t prod
```

Source files stay local in `data/` (gitignored, and excluded from bundle sync).
