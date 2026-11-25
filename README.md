# databricks-interview

A Databricks project (pipeline, tables and Genie space for a business problem), built with the shared process in `docs/databricks-project-playbook.md`. Start with section 4 of the playbook (the new-project checklist).

## Setup (once per clone)

1. Local environment and Databricks connection: follow the `baseball` repo's README sections 2 and 8 (Python 3.12 venv, `.env`, Databricks CLI profile).
2. Databricks agent skills, installed in project scope and not committed. From the repo root:

```
databricks aitools install --agents claude-code --scope project --skills-only \
  --skills databricks-core,databricks-pipelines,databricks-dabs,databricks-serverless,databricks-jobs,databricks-unity-catalog,databricks-metric-views,databricks-genie-agents
python3 scripts/check_process_sync.py
```

The version, the update steps and how the skills relate to CLAUDE.md are in `docs/databricks-project-playbook.md` section 2.8.
