# CLAUDE.md

Instructions for working in this repository.

`docs/databricks-project-playbook.md` is this repo's guide for the concepts and patterns (medallion layers, ingest, pipelines, jobs, bundles, serverless, models, Genie, documentation, agent skills) and the phased plan for building the project.

## Project overview

Not started. The business problem, sources and users are decided in Phase 0 of the playbook's build plan (section 4). Fill in this section from the CLAUDE.md skeleton in playbook section 5.1 as the project is built.

## Security and data

- Never hard-code secrets. On Databricks, use `dbutils.secrets`. Locally, use `.env`, which is gitignored.
- Data files (`*.csv`, `*.parquet`, `*.xlsx`) are gitignored and must not be committed.
- The Databricks CLI is configured locally in `~/.databrickscfg`, which is gitignored. Never commit it, and never print tokens.
- Commits and pushes: see the working rules below.

## Working rules

- The user commits and pushes. Never commit or push.
- Check with the user before changing anything in Databricks: deploying, running jobs or pipelines that overwrite tables, or changing tables, jobs, pipelines, grants, Genie spaces, wikis or Pages.
- Deploy only when the local tree matches the pushed commit. Check the workspace Git folder's commit with `databricks repos get <git folder> -o json`. Run `databricks bundle validate --strict -t prod` before every deploy.
- Never edit code, Genie spaces, wikis or Pages in the workspace UI. Change the repo, then redeploy or republish.
- Coding subagents run on Sonnet.

## Databricks agent skills

Databricks' agent skills are installed per project with `databricks aitools` (project scope, skills version **0.2.47**). The files are in `.databricks/aitools/skills/`, with links in `.claude/skills/`, and are not committed. Install and update steps are in `docs/databricks-project-playbook.md` section 2.8.

Installed skills: `databricks-core`, `databricks-pipelines`, `databricks-dabs`, `databricks-serverless`, `databricks-jobs`, `databricks-unity-catalog`, `databricks-metric-views`, `databricks-genie-agents`.

**The skills win by default.** Follow a skill's guidance even where it differs from older habits or examples in this repo, except for the exceptions below, which only the user can lift. If the user says not to follow a skill in a specific case, do what the user says.

Exceptions (CLAUDE.md wins):
- **Ask first:** check with the user before deploying, before running jobs or pipelines that overwrite tables, and before changing tables, jobs, pipelines, grants, Genie spaces, wikis or Pages. Deploy only what was pushed. The serverless skill says to deploy, then report.
- **Genie:** the space is deployed only by the bundle, from `scripts/build_genie_space.py`. Never run `databricks genie create-space`, `update-space` or `trash-space`. Using the skill to query a space, run evals and diagnose misses is fine. Databricks now calls Genie spaces "Genie Agents".
- **One instruction file and our layout:** CLAUDE.md is the only agent instructions file. Never run `databricks bundle init`, and never create `AGENTS.md`. Keep this folder layout (`ingest/`, `pipeline/transformations/<layer>/`, `pipeline/utilities/`, `models/`, `resources/`, `scripts/`, `docs/`).
- **Profile:** use the default CLI profile (the one `databricks auth profiles` marks as Default). Don't ask the user to pick one.
- **Job tags and compute mode:** don't add `aidevkit_project` tags (or any other tags), and don't add or change `performance_target` without asking.

Following the skills means, for example:
- **Pipelines:** an existing pipeline keeps its language. A new pipeline is SQL by default, and Python is fine when SQL can't do the job or fails (Python-only features, shared helpers in `pipeline/utilities/`, UDFs).
- **New datasets:** one dataset per file, named after the dataset. Use schema-qualified names only when publishing outside the pipeline's default schema, as the medallion layers do.
- **Bundles:** run `databricks bundle validate --strict -t prod` after configuration changes and before every deploy.
