# CLAUDE.md

Instructions for working in this repository.

`docs/databricks-project-playbook.md` is this repo's guide for the concepts and patterns (landing volume, medallion layers, pipeline, bundle, Genie agent, dashboard, agent skills) and the phased plan for building the project: a 4-hour prototype on Databricks Free Edition. Production-only patterns are listed in its section 6 as the roadmap, not built.

## Project overview

Not started. The business problem, sources and users are decided in Phase 0 of the playbook's build plan (section 4), when the scenario arrives. Fill in this section from the CLAUDE.md skeleton in playbook section 5.1 as the project is built.

**Current state: a tested skeleton on dummy data.** One `orders` source (`data/orders.csv`, local only) flows through the landing volume to `bronze.bronze_orders`, `silver.silver_orders` and `gold.gold_daily_revenue` (each table's grain is in its pipeline comment). A Genie agent and a dashboard sit on the gold table. On the day, the skeleton's SQL, Genie configuration and dashboard are replaced with the scenario's.

## Syncing with Databricks

- Workspace: Free Edition, default CLI profile. Catalog `databricks_interview`, created once with SQL (README step 5), not by the bundle.
- Bundle `databricks_interview`, target `prod`, deployed to `/Workspace/Users/<user>/.bundle/databricks_interview/prod`. It creates the schemas `raw`, `bronze`, `silver`, `gold`, the volume `raw.landing` and the pipeline.
- Pipeline: key `databricks_interview_pipeline`, id `da84d6bc-98f9-434d-85d0-0fd78a39ced6`. Run with `databricks bundle run databricks_interview_pipeline -t prod`.
- SQL warehouse: `Serverless Starter Warehouse` (looked up by name in `databricks.yml`).
- Dashboard: bundle key `regional_revenue`, "Regional Revenue", id `01f1e91160f511239e6ae604b05c73f7`, from `dashboards/regional_revenue.lvdash.json`.
- Genie agent: "Regional Revenue", space id `01f1e9112d2d1e8db262657920132c4c`, in `/Workspace/Users/<user>/databricks_interview_demo`, from `genie/genie_agent.json` (outside the bundle; update with `databricks genie update-space`).
- Commands for deploy, upload and run are in the README.

## Security and data

- Never hard-code secrets. On Databricks, use `dbutils.secrets`. Locally, use `.env`, which is gitignored.
- Data files (`*.csv`, `*.parquet`, `*.xlsx`) are gitignored and must not be committed.
- The Databricks CLI is configured locally in `~/.databrickscfg`, which is gitignored. Never commit it, and never print tokens.
- Commits and pushes: see the working rules below.

## Working rules

- The user commits and pushes. Never commit or push. Suggest a commit at each milestone (silver works, gold works, before the demo), so there are points to roll back to.
- Inside the project (its catalog, bundle, Genie agent and dashboard), deploy, run the pipeline and change tables, the Genie agent and the dashboard without asking, then report what was done.
- Ask first before deleting anything (tables, schemas, volumes, files, the Genie agent, the dashboard, or a deploy that would delete or recreate a resource), before changing grants, and before touching anything outside the project.
- Deploy from the local files; they don't need to be pushed first. Run `databricks bundle validate --strict -t prod` before every deploy.
- Never edit deployed code in the workspace UI: change the repo, then redeploy. Scratch notebooks for exploring the data or trying Genie Code are fine; anything worth keeping moves into the repo. The Genie agent and the dashboard may be changed in the UI; copy Genie changes back into `genie/genie_agent.json`, and pull dashboard changes back with `databricks bundle generate dashboard --resource <key> --force` before the next deploy.
- Coding subagents run on Sonnet.

## Databricks agent skills

Databricks' agent skills are installed per project with `databricks aitools` (project scope, skills version **0.2.47**). The files are in `.databricks/aitools/skills/`, with links in `.claude/skills/`, and are not committed. Install and update steps are in `docs/databricks-project-playbook.md` section 2.8.

Installed skills: `databricks-core`, `databricks-pipelines`, `databricks-dabs`, `databricks-serverless`, `databricks-jobs`, `databricks-unity-catalog`, `databricks-metric-views`, `databricks-genie-agents`.

**The skills win by default.** Follow a skill's guidance even where it differs from older habits or examples in this repo, except for the exceptions below, which only the user can lift. If the user says not to follow a skill in a specific case, do what the user says.

Exceptions (CLAUDE.md wins):
- **Ask before deleting or reaching outside the project:** inside the project, deploy and run, then report, as the serverless skill does. Whatever a skill says, ask before deleting anything, changing grants, or touching anything outside the project (see the working rules).
- **Genie:** the agent is built outside the bundle, in the UI or with the skill's `create-space` / `update-space` from `genie/genie_agent.json`. Keep that file current: copy UI changes back into it. Never run `trash-space` without asking. Databricks now calls Genie spaces "Genie Agents".
- **One instruction file and our layout:** CLAUDE.md is the only agent instructions file. Never run `databricks bundle init`, and never create `AGENTS.md`. Keep this folder layout (`pipeline/transformations/<layer>/`, `resources/`, `genie/`, `dashboards/`, `docs/`, plus `ingest/` only if a source is an API).
- **Profile:** use the default CLI profile (the one `databricks auth profiles` marks as Default). Don't ask the user to pick one.
- **Job tags and compute mode:** don't add `aidevkit_project` tags (or any other tags), and don't add or change `performance_target` without asking.

Following the skills means, for example:
- **Pipelines:** an existing pipeline keeps its language. A new pipeline is SQL by default, and Python is fine when SQL can't do the job or fails.
- **New datasets:** one dataset per file, named after the dataset. Use schema-qualified names only when publishing outside the pipeline's default schema, as the medallion layers do.
- **Bundles:** run `databricks bundle validate --strict -t prod` after configuration changes and before every deploy.
