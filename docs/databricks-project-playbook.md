# Databricks project playbook

**Shared process for two repos: `baseball` and `databricks-interview`.** This file is byte-identical in both repos, and `scripts/check_process_sync.py` checks that it is (section 2.9). Change it in both repos in the same change. Keep project-specific facts (tables, IDs, results, open items) out of it; they belong in each repo's CLAUDE.md and `docs/project-status.md`.

Process version: 2026-11-25.

This playbook holds the patterns that worked in the baseball project, the problems we hit, the shared working rules, and a checklist plus templates for building a new Databricks project (new source data, new pipeline, new tables, new Genie space) quickly. Baseball files are cited as worked examples to copy and adapt; in `databricks-interview`, read them in the `baseball` repo.

**`databricks-interview`** is a separate git repo with its own bundle, catalog, pipeline, jobs and Genie space. Nothing in it points back to `baseball`, except that this playbook and the sync script are shared.

**How to use it:** for a new project, work through section 4 (the checklist) in order, using section 5 for the starter files. Sections 2 and 3 explain why each step is done that way. For an existing project, sections 2.6 to 2.9 are the rules both repos follow.

---

## 1. The shape of the system

```
source API / files
   │  ingest notebook (one per source, parameterized, idempotent)
   ▼
<catalog>.raw.<source>_raw            landing table, values kept as returned
   │  one Spark Declarative Pipeline (bronze → silver → gold)
   ▼
bronze.bronze_<source>                raw + basic expectations, no logic
silver.silver_<source>[_<dataset>]    one row per stated grain: typed, renamed, deduplicated, flagged
gold.gold_<topic>                     tables shaped around the questions people ask; may join sources
   │  model notebooks (separate job, outside the pipeline)
   ▼
ml.<model outputs>                    scores, grades, forecasts, evaluation tables
   │
   ▼
Genie space (defined as code)         plain-language questions over a curated set of tables
```

The pieces deploy together as one **Databricks Asset Bundle**:

| Piece | Baseball file | What it does |
|---|---|---|
| Bundle root | `databricks.yml` | variables (catalog, schemas, warehouse), dev/prod targets |
| Ingest + refresh job | `resources/statcast.job.yml` | parallel ingest tasks, then the pipeline task; schedule starts paused |
| Pipeline | `resources/statcast.pipeline.yml` | serverless SDP, `root_path: ../pipeline`, glob over `transformations/**` |
| Model job | `resources/models.job.yml` | DAG of model notebooks on serverless, run manually |
| Genie space | `resources/pitcher_analysis.genie.yml` plus the generated JSON | space deployed with the bundle |

---

## 2. Patterns that worked, and how to repeat them

### 2.1 Git is the source of truth, and the bundle is the deploy unit

- Every job, pipeline and Genie space is defined in `resources/*.yml`. Nothing is created by clicking in the UI, so a workspace can be rebuilt with `databricks bundle deploy -t prod`.
- The `dev` target (`mode: development`) prefixes resources with `[dev <user>]` and pauses schedules, so experiments never touch production.
- Variables in `databricks.yml` (`catalog`, `raw_schema`, `default_schema`, `warehouse_id`) are passed to notebooks as `base_parameters`, so the same code runs in any catalog.
- New schedules start `pause_status: PAUSED`. Unpause after the first manual run succeeds.
- **Workflow:** edit locally, the user commits and pushes, then run `databricks bundle validate --strict -t prod` and deploy from a tree that matches the pushed commit. A quick check: `databricks repos get <git-folder> -o json` shows the commit the workspace Git folder is on; compare it with local `HEAD`.
- Never edit code in the workspace. Edits there drift from git and are overwritten on the next deploy.

### 2.2 Ingest notebooks: parameterized, windowed, idempotent

Examples: `ingest/statcast_ingest.py` (library returns pandas), `ingest/mlb_stats_ingest.py` (REST, per season), `ingest/mlb_boxscore_ingest.py` (REST, per entity on a thread pool).

- **Widgets with blank defaults.** `dbutils.widgets.text("start_date", "")`, where blank means the scheduled default (yesterday, or the last N days). The same notebook then serves the daily job, an ad hoc rerun and a backfill. Read widgets through a `_get_param(name, default)` helper so a missing widget falls back to the default.
- **A lookback window for late data.** The boxscore ingest reloads the last 3 days every run, so late-finishing or corrected records are picked up for free.
- **Chunk long ranges** (monthly windows), and write each chunk as it finishes, so a failed backfill resumes from where it stopped.
- **Idempotent writes:** `mode("overwrite")` with `replaceWhere` on the window column (`game_date >= X AND game_date <= Y`), never `append`. Reruns replace data instead of duplicating it. The `replaceWhere` column must be a real `DATE`, not a string.
- **Land values as returned, as strings,** plus `mergeSchema = true`. New or renamed API fields never break the ingest. Silver does the typing. Keep keys typed (ids as `bigint`, dates as `date`).
- **Stamp `ingested_at`,** so silver can keep the latest copy of a record.
- **Resilient HTTP:** timeouts, retries with exponential backoff on 429 and 5xx, a small thread pool (8 workers), and a generic User-Agent.
- **pandas to Spark through pyarrow** (`spark.createDataFrame(pa.Table.from_pandas(df, preserve_index=False))`), so nullable pandas dtypes keep their nulls.
- **One job task per source,** all running in parallel, with the pipeline task depending on every one of them.
- **Write a backfill runbook** (`docs/backfill-2023-2025.md`): the exact parameters, the chunk sizes and how to verify the result.

### 2.3 One Spark Declarative Pipeline for bronze, silver and gold

- **One pipeline for every source,** so gold tables can join sources and read other gold tables, with the dependency order handled automatically.
- **`root_path` plus a glob include.** Every file under `transformations/` becomes a dataset source automatically. Shared helpers go in `pipeline/utilities/` and are imported as `from utilities.x import y`. Helpers must not live under `transformations/`, or they are treated as datasets.
- **File layout:** files go under `transformations/<layer>/`. Baseball has one Python file per source per layer, sometimes with several datasets in a file. New pipelines follow the `databricks-pipelines` skill: one dataset per file, named after the dataset, in SQL by default, and Python when SQL can't do the job (2.8).
- **Every dataset** has a comment and expectations: in Python, `@dp.materialized_view(name="<schema>.<table>", comment=...)` with `@dp.expect(...)`; in SQL, `CREATE OR REFRESH MATERIALIZED VIEW <schema>.<table> (CONSTRAINT ... EXPECT (...)) COMMENT '...' AS ...`. Use schema-qualified names when a dataset publishes outside the pipeline's default schema, as the medallion layers do. The comment states the **grain** ("exactly one row per pitcher-season-role") and how the table relates to other tables. Comments appear in Unity Catalog and help Genie.
- **Catalog layout:** one catalog per project, one schema per layer (`raw`, `bronze`, `silver`, `gold`, `ml`). Table names are `<layer>_<source>[_<dataset>]`.

**Bronze:** read the raw table, drop junk columns, add NOT NULL expectations on the keys. No business logic.

**Silver** (examples: `silver/mlb_pitching.py`, `silver/mlb_pitching_game.py`, `silver/statcast.py`):
- **Enforce the grain explicitly.** Use `row_number()` over the natural key, ordered by `ingested_at DESC` (or by a business rule, such as preferring a traded player's combined row), and keep row 1. Reloads can then never double count.
- **Cast every field** with fallbacks (`coalesce(api_field, parsed_alternative)`, `try_cast`).
- **Rename in one dict** (`COLUMN_RENAMES`) to readable snake_case names without unit suffixes. Document the units once, in CLAUDE.md.
- **Flag, don't filter.** Silver keeps every row and adds booleans (`has_tracking`, `is_excluded_result`, `is_invalid_count`). Each gold table or model chooses its own filter, and nothing disappears silently.
- **Add a stable single-column key** when the natural key is composite (`pitch_id = game_pk*100000 + at_bat*100 + pitch`), with an expectation that it stays unique.
- **Put reconciliation checks in expectations,** for example `earned_runs <= runs`, or that the official starter flag equals `appearance_order = 1`.
- **Treat legacy rows explicitly,** for example rows landed before the `role` column existed are treated as `role = 'all'`.

**Gold:**
- **Build tables for the questions people ask,** not for the source. `gold_pitcher_game_summary` exists because users ask "was his velocity down last start?". Pick the grain from the question: per entity-period-category, here pitcher x game x pitch type.
- **Keep metric lists as config** at the top of the file (`SHAPE_METRICS = [(col, out_name, decimals)]`) and generate the aggregations in a loop. Adding a metric is then a one-line change.
- **Add comparison-to-baseline columns** (`*_vs_season`), because "is this unusual for them?" is the most common follow-up question.
- **Weighted averages, never averages of averages.** Aggregate from rows, or weight by the count.
- **When two definitions of a concept both matter, keep both and name them differently** (`starter_flag` is usage-based; `game_starter` is official). Document which consumer uses which one.

### 2.4 Models live outside the pipeline

Examples: `models/common.py`, `models/*.py`, `resources/models.job.yml`.

- Model notebooks run as their own job DAG, manually after the pipeline refreshes. Training is slow and occasional, and the pipeline stays fast and deterministic.
- **A shared `common.py`** holds the conventions: table names, `ml_name()`, a date-based holdout (`date_split`, out-of-time validation rather than a random split), and a `Standardizer` that is serialized and logged with the model, so scoring scales features exactly as training did.
- **Filter and sample in Spark, and call `.toPandas()` only on reduced data.** Score full tables in Spark (`mlflow.pyfunc.spark_udf`).
- **Serverless-only workspaces:** each notebook begins with `%pip install ...` and then `dbutils.library.restartPython()`. No ML runtime, no `spark.conf.set`, no RDDs or broadcast variables. ANSI mode is on, so guard every division.
- **Make evaluation its own read-only notebook** that writes `eval_*` tables and **appends a baseline snapshot with `snapshot_at`**. Every rebuild can then be compared with earlier runs ("did this change make things better?").
- **Compare against the official number.** Bring the authoritative figures into silver (here the official MLB stats) and report model outputs next to them.

### 2.5 The Genie space is code

Examples: `scripts/build_genie_space.py` generates `resources/genie/pitcher_analysis.geniespace.json`.

- **A local Python script is the source of truth.** The JSON is generated, committed and deployed by the bundle, and never edited by hand. Edits made in the UI must be copied back into the script, or the next regenerate overwrites them.
- **What the script contains:**
  - `TABLES`, a map from table to `{column: description}`. Column lists are fetched from Unity Catalog (`databricks tables get`), and the build **asserts that every described column still exists**, so renamed columns are caught.
  - `ENTITY`, the columns that get entity matching (names, categories).
  - `INSTRUCTIONS`, text instructions.
  - `EXAMPLES`, question to SQL pairs.
  - `SAMPLE_QUESTIONS`.
  - `BENCHMARKS`, question to SQL pairs used for accuracy evaluation.
- **Fixed, increasing ids** (a counter, rather than random UUIDs). Genie requires lists sorted by id, and regenerating gives a stable diff.
- **`--test` runs every example and benchmark query on the warehouse** through the statements API before you deploy.
- **Instruction structure that worked** (keep this order):
  1. **Required filters** first, in capitals: things that silently produce wrong answers if missed, such as multiple rows per entity by split or role.
  2. **Purpose and audience:** who asks, and in what language to answer.
  3. **Which table to use** for which kind of question.
  4. **Names and joins:** join on ids, never names; formats differ by table; key name differences such as `season` vs `game_year`.
  5. **Definitions** that have more than one meaning (roles).
  6. **Reading the numbers:** scales, the direction of "good", units, display formats.
  7. **Sample sizes:** default minimums for leaderboards; always show the sample; default to the latest period and say so.
- **Examples cover the main question types:** an overview of one entity, a breakdown, a leaderboard, a "last N events" question, and a "vs baseline" question.
- **Benchmarks return only the columns the question asks for.** The grader marks an answer wrong when it is missing a column the benchmark returns.
- **Accuracy loop:** deploy, run `databricks genie genie-create-eval-run <space_id>`, read the misses (`genie-list-eval-results`, `genie-get-eval-result-details`), fix the instructions or examples in the script, then regenerate, test and redeploy.

### 2.6 Working with Claude Code

- **CLAUDE.md is the living spec.** It records layout, conventions, every table's grain and meaning, the pipeline and job IDs, and the rules ("ask before changing tables, jobs or pipelines", "never commit or push"). It is updated in the same change as the code, so a new session starts with an accurate map. The section list in 5.1 is a good skeleton.
- **`docs/project-status.md` is the handoff:** where the work stands, the latest results, open items in priority order, and the job and run IDs. CLAUDE.md answers "how does it work?" and project-status answers "where are we?". Update it when items close.
- **Runbooks in `docs/`** for anything multi-step and rare, such as a backfill.
- **Decisions are written down with their reasons**, for example "MLB-only data from 2023 onward" or "drop BABIP if the no-BABIP fit predicts as well". Then they are not reopened in every session.
- **Plan first, then build in phases.** The model rebuild ran as numbered phases (0 to 9), each producing named tables and a checkpoint.
- **Coding subagents run on Sonnet** (memory: `sonnet-coding-agents`). The main session plans and reviews.

### 2.7 Documentation layers, kept in sync from one source

Baseball keeps table documentation in three hand-maintained places: CLAUDE.md, the `comment=` strings in the pipeline code, and the Genie script's `TABLES` dict. They are kept in line by discipline alone. The new project adds Unity Catalog wikis and Pages, which makes five places. So every piece of documentation gets **one home in the repo**, everything else is **generated** from it, and a **read-only check fails** whenever any copy differs.

**Rule of thumb: what goes where**

| Layer | Audience | Holds | Source of truth (in git) | Published by |
|---|---|---|---|---|
| `README.md` | People setting up or running the project | Setup, how to run, how to deploy | itself | GitHub |
| `CLAUDE.md` | You and Claude Code | How the project is built: layout, conventions, IDs, rules. Each table appears as **one line linking to its doc file**, never as its column list. | itself | not published |
| Table comment | Catalog search, `information_schema`, Genie | One or two sentences: what a row is (the grain) and what the table is for | `summary` in `docs/tables/<schema>.<table>.md` (copied verbatim into the pipeline's `comment=`; the check enforces that they match) | the pipeline |
| Column descriptions | Genie, the wiki | One line per column: meaning, units, sign or scale, allowed values | `columns` in the same doc file | the Genie build, plus the wiki's generated column table |
| UC wiki | Anyone querying the table | The table's README: grain, required filters, joins, how to read the numbers, caveats, lineage, related tables | the body of the same doc file | `scripts/docs.py publish` |
| UC Page | Business users, Genie One | One business term that spans tables (definitions, synonyms, related tables) | `docs/pages/<term>.md` | `scripts/docs.py publish` |
| Genie instructions | The Genie space | Answering rules: required filters, which table to use, sample sizes, examples, benchmarks | `scripts/build_genie_space.py` (column descriptions are read from the doc files, not retyped) | the bundle |
| `docs/project-status.md` | The next session | Where the work stands, open items | itself | not published |

**Anti-drift rules**

1. **Every fact has one home.** If something is needed in two layers, one is generated from the other. Nothing is retyped.
2. **One doc file per table** (`docs/tables/<schema>.<table>.md`, template in 5.5) for every silver, gold and ml table. It holds the summary, the grain, every column, whether the table is in Genie, and the wiki body. Raw and bronze tables get a doc file only if people query them.
3. **Change the code and its doc file together.** Adding, renaming or redefining a column updates the doc file in the same change. CLAUDE.md states this rule.
4. **Never edit wikis, Pages or the Genie space in the UI.** If someone does, copy the edit into the repo file and republish. Every generated wiki and Page ends with a line saying it is generated from `<file>` at `<commit>`, and to edit the file instead of the page.
5. **One script, three commands** (`scripts/docs.py`):
   - `check --local` is offline and runs before every commit. It checks that every doc file's front matter is valid; that each `summary` equals the comment of the matching dataset in the pipeline code (Python `comment=` parsed with `ast`, or the SQL `COMMENT '...'`); that every table named in CLAUDE.md has a doc file; and that the committed Genie JSON equals a fresh regeneration.
   - `check` is read-only against the workspace and runs after every deploy and pipeline refresh. It **exits non-zero** on any failure and prints each difference:
     - every table in the project schemas has a doc file, and every doc file's table exists;
     - every described column exists, and every column of a documented table has a description;
     - the live table comment equals `summary`;
     - every published wiki and Page equals the generated text (the generated-from footer is ignored);
     - every Page's related tables exist;
     - the Genie JSON is current, and every table it uses has a doc file with `genie: true`.
   - `publish` writes the wikis and Pages from the doc files. It writes to Unity Catalog, so it runs only after the user agrees.
     - **Wikis:** `PATCH /api/2.0/knowledge/tables/<catalog.schema.table>/page?update_mask=body,description,state` with `state = PUBLISHED`. The same call creates the wiki if it doesn't exist.
     - **Pages:** see "Pages API" below.
6. **Fixed order after a change:** edit the code and doc file → `check --local` → the user commits and pushes → `bundle deploy` → pipeline refresh → `docs.py publish` → `docs.py check` → `build_genie_space.py --test`. Nothing is done until `check` passes.
7. **Optional: a weekly read-only job task** runs `check`, to catch UI edits made between deploys. Code changes can only arrive through a deploy, so the deploy-time check already covers those.

**Pages API (checked 2026-11-25; Beta)**

Both APIs answer in this workspace. Domains returned only the built-in **General** domain, and there are no Pages yet. The facts that shape the design:

- **Endpoints**, base path `/api/2.0/knowledge`:
  - `POST /pages`: create. Optional `request_id` (a UUID) makes retries safe.
  - `GET /pages/{id}`: get.
  - `GET /pages?filter=domain = "domains/<id>"`: list one domain (subdomains included), 50 per page, with `page_token`.
  - `PATCH /pages/{id}?update_mask=...`: update. Fields: `domain`, `display_name`, `body`, `synonyms`, `description`, `state`, `owner_ids`.
  - `DELETE /pages/{id}`: delete.
  - Publishing is a `PATCH` with `state = PUBLISHED`. There is no separate publish call.
- **Every Page belongs to exactly one domain**, and `domain` is required on create. **Give the project its own domain** (for example `databricks-interview`) rather than using General, so `check` can list everything in that domain and flag Pages that have no repo file.
  - Creating a domain needs a governed tag with the same name, and `MANAGE DISCOVER` permission, which admins have.
  - It's a one-time setup step. Store the domain id as a `databricks.yml` variable.
- **Page ids are server-generated UUIDs.** You can't choose them. Published titles are unique within a domain, so `publish` matches repo files to Pages by `display_name` within the project domain. That means no id file to maintain.
  - A renamed title creates a new Page and orphans the old one. `check` reports the orphan, and `publish --prune` deletes it, after the user agrees.
- **`body` is replaced whole** on update, which suits generated content. **`synonyms`** are trimmed, deduplicated and sorted by the server, so `check` compares them sorted.
- **Owner:** `owner_ids` takes one principal id (not an email) and defaults to the caller.
- **Related tables and sources** use a separate Asset Link API, `.../pages/{id}/asset-link-categories/{SOURCE|RELATED_ASSETS}/asset-links`:
  - Create, get and delete only. Links can't be changed: delete and re-create instead.
  - **There is no list endpoint,** so `check` can't read back which tables a Page links to. Two mitigations:
    - `publish` also writes the related tables as a generated section of the body, which `check` can compare.
    - When building, test whether `GET /pages/{id}` returns the links. If it doesn't, `publish` rebuilds a Page's links only when the `related_tables` list in its repo file changes.
  - Table links use `asset_type = ASSET_TYPE_UC_TABLE`, the metastore UUID as `asset_scope_id`, and the full table name as `asset_id`.
- **Certified or Deprecated** status is set in the UI only. No Page API field covers it. Leave it out of the generated flow, or set it by hand and note it in the Page file.
- **Who reads Pages:** Genie One prioritizes a published Page's definition and cites it. The docs don't say that Genie spaces read Pages, so the Genie script still states key definitions itself.
- **No PII.** Page data is replicated globally.

**To verify when building it** (Beta features, or not yet tested):
- Whether `GET /pages/{id}` returns asset links (see above).
- Whether column comments set with `ALTER TABLE ... ALTER COLUMN ... COMMENT` survive a pipeline refresh on materialized views, or an `overwrite` on notebook-written ml tables. If they don't, column descriptions live only in Genie and the wiki. If they do, `publish` sets them too, and `check` compares them.
- Whether the Genie space reads wikis. It's not documented today, so the Genie script keeps its own instructions.

### 2.8 Databricks agent skills (`databricks aitools`)

Databricks publishes instruction files ("skills") for coding agents, installed with the Databricks CLI. Both repos install **the same eight skills at the same version, in project scope**, and **the skills win by default**: CLAUDE.md lists a few exceptions that only the user can lift (decided 2026-11-25).

**Why these eight.** They cover what both projects use: CLI basics, pipelines, bundles, serverless compute, jobs, Unity Catalog, metric views and Genie. Skipped: apps, Lakebase, model serving, vector search, GPU training, the dashboard, SQL and ingestion-connector skills, and the rest. Each skill's description is always in context, so unused skills cost tokens and add noise. Add a skill to both repos when a project starts needing it.

**Install** (from the repo root, once per clone):

```bash
databricks aitools install --agents claude-code --scope project --skills-only \
  --skills databricks-core,databricks-pipelines,databricks-dabs,databricks-serverless,databricks-jobs,databricks-unity-catalog,databricks-metric-views,databricks-genie-agents
```

- The files go to `.databricks/aitools/skills/` (already ignored through `.databricks/`), with links in `.claude/skills/`. `.gitignore` has `.claude/skills/databricks-*`, so they are never committed: the license allows use only with Databricks, and redistribution has to carry it. The README records the install command instead.
- `--skills-only` installs only skill files: no plugin hooks, and no `/databricks:setup` or `/databricks:doctor` commands.
- The install records the version and skill list in `.databricks/aitools/skills/.state.json`. `check_process_sync.py` compares that file with the pinned version and list below.

**Update** (deliberately, in both repos in the same session, never one alone):
1. `databricks aitools update --check` to see what would change.
2. Read the changed `SKILL.md` files for new conflicts with our process.
3. `databricks aitools update --no-new` in each repo. `--no-new` stops new skills from arriving unannounced.
4. Update the version in the shared block below, copy the block into both CLAUDE.md files, and run `python3 scripts/check_process_sync.py` in both repos.

**The shared CLAUDE.md block.** Both repos' CLAUDE.md files contain this block verbatim, including the marker comments. `check_process_sync.py` fails if either copy differs from this one. The exceptions came from reading the eight `SKILL.md` files at version 0.2.47 and asking which differences matter. They are kept because they protect against real problems: the ask-first guardrail, the Genie space having two owners (bundle and CLI), a second instruction file, being asked to pick a profile every session, and unrequested job changes. Every other difference follows the skill. On a skill update, recheck only these exceptions.

```markdown
<!-- shared-process:databricks-skills BEGIN -->
## Databricks agent skills

Databricks' agent skills are installed per project with `databricks aitools` (project scope, skills version **0.2.47**). The files are in `.databricks/aitools/skills/`, with links in `.claude/skills/`, and are not committed. The `baseball` and `databricks-interview` repos install the same skills at the same version. Install, update and sync steps are in `docs/databricks-project-playbook.md` section 2.8; `python3 scripts/check_process_sync.py` checks them.

Installed skills: `databricks-core`, `databricks-pipelines`, `databricks-dabs`, `databricks-serverless`, `databricks-jobs`, `databricks-unity-catalog`, `databricks-metric-views`, `databricks-genie-agents`.

**The skills win by default.** Follow a skill's guidance even where it differs from older habits or examples in this repo, except for the exceptions below, which only the user can lift. If the user says not to follow a skill in a specific case, do what the user says.

Exceptions (CLAUDE.md wins):
- **Ask first:** check with the user before deploying, before running jobs or pipelines that overwrite tables, and before changing tables, jobs, pipelines, grants, Genie spaces, wikis or Pages. Deploy only what was pushed. The serverless skill says to deploy, then report.
- **Genie:** the space is deployed only by the bundle, from `scripts/build_genie_space.py`. Never run `databricks genie create-space`, `update-space` or `trash-space`. Using the skill to query a space, run evals and diagnose misses is fine. Databricks now calls Genie spaces "Genie Agents".
- **One instruction file and our layout:** CLAUDE.md is the only agent instructions file. Never run `databricks bundle init`, and never create `AGENTS.md`. Keep the folder layout in the playbook (`ingest/`, `pipeline/transformations/<layer>/`, `pipeline/utilities/`, `models/`, `resources/`, `scripts/`, `docs/`).
- **Profile:** use the default CLI profile (the one `databricks auth profiles` marks as Default). Don't ask the user to pick one.
- **Job tags and compute mode:** don't add `aidevkit_project` tags (or any other tags), and don't add or change `performance_target` without asking.

Following the skills means, for example:
- **Pipelines:** an existing pipeline keeps its language. A new pipeline is SQL by default, and Python is fine when SQL can't do the job or fails (Python-only features, shared helpers in `pipeline/utilities/`, UDFs).
- **New datasets:** one dataset per file, named after the dataset. Use schema-qualified names only when publishing outside the pipeline's default schema, as the medallion layers do.
- **Bundles:** run `databricks bundle validate --strict -t prod` after configuration changes and before every deploy.
<!-- shared-process:databricks-skills END -->
```

### 2.9 Keeping both repos on one process

`baseball` and `databricks-interview` follow the same process. To keep them from drifting:

- **Shared, byte-identical files:** `docs/databricks-project-playbook.md` (this file) and `scripts/check_process_sync.py`.
- **Shared CLAUDE.md blocks:** each one sits between `<!-- shared-process:<name> BEGIN -->` and `<!-- shared-process:<name> END -->` markers. Its source copy is in this playbook, and CLAUDE.md holds it verbatim. There are two today, `databricks-skills` (2.8) and `working-rules` (below). Everything outside the markers is project-specific.
- **Shared skills:** the same list and version (2.8).
- **A change to the process** (this file, a shared block, the sync script or the skills) is made **in both repos in the same session**. Finish by running `python3 scripts/check_process_sync.py` in each repo. It finds the other repo next to this one (`~/Documents/GitHub/<name>`) or takes `--other <path>`, checks everything above, and exits non-zero on any difference. Until `databricks-interview` exists, it checks only the current repo.
- **Project-specific facts never go into a shared file.** The playbook may cite baseball files as examples, but tables, IDs, results and open items stay in each repo's CLAUDE.md and `docs/project-status.md`.
- **Adoption gaps are tracked, not hidden.** When one repo hasn't yet adopted part of the process (for example, baseball doesn't have the 2.7 documentation layers yet), the gap is an open item in that repo's `docs/project-status.md`.

```markdown
<!-- shared-process:working-rules BEGIN -->
## Working rules (shared with the sibling repo)

- The user commits and pushes. Never commit or push.
- Check with the user before changing anything in Databricks: deploying, running jobs or pipelines that overwrite tables, or changing tables, jobs, pipelines, grants, Genie spaces, wikis or Pages.
- Deploy only when the local tree matches the pushed commit. Check the workspace Git folder's commit with `databricks repos get <git folder> -o json`. Run `databricks bundle validate --strict -t prod` before every deploy.
- Never edit code, Genie spaces, wikis or Pages in the workspace UI. Change the repo, then redeploy or republish.
- Coding subagents run on Sonnet.
- The shared process (`docs/databricks-project-playbook.md`, the shared blocks in CLAUDE.md, `scripts/check_process_sync.py` and the installed Databricks skills) changes in both repos in the same change. Run `python3 scripts/check_process_sync.py` afterwards. Project-specific facts stay out of the shared files.
<!-- shared-process:working-rules END -->
```

---

## 3. Problems we hit (check for these early in the new project)

| Problem | What to do |
|---|---|
| Python 3.15 breaks `databricks-sql-connector` | Use Python 3.12 for the local venv (README section 2.3). |
| Serverless-only workspace | Use `%pip` in every notebook. No cluster configs, `spark.conf.set`, RDDs or broadcast variables. |
| ANSI mode errors on divide-by-zero | Use `when(den > 0, num / den)` or `try_divide`. |
| pandas nulls lost on conversion | Convert through pyarrow (2.2). |
| An API changes or adds fields | Land as strings with `mergeSchema`, and type in silver. |
| Late or corrected source records | Use a lookback window plus `replaceWhere`, and dedupe in silver on `ingested_at`. |
| A record's "date" has two meanings (listing date vs official date) | Keep both columns, partition raw by the one the window uses, and expose the business date in silver. |
| Past data computed differently from current data (Statcast's historical plate locations come from the fit) | Check input distributions per period before trusting an out-of-time holdout (open item 1 in `project-status.md`). |
| Names stored in different formats in different tables | Join on ids, and tell Genie which format each table uses. |
| Multiple rows per entity (splits, roles) give wrong Genie answers | Add a "REQUIRED FILTERS" instruction, and put a QUALIFY example in the instructions. |
| Genie UI edits lost | The script is the source; copy UI edits back into it. |
| Genie benchmark marked wrong despite a correct answer | Trim benchmark SQL to the columns the question asks for. |
| Helper module picked up as a pipeline dataset | Put it in `pipeline/utilities/`, not under `transformations/`. |
| Table docs copied into CLAUDE.md, pipeline comments and the Genie script, kept in line only by hand | One doc file per table, everything generated from it, plus `docs.py check` (2.7). |

---

## 4. New-project checklist

**Phase 0: frame the problem (before any code)**
1. Write the **10–20 business questions** the Genie space must answer, and who asks them. They decide the gold tables and the Genie benchmarks.
2. List the sources, and for each one: access method, auth (secrets go in `dbutils.secrets`), grain, update cadence, how late data arrives, and the history needed.
3. Write down the **key decisions**: date range, exclusions, the official definitions of contested concepts, and which "official" numbers to reconcile against.

**Phase 1: scaffold (about an hour, mostly copying)**
4. Create the `databricks-interview` repo (for example `~/Documents/GitHub/databricks-interview`) with `.gitignore` (copy this one), a Python 3.12 venv, `.env`, and `test_connection.py` (README sections 2 and 8). Add a Databricks Git folder for it at `/Users/<you>/databricks-interview`.
5. Copy `databricks.yml` and set `bundle.name: databricks-interview` (the bundle then deploys to `/Workspace/Users/<you>/.bundle/databricks-interview/prod/files/`, separate from baseball's). Change the `catalog` variable to the new project's catalog, and set `warehouse_id` (it can be the same warehouse). Rename the resource keys and display names in `resources/*.yml`, such as `statcast_medallion_pipeline` and `Statcast Daily Ingest & Medallion Refresh`, so the two projects' jobs and pipelines don't look alike in the workspace.
6. Create the catalog and schemas: `raw`, `bronze`, `silver`, `gold`, plus `ml` if needed. Create the project's Discover domain (a governed tag first, then the domain) for its Pages, and store its id as a `databricks.yml` variable (2.7, "Pages API").
7. Write CLAUDE.md from the skeleton in 5.1, including the shared blocks from 2.8 and 2.9 pasted verbatim, and write `docs/project-status.md` with Phase 0's decisions.
7a. Copy the shared files from `baseball` unchanged: `docs/databricks-project-playbook.md` and `scripts/check_process_sync.py`. Install the Databricks skills with the command in 2.8, add `.claude/skills/databricks-*` to `.gitignore`, and put the install command in the README. Run `python3 scripts/check_process_sync.py`; it must pass in both repos.
8. Do **not** copy the baseball code (`ingest/`, `pipeline/`, `models/` contents, the Genie JSON). Copy only the folder layout and the templates in section 5: `ingest/`, `pipeline/transformations/{bronze,silver,gold}/`, `pipeline/utilities/`, `resources/`, `scripts/`, `docs/`.

**Phase 2: raw to silver for one source**
9. Write the ingest notebook from 5.2. Run one small window by hand, then rerun it to check that the row count doesn't change (idempotency).
10. Write the bronze and silver files from 5.3. Silver states its grain, dedupes, casts, renames and flags. Use SQL unless SQL can't do the job (2.8). Write each silver table's doc file (5.5) in the same change, and copy its `summary` into the dataset's comment.
10a. Build `scripts/docs.py` (2.7) once the first silver table exists, so every later table goes through `check --local`, `publish` and `check` from the start.
11. Add the job (`resources/<project>.job.yml`, paused) and the pipeline yml. Deploy to `dev` first, then to `prod`.
12. Backfill history with a runbook in `docs/`.

**Phase 3: gold for the questions**
13. For each question from step 1, name the table and grain that answers it in one simple query. Build those gold tables: metric lists as config, baseline comparisons, both definitions where two exist. Each table gets a doc file. Each business term that spans tables (from Phase 0's decisions) gets a Page file in `docs/pages/`.

**Phase 4: Genie**
14. Copy `scripts/build_genie_space.py`. Replace `TABLES` with a loader that reads the doc files with `genie: true` (table list and column descriptions), so column text is never retyped. Replace `OUT`, `WH`, `ENTITY`, `INSTRUCTIONS`, `EXAMPLES`, `SAMPLE_QUESTIONS` and `BENCHMARKS`, keeping the instruction order from 2.5. Add a `resources/<space>.genie.yml`.
15. Run `--test`, then regenerate, deploy, `docs.py publish`, `docs.py check`, and run an eval. Iterate on the misses.

**Phase 5 (optional): models**
16. Add `models/common.py` (date holdout, standardizer, `ml_name`), the model notebooks, a model job DAG, and an evaluation notebook with baseline snapshots. Add the `ml` tables to Genie when they are ready.

**Then:** unpause the schedule. Keep CLAUDE.md, the doc files and project-status.md current with every change, and treat a failing `docs.py check` like a failing test.

---

## 5. Starter templates

### 5.1 CLAUDE.md skeleton

```markdown
# CLAUDE.md

## Project overview
<one paragraph: business problem, sources, users>
<folder tree: ingest/, pipeline/transformations/{bronze,silver,gold}/, pipeline/utilities/, models/, resources/, scripts/, docs/>
- **<Source> ingest**: `ingest/<source>_ingest.py`: endpoint, widgets and defaults, window and lookback, raw table, write mode.
- **<Source> silver**: `silver.silver_<source>`: exact grain ("one row per X-Y"), columns, flags, reconciliation checks.
- **Tables**: one line per table, linking to its doc file (`docs/tables/<schema>.<table>.md`). Never copy the column list here.
(How to add a source: files, naming, job task, doc file.)

## Documentation (no drift)
- Each fact has one home; see the layer table in docs/databricks-project-playbook.md section 2.7.
- Every silver/gold/ml table has a doc file. A change to a table's columns or meaning updates its doc file in the same change, and its `summary` is copied verbatim into the pipeline `comment=`.
- Wikis, Pages and the Genie space are generated. Never edit them in the UI; if someone does, copy the edit into the repo file and republish.
- Before commit: `python3 scripts/docs.py check --local`. After deploy and pipeline refresh: `docs.py publish` (ask the user first), then `docs.py check`. A failing check is a failing test.

## Genie space
Source of truth is scripts/build_genie_space.py (column descriptions come from the doc files); regenerate, --test, never hand-edit the JSON; UI edits are not synced back; eval loop.

## Language and libraries
Python; Spark when it helps, pandas for small data; pyarrow conversion; no new dependencies without need.

## Conventions
Catalog and schema layout, `<schema>.<table>` inside the pipeline, materialized_view + comment + expect,
snake_case names without unit suffixes, units list, idempotent replaceWhere writes, widgets with defaults,
Databricks notebook source markers.

## Syncing with Databricks
Bundle paths, pipeline and job IDs, deploy commands. (The general rules are in the working-rules block; don't repeat them here.)

## Security and data
Secrets in dbutils.secrets / .env; data files gitignored.

<paste the shared blocks from playbook 2.9 (working-rules) and 2.8 (databricks-skills) here, verbatim, markers included>
```

Once a repo has built `scripts/docs.py`, its "Documentation (no drift)" section becomes a third shared block. It stays project-specific until both repos have adopted 2.7.

### 5.2 Ingest notebook skeleton (REST source, date-windowed)

Start from `ingest/mlb_boxscore_ingest.py`. Keep these parts and replace the endpoint and flattening:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # <Source> ingest -> <catalog>.<raw_schema>.<source>_raw (what, grain, window, idempotency)

# COMMAND ----------
from datetime import date, datetime, timedelta, timezone
import pandas as pd, pyarrow as pa, requests, time

LOOKBACK_DAYS = 3
dbutils.widgets.text("start_date", "", "Start date (blank = 3 days ago)")
dbutils.widgets.text("end_date", "", "End date (blank = yesterday)")
dbutils.widgets.text("catalog", "<catalog>", "Catalog")
dbutils.widgets.text("raw_schema", "raw", "Raw schema")

def _get_param(name, default):
    try:
        return dbutils.widgets.get(name).strip() or default
    except Exception:
        return default

# COMMAND ----------
def get_json(url, params=None, retries=5, timeout=30): ...   # backoff on 429/5xx
def month_windows(start, end): ...                         # calendar-month chunks
def fetch_window(ws, we): ...                              # -> list[dict], one per record at the raw grain

# COMMAND ----------
for ws, we in month_windows(start_date, end_date):
    rows = fetch_window(ws, we)
    if not rows:
        continue
    df = pd.DataFrame(rows)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    # fields as strings; keys typed; window column as DATE
    sdf = spark.createDataFrame(pa.Table.from_pandas(df, preserve_index=False))
    (sdf.write.mode("overwrite")
        .option("replaceWhere", f"event_date >= '{ws}' AND event_date <= '{we}'")
        .option("mergeSchema", "true")
        .saveAsTable(table_name))
```

### 5.3 Bronze and silver skeleton

New pipelines are SQL by default (2.8): one dataset per file, named after the dataset. Read the `databricks-pipelines` skill's SQL references before writing them. The SQL versions:

```sql
-- pipeline/transformations/bronze/bronze_<source>.sql
CREATE OR REFRESH MATERIALIZED VIEW bronze.bronze_<source> (
  CONSTRAINT valid_id EXPECT (<id> IS NOT NULL),
  CONSTRAINT valid_date EXPECT (event_date IS NOT NULL)
)
COMMENT 'Raw <source> records as landed by ingest/<source>_ingest.py.'
AS SELECT * FROM raw.<source>_raw;
```

```sql
-- pipeline/transformations/silver/silver_<source>.sql
CREATE OR REFRESH MATERIALIZED VIEW silver.silver_<source> (
  CONSTRAINT valid_id EXPECT (<id> IS NOT NULL),
  CONSTRAINT <reconciliation_rule> EXPECT (<sql predicate>)
)
COMMENT 'Exactly one row per <grain>. <meaning of key columns, flags>.'
AS SELECT
  CAST(<id> AS BIGINT) AS <id>,
  CAST(event_date AS DATE) AS event_date,
  CAST(apiFieldName AS DOUBLE) AS readable_name,   -- renames live in the SELECT
  status = 'VOID' AS is_void                       -- flags, not filters
FROM bronze.bronze_<source>
QUALIFY ROW_NUMBER() OVER (PARTITION BY <id>, event_date ORDER BY ingested_at DESC NULLS LAST) = 1;
```

The Python versions, for when SQL can't do the job (Python-only features, shared helpers in `pipeline/utilities/`, UDFs), or for an existing Python pipeline such as baseball's:

```python
# pipeline/transformations/bronze/<source>.py
from pyspark import pipelines as dp

@dp.materialized_view(name="bronze.bronze_<source>", comment="Raw <source> records as landed by ingest/<source>_ingest.py.")
@dp.expect("valid_id", "<id> IS NOT NULL")
@dp.expect("valid_date", "event_date IS NOT NULL")
def bronze_<source>():
    return spark.read.table("raw.<source>_raw")
```

```python
# pipeline/transformations/silver/<source>.py
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

KEY = ["<id>", "event_date"]
COLUMN_RENAMES = {"apiFieldName": "readable_name"}

@dp.materialized_view(name="silver.silver_<source>", comment="Exactly one row per <grain>. <meaning of key columns, flags>.")
@dp.expect("valid_id", "<id> IS NOT NULL")
@dp.expect("<reconciliation_rule>", "<sql predicate>")
def silver_<source>():
    raw = spark.read.table("bronze.bronze_<source>")
    w = Window.partitionBy(*KEY).orderBy(col("ingested_at").desc_nulls_last())
    one = raw.withColumn("_rn", row_number().over(w)).where("_rn = 1").drop("_rn")
    for old, new in COLUMN_RENAMES.items():
        one = one.withColumnRenamed(old, new)
    return one.select(
        col("<id>").cast("bigint"),
        col("event_date").cast("date"),
        col("amount").cast("double"),
        # flags, not filters:
        (col("status") == "VOID").alias("is_void"),
    )
```

### 5.4 Genie script

Copy `scripts/build_genie_space.py` as is. Only the constants change: `OUT`, `WH`, `TABLES`, `ENTITY`, `INSTRUCTIONS`, `EXAMPLES`, `SAMPLE_QUESTIONS`, `BENCHMARKS`. Change the starting id constant in `_id` too, so ids from different spaces never collide. `table_columns`, `run_sql`, `build()` and `--test` work unchanged.

### 5.5 Table doc file (`docs/tables/<schema>.<table>.md`)

The front matter feeds the table comment, Genie and the checks. The body becomes the wiki. The wiki's column table and its generated-from footer are added by `publish`, so they aren't written here.

```markdown
---
table: gold.gold_customer_month          # <schema>.<table>; the catalog comes from databricks.yml
summary: >-
  One row per customer and calendar month: revenue, orders and churn flags for active customers.
grain: [customer_id, month]
genie: true
entity_columns: [customer_name, segment]  # Genie entity matching
columns:
  customer_id: Customer key. Join to every customer table on this, never on name.
  month: First day of the calendar month (date).
  revenue: Net revenue in USD after refunds, for the month.
  is_churned: True when the customer had no orders in this month or the next two.
related_pages: [active-customer, net-revenue]
---

## How to use it
- Required filters: ...
- Joins: ...

## How to read the numbers
...

## Caveats
...

## Lineage
Built by `pipeline/transformations/gold/customer_month.py` from `silver.silver_orders`.
```

### 5.6 Page file (`docs/pages/<term>.md`)

```markdown
---
title: Active customer                   # display_name; unique per domain, used to match the live Page
domain: databricks-interview             # the project domain (or a subdomain of it)
synonyms: [active account, live customer]
related_tables: [gold.gold_customer_month, silver.silver_customers]
---

A customer with at least one paid order in the trailing 90 days. ...
```
