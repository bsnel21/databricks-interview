# Databricks project playbook: concepts and build plan

This playbook holds the concepts and patterns for building a Databricks data project (ingest, medallion pipeline, tables, models, Genie space, documentation), the problems to expect and their fixes, the working rules, and a phased plan with starter templates. It is this repo's guide: the project itself is not started, and the business questions and data are decided in Phase 0 (section 4).

**How to use it:** work through section 4 (the plan) in order, using section 5 for the starter files. Sections 2 and 3 explain why each step is done that way. Project-specific facts (tables, IDs, results, open items) belong in CLAUDE.md and `docs/project-status.md`, not here.

---

## 1. The shape of the system

```
source API / files
   |  ingest notebook (one per source, parameterized, idempotent)
   v
<catalog>.raw.<source>_raw            landing table, values kept as returned
   |  one Spark Declarative Pipeline (bronze -> silver -> gold)
   v
bronze.bronze_<source>                raw + basic expectations, no logic
silver.silver_<source>[_<dataset>]    one row per stated grain: typed, renamed, deduplicated, flagged
gold.gold_<topic>                     tables shaped around the questions people ask; may join sources
   |  model notebooks (separate job, outside the pipeline)
   v
ml.<model outputs>                    scores, forecasts, evaluation tables
   |
   v
Genie space (defined as code)         plain-language questions over a curated set of tables
```

The pieces deploy together as one **Declarative Automation Bundle** (formerly Databricks Asset Bundle):

| Piece | Typical file | What it does |
|---|---|---|
| Bundle root | `databricks.yml` | variables (catalog, schemas, warehouse), dev/prod targets |
| Ingest + refresh job | `resources/<project>.job.yml` | parallel ingest tasks, then the pipeline task; schedule starts paused |
| Pipeline | `resources/<project>.pipeline.yml` | serverless pipeline, `root_path: ../pipeline`, glob over `transformations/**` |
| Model job | `resources/models.job.yml` | DAG of model notebooks on serverless, run manually |
| Genie space | `resources/<space>.genie.yml` plus the generated JSON | space deployed with the bundle |

---

## 2. Patterns, and how to apply them

### 2.1 Git is the source of truth, and the bundle is the deploy unit

- Every job, pipeline and Genie space is defined in `resources/*.yml`. Nothing is created by clicking in the UI, so a workspace can be rebuilt with `databricks bundle deploy -t prod`.
- The `dev` target (`mode: development`) prefixes resources with `[dev <user>]` and pauses schedules, so experiments never touch production.
- Variables in `databricks.yml` (`catalog`, `raw_schema`, `default_schema`, `warehouse_id`) are passed to notebooks as `base_parameters`, so the same code runs in any catalog.
- New schedules start `pause_status: PAUSED`. Unpause after the first manual run succeeds.
- **Workflow:** edit locally, the user commits and pushes, then run `databricks bundle validate --strict -t prod` and deploy from a tree that matches the pushed commit. A quick check: `databricks repos get <git-folder> -o json` shows the commit the workspace Git folder is on; compare it with local `HEAD`.
- Never edit code in the workspace. Edits there drift from git and are overwritten on the next deploy.

### 2.2 Ingest notebooks: parameterized, windowed, idempotent

One notebook per source, whether the source is a library that returns pandas, a REST API read per period, or a REST API read per entity on a thread pool.

- **Widgets with blank defaults.** `dbutils.widgets.text("start_date", "")`, where blank means the scheduled default (yesterday, or the last N days). The same notebook then serves the daily job, an ad hoc rerun and a backfill. Read widgets through a `_get_param(name, default)` helper so a missing widget falls back to the default.
- **A lookback window for late data.** Reload the last few days every run, so late-arriving or corrected records are picked up for free.
- **Chunk long ranges** (monthly windows), and write each chunk as it finishes, so a failed backfill resumes from where it stopped.
- **Idempotent writes:** `mode("overwrite")` with `replaceWhere` on the window column (`event_date >= X AND event_date <= Y`), never `append`. Reruns replace data instead of duplicating it. The `replaceWhere` column must be a real `DATE`, not a string.
- **Land values as returned, as strings,** plus `mergeSchema = true`. New or renamed API fields never break the ingest. Silver does the typing. Keep keys typed (ids as `bigint`, dates as `date`).
- **Stamp `ingested_at`,** so silver can keep the latest copy of a record.
- **Resilient HTTP:** timeouts, retries with exponential backoff on 429 and 5xx, a small thread pool (8 workers), and a generic User-Agent.
- **pandas to Spark through pyarrow** (`spark.createDataFrame(pa.Table.from_pandas(df, preserve_index=False))`), so nullable pandas dtypes keep their nulls.
- **One job task per source,** all running in parallel, with the pipeline task depending on every one of them.
- **Write a backfill runbook** in `docs/`: the exact parameters, the chunk sizes and how to verify the result.

### 2.3 One Spark Declarative Pipeline for bronze, silver and gold

- **One pipeline for every source,** so gold tables can join sources and read other gold tables, with the dependency order handled automatically.
- **`root_path` plus a glob include.** Every file under `transformations/` becomes a dataset source automatically. Shared helpers go in `pipeline/utilities/` and are imported as `from utilities.x import y`. Helpers must not live under `transformations/`, or they are treated as datasets.
- **File layout:** files go under `transformations/<layer>/`. Follow the `databricks-pipelines` skill: one dataset per file, named after the dataset, SQL by default, and Python when SQL can't do the job (2.8).
- **Every dataset** has a comment and expectations: in Python, `@dp.materialized_view(name="<schema>.<table>", comment=...)` with `@dp.expect(...)`; in SQL, `CREATE OR REFRESH MATERIALIZED VIEW <schema>.<table> (CONSTRAINT ... EXPECT (...)) COMMENT '...' AS ...`. Use schema-qualified names when a dataset publishes outside the pipeline's default schema, as the medallion layers do. The comment states the **grain** ("exactly one row per customer-month") and how the table relates to other tables. Comments appear in Unity Catalog and help Genie.
- **Catalog layout:** one catalog per project, one schema per layer (`raw`, `bronze`, `silver`, `gold`, `ml`). Table names are `<layer>_<source>[_<dataset>]`.

**Bronze:** read the raw table, drop junk columns, add NOT NULL expectations on the keys. No business logic.

**Silver:**
- **Enforce the grain explicitly.** Use `row_number()` over the natural key, ordered by `ingested_at DESC` (or by a business rule, such as preferring a combined row over its parts), and keep row 1. Reloads can then never double count.
- **Cast every field** with fallbacks (`coalesce(api_field, parsed_alternative)`, `try_cast`).
- **Rename in one dict** (`COLUMN_RENAMES`) to readable snake_case names without unit suffixes. Document the units once, in CLAUDE.md.
- **Flag, don't filter.** Silver keeps every row and adds booleans (`is_void`, `is_test_order`, `is_invalid_quantity`). Each gold table or model chooses its own filter, and nothing disappears silently.
- **Add a stable single-column key** when the natural key is composite (for example `line_id = order_id*1000 + line_number`), with an expectation that it stays unique.
- **Put reconciliation checks in expectations,** for example `discount <= gross_amount`, or that a source's total equals the sum of its parts.
- **Treat legacy rows explicitly,** for example rows landed before a `channel` column existed are treated as `channel = 'unknown'`.

**Gold:**
- **Build tables for the questions people ask,** not for the source. A `gold_customer_month` table exists because users ask "did this customer's spend drop last month?". Pick the grain from the question: per entity-period-category.
- **Keep metric lists as config** at the top of the file (`METRICS = [(col, out_name, decimals)]`) and generate the aggregations in a loop. Adding a metric is then a one-line change.
- **Add comparison-to-baseline columns** (`*_vs_average`), because "is this unusual for them?" is the most common follow-up question.
- **Weighted averages, never averages of averages.** Aggregate from rows, or weight by the count.
- **When two definitions of a concept both matter, keep both and name them differently** (for example `is_active_usage` is usage-based and `is_active_official` follows the business definition). Document which consumer uses which one.

### 2.4 Models live outside the pipeline

- Model notebooks run as their own job DAG, manually after the pipeline refreshes. Training is slow and occasional, and the pipeline stays fast and deterministic.
- **A shared `models/common.py`** holds the conventions: table names, `ml_name()`, a date-based holdout (out-of-time validation rather than a random split), and a standardizer that is serialized and logged with the model, so scoring scales features exactly as training did.
- **Filter and sample in Spark, and call `.toPandas()` only on reduced data.** Score full tables in Spark (`mlflow.pyfunc.spark_udf`).
- **Serverless-only workspaces:** each notebook begins with `%pip install ...` and then `dbutils.library.restartPython()`. No ML runtime, no `spark.conf.set`, no RDDs or broadcast variables. ANSI mode is on, so guard every division.
- **Make evaluation its own read-only notebook** that writes `eval_*` tables and **appends a baseline snapshot with `snapshot_at`**. Every rebuild can then be compared with earlier runs ("did this change make things better?").
- **Compare against the authoritative number.** Bring the official figures into silver and report model outputs next to them.

### 2.5 The Genie space is code

- **A local Python script (`scripts/build_genie_space.py`) is the source of truth.** The JSON it generates is committed and deployed by the bundle, and never edited by hand. Edits made in the UI must be copied back into the script, or the next regenerate overwrites them.
- **What the script contains:**
  - `TABLES`, a map from table to `{column: description}`. Column lists are fetched from Unity Catalog (`databricks tables get`), and the build **asserts that every described column still exists**, so renamed columns are caught.
  - `ENTITY`, the columns that get entity matching (names, categories).
  - `INSTRUCTIONS`, text instructions.
  - `EXAMPLES`, question to SQL pairs.
  - `SAMPLE_QUESTIONS`.
  - `BENCHMARKS`, question to SQL pairs used for accuracy evaluation.
- **Fixed, increasing ids** (a counter, rather than random UUIDs). Genie requires lists sorted by id, and regenerating gives a stable diff.
- **`--test` runs every example and benchmark query on the warehouse** through the statements API before you deploy.
- **Instruction structure** (keep this order):
  1. **Required filters** first, in capitals: things that silently produce wrong answers if missed, such as multiple rows per entity by split or role.
  2. **Purpose and audience:** who asks, and in what language to answer.
  3. **Which table to use** for which kind of question.
  4. **Names and joins:** join on ids, never names; formats differ by table; key name differences such as `season` vs `year`.
  5. **Definitions** that have more than one meaning.
  6. **Reading the numbers:** scales, the direction of "good", units, display formats.
  7. **Sample sizes:** default minimums for leaderboards; always show the sample; default to the latest period and say so.
- **Examples cover the main question types:** an overview of one entity, a breakdown, a leaderboard, a "last N events" question, and a "vs baseline" question.
- **Benchmarks return only the columns the question asks for.** The grader marks an answer wrong when it is missing a column the benchmark returns.
- **Accuracy loop:** deploy, run `databricks genie genie-create-eval-run <space_id>`, read the misses (`genie-list-eval-results`, `genie-get-eval-result-details`), fix the instructions or examples in the script, then regenerate, test and redeploy.

### 2.6 Working with Claude Code

- **CLAUDE.md is the living spec.** It records layout, conventions, every table's grain and meaning, the pipeline and job IDs, and the rules ("ask before changing tables, jobs or pipelines", "never commit or push"). It is updated in the same change as the code, so a new session starts with an accurate map. The section list in 5.1 is a good skeleton.
- **`docs/project-status.md` is the handoff:** where the work stands, the latest results, open items in priority order, and the job and run IDs. CLAUDE.md answers "how does it work?" and project-status answers "where are we?". Update it when items close.
- **Runbooks in `docs/`** for anything multi-step and rare, such as a backfill.
- **Decisions are written down with their reasons**, for example "data from 2023 onward only" or "drop a predictor if the fit without it predicts as well". Then they are not reopened in every session.
- **Plan first, then build in numbered phases,** each producing named tables and a checkpoint.
- **Coding subagents run on Sonnet.** The main session plans and reviews.

### 2.7 Documentation layers, kept in sync from one source

Table documentation naturally lands in many places: CLAUDE.md, the `comment=` strings in the pipeline code, the Genie script's `TABLES` dict, and Unity Catalog wikis and Pages. Kept by hand, they drift. So every piece of documentation gets **one home in the repo**, everything else is **generated** from it, and a **read-only check fails** whenever any copy differs.

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
- **Every Page belongs to exactly one domain**, and `domain` is required on create. **Give the project its own domain** (for example the project name) rather than using General, so `check` can list everything in that domain and flag Pages that have no repo file.
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

Databricks publishes instruction files ("skills") for coding agents, installed with the Databricks CLI. Install **the same eight skills at one pinned version, in project scope**, and **let the skills win by default**: CLAUDE.md lists a few exceptions that only the user can lift.

**Why these eight.** They cover what a project like this uses: CLI basics, pipelines, bundles, serverless compute, jobs, Unity Catalog, metric views and Genie. Skip apps, Lakebase, model serving, vector search, GPU training, the dashboard, SQL and ingestion-connector skills, and the rest until a project needs them. Each skill's description is always in context, so unused skills cost tokens and add noise.

**Install** (from the repo root, once per clone):

```bash
databricks aitools install --agents claude-code --scope project --skills-only \
  --skills databricks-core,databricks-pipelines,databricks-dabs,databricks-serverless,databricks-jobs,databricks-unity-catalog,databricks-metric-views,databricks-genie-agents
```

- The files go to `.databricks/aitools/skills/` (already ignored through `.databricks/`), with links in `.claude/skills/`. `.gitignore` has `.claude/skills/databricks-*`, so they are never committed: the license allows use only with Databricks, and redistribution has to carry it. The README records the install command instead.
- `--skills-only` installs only skill files: no plugin hooks, and no `/databricks:setup` or `/databricks:doctor` commands.
- The install records the version and skill list in `.databricks/aitools/skills/.state.json`.

**Update** (deliberately):
1. `databricks aitools update --check` to see what would change.
2. Read the changed `SKILL.md` files for new conflicts with the exceptions below.
3. `databricks aitools update --no-new`. `--no-new` stops new skills from arriving unannounced.
4. Update the version in the CLAUDE.md skills section and in this playbook.

**The CLAUDE.md skills block** (version 0.2.47). The exceptions came from reading the eight `SKILL.md` files at that version and asking which differences matter. They are kept because they protect against real problems: the ask-first guardrail, the Genie space having two owners (bundle and CLI), a second instruction file, being asked to pick a profile every session, and unrequested job changes. Every other difference follows the skill. On a skill update, recheck only these exceptions.

```markdown
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
```

### 2.9 Working rules

These go in CLAUDE.md verbatim (adjust only if the user's preferences change):

```markdown
## Working rules

- The user commits and pushes. Never commit or push.
- Check with the user before changing anything in Databricks: deploying, running jobs or pipelines that overwrite tables, or changing tables, jobs, pipelines, grants, Genie spaces, wikis or Pages.
- Deploy only when the local tree matches the pushed commit. Check the workspace Git folder's commit with `databricks repos get <git folder> -o json`. Run `databricks bundle validate --strict -t prod` before every deploy.
- Never edit code, Genie spaces, wikis or Pages in the workspace UI. Change the repo, then redeploy or republish.
- Coding subagents run on Sonnet.
```

---

## 3. Problems to expect (check for these early)

| Problem | What to do |
|---|---|
| Python 3.15 breaks `databricks-sql-connector` | Use Python 3.12 for the local venv. |
| Serverless-only workspace | Use `%pip` in every notebook. No cluster configs, `spark.conf.set`, RDDs or broadcast variables. |
| ANSI mode errors on divide-by-zero | Use `when(den > 0, num / den)` or `try_divide`. |
| pandas nulls lost on conversion | Convert through pyarrow (2.2). |
| An API changes or adds fields | Land as strings with `mergeSchema`, and type in silver. |
| Late or corrected source records | Use a lookback window plus `replaceWhere`, and dedupe in silver on `ingested_at`. |
| A record's "date" has two meanings (listing date vs business date) | Keep both columns, partition raw by the one the window uses, and expose the business date in silver. |
| Past data computed differently from current data (for example a source backfills history from a newer method) | Check input distributions per period before trusting an out-of-time holdout. |
| Names stored in different formats in different tables | Join on ids, and tell Genie which format each table uses. |
| Multiple rows per entity (splits, roles) give wrong Genie answers | Add a "REQUIRED FILTERS" instruction, and put a QUALIFY example in the instructions. |
| Genie UI edits lost | The script is the source; copy UI edits back into it. |
| Genie benchmark marked wrong despite a correct answer | Trim benchmark SQL to the columns the question asks for. |
| Helper module picked up as a pipeline dataset | Put it in `pipeline/utilities/`, not under `transformations/`. |
| Table docs copied into CLAUDE.md, pipeline comments and the Genie script, kept in line only by hand | One doc file per table, everything generated from it, plus `docs.py check` (2.7). |

---

## 4. Build plan (checklist)

**Phase 0: frame the problem (before any code)**
1. Write the **10-20 business questions** the Genie space must answer, and who asks them. They decide the gold tables and the Genie benchmarks.
2. List the sources, and for each one: access method, auth (secrets go in `dbutils.secrets`), grain, update cadence, how late data arrives, and the history needed.
3. Write down the **key decisions**: date range, exclusions, the official definitions of contested concepts, and which "official" numbers to reconcile against.

**Phase 1: scaffold (about an hour, mostly copying templates)**
4. Set up the local environment (README): `.gitignore`, a Python 3.12 venv, `.env`, and the Databricks CLI. Add a Databricks Git folder for the repo at `/Users/<you>/<repo>`.
5. Write `databricks.yml` with `bundle.name`, a `catalog` variable, `warehouse_id`, and dev/prod targets. Give resource keys and display names in `resources/*.yml` project-specific names.
6. Create the catalog and schemas: `raw`, `bronze`, `silver`, `gold`, plus `ml` if needed. Create the project's Discover domain (a governed tag first, then the domain) for its Pages, and store its id as a `databricks.yml` variable (2.7, "Pages API").
7. Fill in CLAUDE.md from the skeleton in 5.1 (including the skills block from 2.8 and the working rules from 2.9), and write `docs/project-status.md` with Phase 0's decisions. Install the Databricks skills with the command in 2.8.
8. Create the folder layout from section 5: `ingest/`, `pipeline/transformations/{bronze,silver,gold}/`, `pipeline/utilities/`, `resources/`, `scripts/`, `docs/`.

**Phase 2: raw to silver for one source**
9. Write the ingest notebook from 5.2. Run one small window by hand, then rerun it to check that the row count doesn't change (idempotency).
10. Write the bronze and silver files from 5.3. Silver states its grain, dedupes, casts, renames and flags. Use SQL unless SQL can't do the job (2.8). Write each silver table's doc file (5.5) in the same change, and copy its `summary` into the dataset's comment.
10a. Build `scripts/docs.py` (2.7) once the first silver table exists, so every later table goes through `check --local`, `publish` and `check` from the start.
11. Add the job (`resources/<project>.job.yml`, paused) and the pipeline yml. Deploy to `dev` first, then to `prod`.
12. Backfill history with a runbook in `docs/`.

**Phase 3: gold for the questions**
13. For each question from step 1, name the table and grain that answers it in one simple query. Build those gold tables: metric lists as config, baseline comparisons, both definitions where two exist. Each table gets a doc file. Each business term that spans tables (from Phase 0's decisions) gets a Page file in `docs/pages/`.

**Phase 4: Genie**
14. Write `scripts/build_genie_space.py` (5.4). `TABLES` is a loader that reads the doc files with `genie: true` (table list and column descriptions), so column text is never retyped. Write `INSTRUCTIONS` in the order from 2.5, plus `EXAMPLES`, `SAMPLE_QUESTIONS` and `BENCHMARKS`. Add a `resources/<space>.genie.yml`.
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
Bundle paths, pipeline and job IDs, deploy commands. (The general rules are in the working rules; don't repeat them here.)

## Security and data
Secrets in dbutils.secrets / .env; data files gitignored.

<working rules (2.9) and the skills block (2.8), verbatim>
```

### 5.2 Ingest notebook skeleton (REST source, date-windowed)

Keep these parts and replace the endpoint and flattening:

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

New pipelines are SQL by default (2.8): one dataset per file, named after the dataset. Read the `databricks-pipelines` skill's SQL references before writing them. The SQL versions, with an `orders` source as the example:

```sql
-- pipeline/transformations/bronze/bronze_orders.sql
CREATE OR REFRESH MATERIALIZED VIEW bronze.bronze_orders (
  CONSTRAINT valid_id EXPECT (order_id IS NOT NULL),
  CONSTRAINT valid_date EXPECT (order_date IS NOT NULL)
)
COMMENT 'Raw orders as landed by ingest/orders_ingest.py.'
AS SELECT * FROM raw.orders_raw;
```

```sql
-- pipeline/transformations/silver/silver_orders.sql
CREATE OR REFRESH MATERIALIZED VIEW silver.silver_orders (
  CONSTRAINT valid_id EXPECT (order_id IS NOT NULL),
  CONSTRAINT discount_within_gross EXPECT (discount <= gross_amount)
)
COMMENT 'Exactly one row per order. Amounts in USD; is_void marks cancelled orders.'
AS SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(grossAmt AS DOUBLE) AS gross_amount,        -- renames live in the SELECT
  CAST(discountAmt AS DOUBLE) AS discount,
  status = 'VOID' AS is_void                       -- flags, not filters
FROM bronze.bronze_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC NULLS LAST) = 1;
```

A gold table then answers a question directly, for example `gold.gold_daily_revenue` (one row per day and region, revenue and order counts from `silver.silver_orders WHERE NOT is_void`).

The Python versions, for when SQL can't do the job (Python-only features, shared helpers in `pipeline/utilities/`, UDFs), or for an existing Python pipeline:

```python
# pipeline/transformations/bronze/bronze_orders.py
from pyspark import pipelines as dp

@dp.materialized_view(name="bronze.bronze_orders", comment="Raw orders as landed by ingest/orders_ingest.py.")
@dp.expect("valid_id", "order_id IS NOT NULL")
@dp.expect("valid_date", "order_date IS NOT NULL")
def bronze_orders():
    return spark.read.table("raw.orders_raw")
```

```python
# pipeline/transformations/silver/silver_orders.py
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

KEY = ["order_id"]
COLUMN_RENAMES = {"grossAmt": "gross_amount", "discountAmt": "discount"}

@dp.materialized_view(name="silver.silver_orders", comment="Exactly one row per order. Amounts in USD; is_void marks cancelled orders.")
@dp.expect("valid_id", "order_id IS NOT NULL")
@dp.expect("discount_within_gross", "discount <= gross_amount")
def silver_orders():
    raw = spark.read.table("bronze.bronze_orders")
    w = Window.partitionBy(*KEY).orderBy(col("ingested_at").desc_nulls_last())
    one = raw.withColumn("_rn", row_number().over(w)).where("_rn = 1").drop("_rn")
    for old, new in COLUMN_RENAMES.items():
        one = one.withColumnRenamed(old, new)
    return one.select(
        col("order_id").cast("bigint"),
        col("order_date").cast("date"),
        col("gross_amount").cast("double"),
        col("discount").cast("double"),
        # flags, not filters:
        (col("status") == "VOID").alias("is_void"),
    )
```

### 5.4 Genie script

Write `scripts/build_genie_space.py` with these constants: `OUT` (the generated JSON path), `WH` (warehouse id), `TABLES` (loaded from the doc files), `ENTITY`, `INSTRUCTIONS`, `EXAMPLES`, `SAMPLE_QUESTIONS`, `BENCHMARKS`. Use a counter for ids (`_id`) with a starting value unique to the space. Helpers: `table_columns` (column lists from Unity Catalog), `run_sql` (statements API), `build()` (assembles the JSON and asserts every described column exists) and a `--test` flag that runs every example and benchmark query.

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
domain: <project>                        # the project domain (or a subdomain of it)
synonyms: [active account, live customer]
related_tables: [gold.gold_customer_month, silver.silver_customers]
---

A customer with at least one paid order in the trailing 90 days. ...
```
