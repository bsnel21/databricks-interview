# Databricks project playbook: concepts and build plan

This playbook is the guide for a **4-hour prototype on Databricks Free Edition**: land the files in a volume, build one pipeline from bronze to silver to gold, and answer the stakeholder's question live in a Genie agent and an AI/BI dashboard. It covers the patterns, the problems to expect, the working rules, a build plan and starter templates. The project itself is not started: the business question and data come from the scenario (section 4, Phase 0).

**Scope.** Build the smallest thing that answers the stakeholder's decision live. Everything a production system would add (scheduled ingest, dev/prod promotion, generated documentation, models) is left out of the build on purpose and listed in section 6, the production roadmap, which is the "next steps" part of the pitch.

**What the panel sees:** the workspace and the deck. Something that isn't deployed to the workspace doesn't exist for the demo.

**How to use it:** work through section 4 (the plan) in order, using section 5 for the starter files. Sections 2 and 3 explain why each step is done that way. Project-specific facts (tables, IDs, decisions, open items) belong in CLAUDE.md and `docs/project-status.md`, not here.

---

## 1. The shape of the system

```
source files (CSV, JSON, ...) from the scenario
   |  uploaded as-is (or written by one small notebook, if the source is an API)
   v
/Volumes/<catalog>/raw/landing/<source>/   landing zone: files, not a table
   |  one Spark Declarative Pipeline (bronze -> silver -> gold), SQL
   v
bronze.bronze_<source>                     the files read as-is with read_files, plus file name and load time
silver.silver_<source>                     one row per stated grain: typed, renamed, deduplicated, flagged
gold.gold_<topic>                          one or two tables shaped around the stakeholder's questions
   |
   v
Genie agent + AI/BI dashboard              plain-language questions and the charts for the decision
```

This matches Databricks' medallion vocabulary: **bronze is the raw layer** (data as received), silver is cleaned and checked, gold is shaped for business use. The landing volume holds files and is not counted as a layer. For a non-technical stakeholder, say it plainly: "data as we received it → cleaned and checked → shaped to answer your question."

The data pieces deploy together as one **Declarative Automation Bundle** (formerly Databricks Asset Bundle):

| Piece | File | What it does |
|---|---|---|
| Bundle root | `databricks.yml` | variables (catalog, warehouse looked up by name), one target, `sync` excludes for PDFs and `data/` |
| Schemas | `resources/<project>.schemas.yml` | `raw`, `bronze`, `silver`, `gold` |
| Landing volume | `resources/<project>.volume.yml` | the `raw.landing` volume the files are uploaded to |
| Pipeline | `resources/<project>.pipeline.yml` | serverless pipeline, `root_path: ../pipeline`, glob over `transformations/**`, `landing_path` in its configuration |
| Dashboard | `resources/<project>.dashboard.yml` plus `dashboards/<name>.lvdash.json` | AI/BI dashboard on the gold tables, published on deploy |

The catalog itself is created once with SQL, outside the bundle: Free Edition refuses to create a catalog through the API, because it needs Default Storage (README). The Genie agent is also outside the bundle, because bundles don't support Genie agents (2.4).

---

## 2. Patterns, and how to apply them

### 2.1 The bundle is the deploy unit, with one target

- The schemas, the landing volume and the pipeline are defined in `resources/*.yml`, so the data side can be redeployed in one command: `databricks bundle deploy -t prod`.
- **One target, `prod`, without `mode: development`.** Development mode prefixes names with `[dev <user>]`, which looks unfinished in a demo, and there is no promotion to do.
- Variables in `databricks.yml` (`catalog`, `warehouse_id`) keep catalog names out of the code.
- No job and no schedule: the pipeline is run by hand with `databricks bundle run <pipeline key> -t prod`.
- **Resource keys are unique across all resource types** (a catalog and a pipeline can't both be `databricks_interview`).
- **Bundle sync uploads every file git doesn't ignore** to the workspace. `databricks.yml` also excludes `*.pdf` and `data/` explicitly, so local-only material never reaches the workspace.
- **Workflow:** edit locally, run `databricks bundle validate --strict -t prod`, and deploy from the local files. The user commits at milestones, so git stays roughly in step with the workspace without slowing each deploy.
- Never edit code in the workspace. Edits there drift from git and are overwritten on the next deploy.

### 2.2 Landing: files in a volume

- The scenario's files go **as-is** into `/Volumes/<catalog>/raw/landing/<source>/`, one folder per source, uploaded with `databricks fs cp` or the UI. The CLI needs the `dbfs:` prefix and an existing folder: `databricks fs mkdir dbfs:/Volumes/<catalog>/raw/landing/<source>`, then `databricks fs cp <file> dbfs:/Volumes/<catalog>/raw/landing/<source>/`. Data files are gitignored, so they never go into the repo.
- **If the source is an API,** write one small notebook in `ingest/` that calls it and writes the responses as JSON files into the same landing folder. The rest of the flow stays the same. Secrets go in `dbutils.secrets`, never in the notebook.
- Keep the files as received. Every fix happens in silver, so you can always show "this is what we were given".

**Intake when the data arrives by email.** Save every attachment to `data/<source>/` locally (gitignored), and don't open-and-save it in Excel, since that rewrites dates and ids. Before uploading:
1. **List what came:** files, formats, sizes, and whether several files share one shape (monthly extracts of the same table) or are different tables.
2. **Look at each file:** `head -5` for CSV and JSON Lines; the first characters (`[` means a JSON array, `{` per line means JSON Lines); the sheet names and header row for Excel. Note the separator, header row, encoding, keys and date format.
3. **Decide the folders:** **one landing folder = one shape = one bronze table.** Files with the same columns (even in a different order, or with an added column) share a folder; a different table gets its own folder. A workbook with several sheets is one folder, read once per sheet.
4. **Unzip locally** and upload the contents. `read_files` decompresses `.gz` but reads a `.zip` archive as garbage bytes (tested).
5. Write down each source's grain and keys in `docs/project-status.md` before writing SQL.

**`read_files` by format.** Tested on the Free Edition SQL warehouse (not yet inside the pipeline) unless a row says **untested**. Always keep `inferColumnTypes => false`.

| Delivery | Bronze `read_files` options | Notes from the test |
|---|---|---|
| One or many CSVs, same table | `format => 'csv', header => true` | Files with columns in a different order line up by name; a column missing from a file is NULL. Add `_metadata.file_name` to see which file a row came from. A UTF-8 BOM is handled. |
| CSV with `;` or tabs | add `sep => ';'` (tabs: `'\t'`, **untested**) | European numbers like `1.234,50` stay strings; fix them in silver with `replace` before `try_cast`. |
| JSON array (`[ {...}, {...} ]`) | `format => 'json', multiLine => true` | Set `multiLine` for arrays and pretty-printed files. |
| JSON Lines (one object per line) | `format => 'json'` | |
| Nested JSON | `format => 'json'` | With all strings, a nested object or array lands as a **JSON string**. Read fields with `customer:name::string`, and explode arrays in silver with `LATERAL variant_explode(parse_json(items)) AS i`, then `i.value:sku::string`. |
| Excel `.xlsx` | `format => 'excel', headerRows => 1, dataAddress => '<Sheet>'` | List sheets with `operation => 'listSheets'`. A title row above the header needs a bounded range: `dataAddress => 'Sheet!A3:C500'` (an open range like `A3:C` fails). Date cells land as `2026-11-01T00:00:00.000`. Numbers can land as `7.0`: `try_cast('1001.0' AS BIGINT)` is **NULL**, so cast ids through `DECIMAL` first. Not yet run inside the pipeline: if `excel` fails there, convert the sheet to CSV locally with openpyxl and land the CSV. |
| `.csv.gz` / `.json.gz` | same as the uncompressed format | `.csv.gz` tested and decompressed automatically; `.json.gz` **untested**. |
| `.zip` | not readable | Unzip before uploading. |
| Parquet | `format => 'parquet'` | **Untested.** Expected to keep the file's own column types. |

### 2.3 One Spark Declarative Pipeline for bronze, silver and gold

- **SQL, one dataset per file, named after the dataset,** under `pipeline/transformations/<layer>/` (the `databricks-pipelines` skill's defaults).
- **Every dataset** has a comment and expectations: `CREATE OR REFRESH MATERIALIZED VIEW <schema>.<table> (CONSTRAINT ... EXPECT (...)) COMMENT '...' AS ...`. Use schema-qualified names, since each layer has its own schema. The comment states the **grain** ("exactly one row per customer-month") and what the table is for. Comments show in Unity Catalog and help Genie, so they are the project's table documentation.
- **Catalog layout:** one catalog for the project, one schema per layer (`raw` for the landing volume, then `bronze`, `silver`, `gold`). Table names are `<layer>_<source>[_<dataset>]`.

**Bronze:** read the landing folder with `read_files`, as-is: `inferColumnTypes => false`, so every value lands as a string and an odd value never breaks the load. Add `_metadata.file_path` and `_metadata.file_modification_time` (when the file landed), and NOT NULL expectations on the keys. No business logic. The folder path comes from the pipeline's `landing_path` setting (`'${landing_path}/<source>/'`), so no catalog name is hard-coded.
- Use a **materialized view with a batch `read_files`**. Every refresh rereads every file, which is fine at prototype size and means a corrected, re-uploaded file is picked up without a full refresh. A production pipeline would use a streaming table (`FROM STREAM read_files(...)`) to read only new files (section 6).
- Landing strings means silver does all the typing, with `try_cast`, so a bad value becomes NULL and a flag rather than a failed run.

**Silver:**
- **Enforce the grain explicitly.** Use `QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <load time or business rule> DESC) = 1`, so duplicates in the source can't double count.
- **Cast every field** with `try_cast` and rename to readable snake_case names, all in the `SELECT`. Add a `has_bad_values` flag for rows whose key, date or amount is missing or unreadable.
- **Flag, don't filter.** Silver keeps every row and adds booleans (`is_void`, `is_test_order`). Gold chooses its own filter, and nothing disappears silently. In the demo, flags are an easy way to show data quality ("3% of orders were test orders; here's how we handled them").
- **Put reconciliation checks in expectations,** for example `discount <= gross_amount`.

**Gold:**
- **Build tables for the stakeholder's questions,** not for the source. Pick the grain from the question: per entity-period-category.
- **Add comparison-to-baseline columns** (`*_vs_average`), because "is this unusual?" is the most common follow-up.
- **Weighted averages, never averages of averages.** Aggregate from rows, or weight by the count.
- **When two definitions of a concept both matter, keep both and name them differently,** and say which one the decision uses.

**Where and when the joins happen:**

| Layer | Combining allowed | Not allowed |
|---|---|---|
| Bronze | Many files of one shape read together from one folder (a `read_files` union, not a join). | Any join. One bronze table per source shape. |
| Silver | `UNION ALL BY NAME` of the same entity delivered in two formats or files (orders as CSV plus orders as JSON), with a `source` column, deduped after the union. Exploding nested JSON into a child table (`silver_order_items`). A lookup that only standardizes a key or code (region name to region id). | Joining one business entity onto another. |
| Gold | Fact-to-dimension joins for the stakeholder's questions (orders to customers to regions). This is the default. | Joining before each side is deduped to its grain in silver. |
| Silver (exception) | When two or more gold tables repeat the same fact-to-dimension join, build it once as an enriched silver table (`silver_orders_enriched`, one row per order) and have gold read that. Decided per scenario; note the decision in `docs/project-status.md`. | Enriched tables that change the fact's grain. |

Join rules:
- **Join only after both sides are at their stated grain** (silver's `QUALIFY`), on typed keys of the same type (`BIGINT` to `BIGINT`, trimmed, same case).
- **`LEFT JOIN` from the fact**, so a missing dimension row never drops revenue, and flag the miss (`customer_id IS NOT NULL AND c.customer_id IS NULL AS is_unmatched_customer`).
- **Check fan-out:** the joined table must have the fact's row count. Add an expectation or compare `COUNT(*)` before and after; a duplicate dimension key silently doubles the sums.
- **Report the match rate** ("97% of orders match a customer") in the demo.
- **Ask the user before choosing** whenever a join is unclear: the key isn't obvious, the keys are in different formats, a dimension changes over time (which version applies to an old order?), or one row could match many.

### 2.4 The Genie agent

Built outside the bundle (bundles don't support Genie agents): by Claude Code with the `databricks-genie-agents` skill, or in the workspace UI. The configuration is kept in the repo as `genie/genie_agent.json`, so the instructions and examples are written down. If it's changed in the UI, copy the change back into the file (export with `databricks genie get-space <id> --include-serialized-space`).

- **Create:** `databricks genie create-space <warehouse id> "<json>" --title ... --description ... --parent-path /Workspace/Users/<you>/databricks_interview_demo`. **Update:** `databricks genie update-space <id> --serialized-space "<json>"` (a full replacement).
- **Format rules** (strictly validated): every item has a unique 32-character hex `id`, and lists are sorted by id; text fields are arrays of strings; `benchmarks` sits at the top level; at most one `text_instructions` item. Fixed ids (`a000...001`, `b000...001`, `c000...001`) keep diffs stable.
- **Column descriptions and synonyms** (`column_configs`) do most of the work; turn on entity matching only for categorical columns people name, such as region.
- **Test every example query on the warehouse before it goes in,** then ask the agent questions phrased differently from its examples (`databricks genie start-conversation <id> "<question>"`).

Building it means writing its briefing; testing it means asking it questions as a user would and fixing the misses.

- **Tables:** only the gold tables (and silver if needed). Fewer tables give better answers.
- **Instructions** (keep this order):
  1. **Required filters** first, in capitals: things that silently produce wrong answers if missed, such as multiple rows per entity.
  2. **Purpose and audience:** who asks, and in what language to answer.
  3. **Which table to use** for which kind of question.
  4. **Definitions** that have more than one meaning.
  5. **Reading the numbers:** units, the direction of "good", display formats. Default to the latest period and say so.
- **Example SQL** for the main question types: an overview, a breakdown, a ranking, and a "vs baseline" question.
- **Sample questions:** the questions the stakeholder will actually ask in the demo.
- **Benchmarks:** a few questions with known answers. Run them before the demo. Benchmark SQL returns only the columns the question asks for, or a correct answer is marked wrong.

### 2.5 The AI/BI dashboard

One dashboard with the one or two charts that support the decision, on the gold tables. It's often a stronger demo than Genie alone: open the dashboard to show the answer, then switch to Genie for the follow-up questions.

- **A bundle resource,** so Claude Code writes and deploys it: `dashboards/<name>.lvdash.json` (datasets with `queryLines`, a canvas page, widgets with `queries` and `spec`) and `resources/<project>.dashboard.yml` with `warehouse_id` and `dataset_catalog: ${var.catalog}`, so the dataset SQL uses `gold.<table>` without a catalog name.
- **Widget specs:** counters use `spec.version: 2`; bar and line charts use `spec.version: 3` with `encodings.x`, `y` and optional `color`, each with a `scale.type` (`categorical`, `quantitative`, `temporal`). Field names in `encodings` must match the `fields[].name` in the widget's query.
- **Editing in the UI is encouraged** (layout, chart types, colours, filters): Claude Code drafts from code, the user refines visually. Keep the repo in step:
  - While editing, run `databricks bundle generate dashboard --resource <key> --watch --force -t prod` in the background. It rewrites the `.lvdash.json` file on every save in the UI.
  - Without the watch, run the same command without `--watch` before the next deploy, or the deploy overwrites the UI edits.
  - Stop the watch before changing the `.lvdash.json` from code, or the next UI save overwrites the code change.

### 2.6 Working with Claude Code

- **CLAUDE.md is the living spec:** layout, conventions, every table's grain and meaning, the IDs, and the rules. It is updated in the same change as the code.
- **`docs/project-status.md` is the handoff and decision log:** where the work stands, open items, and every decision with its reason ("used net revenue, not gross, because the decision is about margin"). The decision log doubles as material for the "how did you use AI?" part of the debrief.
- **Coding subagents run on Sonnet.** The main session plans and reviews.

### 2.7 Documentation

Four places, nothing generated:

| Where | Holds |
|---|---|
| `README.md` | Setup and how to deploy |
| `CLAUDE.md` | How the project is built: layout, conventions, IDs, rules, one line per table |
| Table comments in the pipeline SQL | Each table's grain and purpose. Shows in Unity Catalog and Genie. |
| `docs/project-status.md` | Where the work stands, decisions and their reasons |

### 2.8 Databricks agent skills (`databricks aitools`)

Databricks publishes instruction files ("skills") for coding agents, installed with the Databricks CLI. Install **the same eight skills at one pinned version, in project scope**, and **let the skills win by default**: CLAUDE.md lists a few exceptions that only the user can lift.

**Why these eight.** They cover CLI basics, pipelines, bundles, serverless compute, jobs, Unity Catalog, metric views and Genie. Each skill's description is always in context, so unused skills cost tokens and add noise.

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

**The CLAUDE.md skills block** (version 0.2.47). The exceptions keep deletes, grants and anything outside the project behind a question, a single instruction file, the default profile and unrequested job changes in the user's hands. Every other difference follows the skill. On a skill update, recheck only these exceptions.

```markdown
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
```

### 2.9 Working rules

These go in CLAUDE.md verbatim (adjust only if the user's preferences change). They are sized for a prototype nobody else depends on: act inside the project and report, ask before anything destructive or outside it, and keep git in step through milestone commits rather than a pushed-commit check before every deploy.

```markdown
## Working rules

- The user commits and pushes. Never commit or push. Suggest a commit at each milestone (silver works, gold works, before the demo), so there are points to roll back to.
- Inside the project (its catalog, bundle, Genie agent and dashboard), deploy, run the pipeline and change tables, the Genie agent and the dashboard without asking, then report what was done.
- Ask first before deleting anything (tables, schemas, volumes, files, the Genie agent, the dashboard, or a deploy that would delete or recreate a resource), before changing grants, and before touching anything outside the project.
- Deploy from the local files; they don't need to be pushed first. Run `databricks bundle validate --strict -t prod` before every deploy.
- Never edit deployed code in the workspace UI: change the repo, then redeploy. Scratch notebooks for exploring the data or trying Genie Code are fine; anything worth keeping moves into the repo. The Genie agent and the dashboard may be changed in the UI; copy Genie changes back into `genie/genie_agent.json`, and pull dashboard changes back with `databricks bundle generate dashboard --resource <key> --force` before the next deploy.
- Coding subagents run on Sonnet.
```

---

## 3. Problems to expect (check for these early)

| Problem | What to do |
|---|---|
| Python 3.15 breaks `databricks-sql-connector` | Use Python 3.12 for the local venv. |
| Serverless-only workspace (Free Edition) | Use `%pip` in every notebook. No cluster configs, `spark.conf.set`, RDDs or broadcast variables. |
| ANSI mode errors on divide-by-zero | Use `try_divide`, or `CASE WHEN den > 0 THEN num / den END`. |
| Catalog creation fails with "Metastore storage root URL does not exist. Default Storage is enabled" | Free Edition only creates catalogs through SQL or the UI. Run `CREATE CATALOG IF NOT EXISTS <catalog>` on the warehouse once (README); keep the catalog out of the bundle. |
| `databricks fs cp` to a volume fails with "no such directory" | `databricks fs mkdir` the folder first, and use the `dbfs:/Volumes/...` prefix. |
| `bundle validate --strict` fails on a deprecated field (for example a pipeline's `development`) | Strict mode treats warnings as errors. Remove the field. |
| Unreadable values in a source file | Bronze lands strings; silver's `try_cast` turns them into NULL and `has_bad_values`. Count them and mention them in the demo. |
| A record's "date" has two meanings (order date vs ship date) | Keep both columns and say which one the decision uses. |
| Names stored in different formats in different tables | Join on ids, and tell Genie which format each table uses. |
| Data arrives as a `.zip` | Unzip locally first; `read_files` can't open zip archives. |
| Excel ids land as `1001.0` and `try_cast` to `BIGINT` gives NULL | Cast through `DECIMAL(38, 6)` first, or land a CSV export. |
| Nested JSON fields come back as strings | Use `col:field::string` and `variant_explode(parse_json(col))` in silver. |
| Sums double after a join | A dimension key isn't unique. Dedupe the dimension in silver, and check the row count after the join. |
| Multiple rows per entity give wrong Genie answers | Add a "REQUIRED FILTERS" instruction, and put a QUALIFY example in the instructions. |
| Genie UI changes lost | Copy them back into `genie/genie_agent.json`. |
| Genie benchmark marked wrong despite a correct answer | Trim benchmark SQL to the columns the question asks for. |

---

## 4. Build plan (checklist)

**Phase 0: frame the problem (before any code)**
1. Name the **decision** the stakeholder needs to make, and the few questions that decide it. They decide the gold tables and the Genie sample questions and benchmarks.
2. List the sources: what each file or API holds, its grain, and what looks wrong in it at first glance.
3. Write down the **key decisions** in `docs/project-status.md`: date range, exclusions, definitions of contested concepts, and what is mocked or assumed.

**Phase 1: scaffold**
4. Local environment (README): Python 3.12 venv, `.env`, Databricks CLI on the default profile, pointing at the Free Edition workspace.
5. Write `databricks.yml` with `bundle.name`, a `catalog` variable, `warehouse_id` and the single `prod` target. Give resource keys and display names project-specific names.
6. Create the catalog once with SQL (README); the bundle creates the schemas `raw`, `bronze`, `silver`, `gold` and the `raw.landing` volume.
7. Fill in CLAUDE.md from the skeleton in 5.1.
8. Create the folder layout: `pipeline/transformations/{bronze,silver,gold}/`, `resources/`, `genie/`, `dashboards/`, `docs/`, plus `ingest/` only if a source is an API.

**Phase 2: landing to silver**
9. Upload the files to the landing volume (or write the API notebook from 2.2 and run it once).
10. Write the bronze and silver files from 5.2. Silver states its grain, dedupes, casts, renames and flags.
11. Add the pipeline yml, deploy, and run the pipeline. Check the row counts and the expectation results.

**Phase 3: gold for the questions**
12. For each question from step 1, name the table and grain that answers it in one simple query. Build those gold tables, with baseline comparisons, and both definitions where two exist.

**Phase 4: Genie agent and dashboard**
13. Build the Genie agent (2.4): tables, instructions, example SQL, sample questions and benchmarks. Ask it test questions and fix the misses. The skeleton's `genie/genie_agent.json` is the template.
14. Build the dashboard (2.5) with the charts for the decision. The skeleton's `dashboards/regional_revenue.lvdash.json` is the template.

**Phase 5: deck and demo script**

**The template is a starting point, not a script.** The scenario decides the story: the slides, their order, the titles and the wording all follow the prompt and the business problem. Questions, data sources, the gold tables and the number of findings can all change. Drop, reorder, duplicate or retitle slides to fit, for example:
- several findings, each on a copy of the evidence slide;
- no single headline number, so the short-answer slide becomes a two-column comparison;
- a risk or cost question, which needs its own slide.

The fixed parts are the theme, the layouts and the find-and-replace way of filling them. Sketch the slide list from the Phase 0 framing before filling anything, and confirm it with the user.

The deck is a **Google Slides deck from the start**. Don't draft it as a claude.ai artifact or a .pptx and convert it later. Claude Code builds and edits it in place through the Google Slides connector, so the link stays the same from first draft to the demo.

15. **Copy the template.** The template is the Google Slides deck `Databricks Interview Deck Template` in the user's Drive. Its 11 slides are: cover, decision, short answer, evidence screenshot (spare), data, what I built, live demo, real and assumed, next steps, appendix divider and AI backup. It uses a Databricks-style theme set on the master: Navy `#1B3139`, Oat `#F9F7F4` and `#EEEDE9`, Lava `#FF3621` as the accent, and DM Sans. Text sits in the layouts' own title and body boxes, so the theme styles it. Every slide has speaker notes. Copy the template with Drive `copy_file` and give the copy the scenario's name; never edit the template itself. If the copy fails, ask the user to make the copy and paste its link.
16. **Fill it with find-and-replace.** Before the first edit, load the `google-workspace` skill and read its `references/slides.md`. Every placeholder is unique bracketed text, such as `[Your recommendation, in one sentence]`. Read the deck's text with Drive `read_file_content`, which takes a few KB. Then fill everything in **one `update_presentation` call made only of `replaceAllText` requests**. That call needs no object IDs, no revision guard and no styling, because the replacement keeps the box's style. Repeated placeholders such as `[Company]` change everywhere at once. Take the content from `docs/project-status.md` and the gold tables. Never invent a figure: anything not yet known stays a `[placeholder]` and is listed for the user.
    - **Removing a slide:** delete a spare slide you don't need with `deleteObject`, but only after the user agrees.
    - **Adding a slide:** use `duplicateObject` on a slide of the same type, then fill the copy.
    - **Pictures:** screenshots go in by hand (the user pastes them over the grey frame), because `createImage` needs a public URL.
    - **Rows and slide numbers:** add table rows with `insertTableRows`. Slide numbers are the user's one click: Insert → Slide numbers.
    - **Layout changes:** only these need guarded batches (`requiredRevisionId` from a fresh read). When moving or resizing a box, keep in mind that Slides stores most elements at a 3,000,000 EMU base size with a scale. An `ABSOLUTE` transform therefore needs `scaleX` and `scaleY` set to the target size divided by 3,000,000; setting them to 1 shrinks the box.
17. **Check it.** Skip this after a text-only fill whose texts are about the same length as the placeholders. Otherwise, export the deck to PDF with Drive `download_file_content`. Poppler isn't installed locally, so render the pages with a small Swift PDFKit script into a two-column contact sheet: one image per six slides costs fewer tokens than one per page. Look for overflow, overlap and clipped text, then fix and check again.
18. **Write the demo script** in the speaker notes of the live-demo slide: the questions in order, which tab answers each, and the Genie fallback if an answer misses.

Update CLAUDE.md and `docs/project-status.md` as you go.

**Before the demo:** the user makes the final milestone commit and pushes, then pulls it into the workspace Git folder (`/Users/<you>/databricks-interview`), so the Git folder shows the same code that is deployed. In the Q&A, show code from the pipeline page first (flow diagram and SQL side by side); open the Git folder only for version-control questions.

---

## 5. Starter templates

### 5.1 CLAUDE.md skeleton

```markdown
# CLAUDE.md

## Project overview
<one paragraph: the stakeholder, the decision, the sources>
<folder tree: pipeline/transformations/{bronze,silver,gold}/, resources/, genie/, dashboards/, docs/, ingest/ if needed>
- **Landing**: `/Volumes/<catalog>/raw/landing/<source>/`: what each source's files hold.
- **Tables**: one line per table: grain and purpose (the same text as its pipeline comment).

## Genie agent and dashboard
Genie configuration in genie/genie_agent.json (copy UI changes back); space and dashboard ids.

## Conventions
Catalog and schema layout, `<schema>.<table>` inside the pipeline, materialized view + comment + expectations,
snake_case names, units.

## Syncing with Databricks
Bundle paths, pipeline id, deploy commands. (The general rules are in the working rules; don't repeat them here.)

## Security and data
Secrets in dbutils.secrets / .env; data files gitignored.

<working rules (2.9) and the skills block (2.8), verbatim>
```

### 5.2 Pipeline skeleton (deployed and tested)

The repo's working skeleton is the template, so there is no second copy here to drift. It runs end to end on a dummy `orders` CSV with deliberate problems (a duplicate, a void order, a test order, an unreadable date, a missing id):

- `pipeline/transformations/bronze/bronze_orders.sql`: `read_files` from `'${landing_path}/orders/'`, all strings, source file and landing time.
- `pipeline/transformations/silver/silver_orders.sql`: one row per order with `QUALIFY`, `try_cast`, renames, and the flags `is_void`, `is_test_order`, `has_bad_values`.
- `pipeline/transformations/gold/gold_daily_revenue.sql`: one row per day and region, valid orders only, with `net_revenue_vs_region_average`.

On the day, rename these to the scenario's sources, change the columns, and replace the gold table with the ones the questions need. Read the `databricks-pipelines` skill's SQL references before writing new dataset types.

### 5.3 Genie configuration

`genie/genie_agent.json` follows the `serialized_space` format in the `databricks-genie-agents` skill (`references/create-genie-agent.md`): tables, instructions in the order from 2.4, example SQL, sample questions and benchmarks.

---

## 6. Production roadmap (talk about, don't build)

What the prototype leaves out, and what a production version would add. This is the "next steps" slide, and the answer to "how would this run for real?".

- **Incremental, scheduled ingest.** Bronze becomes a streaming table (`FROM STREAM read_files(...)`) that reads only new files. API sources get a parameterized ingest notebook that reloads a lookback window for late data and writes idempotently (`replaceWhere`), run by a scheduled job ahead of the pipeline.
- **Dev and prod targets** in the bundle, so changes are tried in dev before they reach the tables people use.
- **Everything as code,** including the Genie agent and the dashboard deployed by the bundle, so a workspace can be rebuilt from git.
- **Governed documentation:** one doc file per table generating the table comments, Unity Catalog wikis and business-term Pages, with a check that fails when they drift.
- **Governance:** grants per audience, and metric views for the business definitions so every tool uses the same numbers.
- **Models** where the decision needs a prediction, run as a separate job with an evaluation table that tracks accuracy over time.
- **Monitoring:** pipeline expectations as alerts, and a regular Genie benchmark run to catch answer quality drifting.
