# PROCESS

## Architecture

**Ingestion (`src/ingest.py`).** The script reads the BLS directory listing on every run, so no filenames
are hardcoded. Everything lands in one Volume:

```
bls/pr/<file>/<version>/<file>                  immutable versions of each BLS file
datausa/population/<version>/population.json    immutable versions of the DataUSA JSON
_state/active_files.json                        which version of each file is live
```

Each run then:
- skips unchanged files; BLS files are compared on listed timestamp and size, DataUSA on a content hash,
- writes new or changed files as a new version,
- drops files removed upstream from `active_files.json`.

Downloads are streamed to a temp file and moved into place, so memory stays flat and a half-written file
never lands. The state file is written last, so a failed run is simply redone on the next one. If the
listing parses as empty, the script refuses to continue instead of treating every file as removed. BLS returns 403
to anonymous robots (see https://www.bls.gov/bls/pss.htm), so every request sends
`User-Agent: rearc-data-quest/1.0 (contact: <email>)`. Requests are spaced out, and 429/5xx responses
get a backoff that respects `Retry-After`.

The old DataUSA endpoint (`/api/data?...`) now returns 404. DataUSA's API docs point to the Tesseract
API, which returns exactly the document in the brief. That document is only rewritten when its content
changes.

**Bronze (Python, streaming tables + Auto Loader).** Auto Loader checkpoints which files it has read, so
each file version is ingested exactly once. Refresh cost scales with what changed, not with the total
history. Bronze keeps the values exactly as published, as strings. Expectations enforce the schema on
the way in:
- If a BLS header doesn't match the documented columns, the update fails rather than mis-parsing
  positional data.
- Rows with the wrong field count or null keys are dropped.
- DataUSA is read with an explicit schema.

**Why versioned file paths.** Auto Loader identifies files by path. With the default
`cloudFiles.allowOverwrites = false`, a file overwritten in place at the same path is never picked up
again, so an upstream change would be silently missed. Turning `allowOverwrites` on re-reads the whole
file, but the old and new rows then share one path in bronze, and nothing tells you which is current.
So ingestion never overwrites anything. Every new or changed file gets a new `<version>/` directory,
which Auto Loader treats as a new file. Bronze records `_metadata.file_path` on each row, and silver
uses that path to keep only the version listed in `active_files.json`.

Data files are found by the glob `*.data.*`, so a new data partition gets picked up without code
changes.

**Silver (SQL, materialized views).** Silver joins bronze to `active_files`, which is how changes and
deletions propagate: a replaced version or a removed file is simply no longer active, and its old rows
stay in bronze for audit. It then trims the space-padded BLS fields, types them, and checks year, period
and value with expectations. `value` is stored as `DECIMAL(18,3)` so sums are exact. `pr.data.0.Current` and
`pr.data.1.AllData` overlap, so silver keeps one row per `(series_id, year, period)`.

**Gold.**
- **Q1 and Q3: SQL is primary.** Both are a filter plus an aggregate or join, which reads most clearly in
  SQL.
- **Q2: PySpark is primary.** The window "top-1 per group" logic is easier to compose and comment in
  PySpark.

Each question also has an alternative in the other language (`alternatives.py` / `alternatives.sql`).
They are documented alternatives only: the pipeline runs just the primary versions, so the alternatives
aren't deployed as tables. I checked locally that each pair returns identical rows.

**Decisions taken after reading the provider docs:**
- **Q05 is excluded from Q2.** `pr.period` defines `Q05` as the *Annual Average*. Summing it with Q01–Q04
  would double count.
- **54 series drop out of Q2.** These manufacturing series publish only annual averages and have no
  quarters to sum, which leaves 283 of the 337 series.
- **Ties go to the earliest year.**
- **The label is `series_title`.** It comes from `pr.series`, and it is BLS's own human-readable
  description, e.g. "Employment for the Manufacturing sector, % Change same quarter 1 year ago".
- **Q1 reports sample stddev as the headline.** The population variant is included too. The update fails
  if any of the six years is missing.
- **Q3 uses a LEFT JOIN.** ACS 1-year data doesn't exist before 2013 or for 2020.

**Re-runs.** Nothing is downloaded twice, and nothing is ingested into bronze twice. Silver and gold are
materialized views, which serverless refreshes incrementally where it can.

## Deployment (Free Edition)

Deployed with `databricks bundle deploy` (dev target), then ran locally because **Free Edition restricts
outbound egress to a fixed set of trusted domains** — `download.bls.gov` fails DNS resolution on
serverless compute. The job's `ingest` task therefore can't run on Free Edition; the documented fallback
was used instead:

1. `python src/ingest.py --target ./raw` on a laptop (internet is open there),
2. `databricks fs cp -r --overwrite ./raw dbfs:/Volumes/rearc/dev_.../raw`,
3. `databricks bundle run rearc_quest_pipeline` — bronze → gold completed in ~80 seconds.

On a paid workspace the same `rearc_quest_job` runs end-to-end with no manual step. The ingest code is
identical either way; only the runner changes.

Verified in the workspace: `gold_population_stats` = 6 years, mean 322,069,808, sample stddev
4,158,441.04; `gold_best_year_by_series` = 283 rows; `gold_series_population` = 39 rows — all matching
the local run.

### Workspace screenshots

Pipeline run: bronze → silver → gold, all flows completed, expectations met.

![Pipeline graph](docs/images/pipeline_graph.png)

The pipeline succeeded. The job's red run is the `ingest` task, which failed because Free Edition blocks
outbound access to bls.gov. Ingestion was run locally instead (see above).

![Jobs and pipelines](docs/images/jobs_and_pipelines.png)

Resulting schema: 10 pipeline tables and the `raw` Volume.

![Catalog tables](docs/images/catalog_tables.png)

Q1: `gold_population_stats`

![Q1 result](docs/images/q1_population_stats.png)

Q2: `gold_best_year_by_series` (283 series)

![Q2 result](docs/images/q2_best_year_count.png)

Q3: `gold_series_population` (39 rows; population is null outside ACS 1-year coverage)

![Q3 result](docs/images/q3_series_population.png)

## Trade-offs / what I'd change for a real client

- **Volume.** For very large snapshot files I'd replace the silver MV with `AUTO CDC FROM SNAPSHOT`. It
  diffs each new snapshot, so even silver stops recomputing unchanged rows. I'd also expire old raw
  versions with a retention job.
- **Schema drift.** Today any drift fails fast. I'd add a rescued-data column and alerts so additive
  changes don't block the pipeline.
- **Access control.** Use separate schemas or catalogs per layer, give readers grants on gold only, and
  run jobs as a service principal.
- **Monitoring.** Alert on expectation metrics from the pipeline event log, set job failure
  notifications, and add a freshness check against BLS's release schedule.
- **Cost.** Use serverless with a daily or release-aligned schedule. Unchanged runs are almost free
  because nothing is downloaded.

## AI usage

I used an AI coding assistant (Devin) to:
- scaffold the bundle and pipeline files,
- look up current Databricks APIs (the `pyspark.pipelines` module, DAB resource fields),
- write a throwaway local PySpark harness that ran the pipeline files against real downloaded data.

Things I corrected or validated:
- **The design went through three rounds.** The first draft was over-engineered, so I asked for a
  simpler one. That version used materialized views over a mirrored Volume, which re-read every file on
  each refresh and wouldn't scale. The final design is streaming bronze tables over immutable file
  versions, plus one `active_files.json` state file.
- **Change and deletion handling was tested locally.** I simulated an upstream change to `pr.series`:
  bronze held both versions (676 rows) and silver showed only the active one (337 series).
- **`QUALIFY` failed on open-source Spark**, so I rewrote it as a portable `row_number()` subquery.
- **Q05 and the overlapping data files** were found by inspecting the raw files and `pr.txt` rather than
  assuming.
- **Answers were checked by hand** against the raw data, e.g. the Q1 mean from the six population
  values.

## Retrospective

The hardest part was the data semantics, not the code:
- the annual-average period hidden among the quarters,
- the two overlapping data files,
- padded fields and CRLF line endings,

The other hard part was making re-runs correct when files change or disappear. On deployment, the
surprise was Free Edition's egress whitelist — the ingest task failed on DNS before any code ran, which
is exactly why the ingest step is a standalone script that can run anywhere.
