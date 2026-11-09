# PROCESS

## Architecture

**Ingestion (`src/ingest.py`).** A standalone script lands raw files in one Unity Catalog Volume:

```
bls/pr/<file>/<version>/<file>                  immutable versions of each BLS file
datausa/population/<version>/population.json    immutable versions of the DataUSA API response
_state/active_files.json                        which version of each file is live
```

- The BLS directory listing is parsed on every run, so no filenames are hardcoded.
- The population data is fetched live from the DataUSA Tesseract API on every run.
- BLS requests send a `User-Agent` with a contact email, because BLS returns 403 to anonymous robots
  (https://www.bls.gov/bls/pss.htm). Requests are spaced out, and 429/5xx responses back off using
  `Retry-After`.

**Re-running ingestion safely.**
- Unchanged files are skipped. BLS files are compared on listed timestamp and size, the DataUSA
  response on a content hash.
- New or changed files are written to a new `<version>/` directory. Nothing is overwritten.
- Files removed upstream are dropped from `active_files.json`.
- Downloads go to a temp file and are then moved into place, so a half-written file never lands.
- The state file is written last, so a failed run is simply redone on the next run.
- An empty or unparseable listing aborts the run instead of being treated as "every file deleted".

**Why versioned paths.** Auto Loader tracks files by path. A file overwritten at the same path is
either missed (the default) or re-read with no way to tell old rows from new. A new path per version
fixes this: each version is ingested exactly once, and `active_files.json` says which one is current.

**Bronze (PySpark, streaming tables + Auto Loader).** Bronze keeps values exactly as published, as
strings, and records the source path and version on each row. Refresh cost scales with what changed.
Schema checks happen on the way in:
- A BLS header that doesn't match the documented columns fails the update. Mis-parsing positional
  data silently would be worse.
- Rows with the wrong field count or a null key are dropped.
- The DataUSA JSON is read with an explicit schema.

Data files are matched by `*.data.*`, so a new BLS data partition is picked up without code changes.

**Silver (PySpark, `AUTO CDC FROM SNAPSHOT` into streaming tables).**
- BLS and DataUSA publish full snapshots, not change events. For each silver table, a temporary
  `*_snapshot` view builds the current snapshot:
  - it joins bronze to `active_files`, so only the live file versions are used,
  - it trims the space-padded fields and casts types,
  - it de-duplicates to one row per key, because `pr.data.0.Current` and `pr.data.1.AllData` overlap,
  - it validates year, period, value and population with expectations.
- `value` is `DECIMAL(18,3)`, so sums are exact.
- `create_auto_cdc_from_snapshot_flow` (SCD type 1) diffs each snapshot against the previous one and
  applies only the changes:
  - a new key is **inserted**,
  - a key whose values changed is **updated**,
  - a key that disappeared is **deleted**, whether one row was removed or a whole file was deleted
    upstream.
- Unchanged rows are never rewritten. Replaced versions stay in bronze for audit.
- Keys: `(series_id, year, period)` for observations, `series_id` for series, `(nation_id, year)` for
  population.

**Streaming tables vs. materialized views.** A Spark Declarative Pipeline creates and manages three
kinds of dataset: streaming tables, materialized views and temporary views. It creates them on the
first run, so nothing needs a `CREATE TABLE` upfront. Both streaming tables and materialized views are
ordinary Unity Catalog Delta tables that consumers query with plain SQL. Only the pipeline writes to
them. "Streaming" here describes how a flow writes the table, not a job that's always running: this
pipeline is triggered, so it processes what changed and then stops.
- **Bronze: streaming tables (Auto Loader).** The raw files are immutable and append-only, which is
  what Auto Loader is built for. Each file version is read once and never re-scanned.
- **`active_files`: materialized view.** It is one small state file that ingestion rewrites at the
  same path. A streaming read would miss that rewrite, so it is re-read on every refresh.
- **Silver: streaming tables written by `AUTO CDC FROM SNAPSHOT`.**
  - Silver must retract rows when a file version is replaced or deleted upstream.
  - A streaming table written by a plain append flow can't retract rows.
  - A materialized view over a snapshot, which has no row lineage, tends to recompute everything.
  - In a pipeline, only a streaming table can be the target of an `AUTO CDC` flow. That flow writes
    just the inserts, updates and deletes, as a `MERGE`, and gold can refresh from those row-level
    changes.
- **Gold: materialized views.** Gold is small derived answers: aggregates, a window and a join over
  silver. A materialized view always returns the same result as the equivalent batch query, and
  serverless refreshes it incrementally where it can. Gold doesn't join `active_files`, because silver
  already contains only the active versions.

**Is gold incremental?** Serverless decides on each refresh, based on cost and on whether the query is
supported. The silver tables have row tracking and deletion vectors enabled, which makes them supported
sources.
- **No silver change:** gold is skipped (`NO_OP`). This was verified on the workspace, see below.
- **Silver rows changed:**
  - **Q3** (`gold_series_population`) is a filter plus a LEFT JOIN, so it can refresh incrementally
    (`ROW_BASED`).
  - **Q2** (`gold_best_year_by_series`) is a `GROUP BY` over a `DECIMAL` sum, then `row_number()`
    partitioned by `series_id`, so it can refresh incrementally (`GROUP_AGGREGATE` /
    `WINDOW_FUNCTION`). Only the series whose rows changed are recomputed.
  - **Q1** (`gold_population_stats`) uses `avg` and `stddev`, which work on `DOUBLE`. Databricks
    recomputes floating-point aggregates in full, but the query reads only six rows.
- **First build:** every gold table runs as `COMPLETE_RECOMPUTE`.
- The technique each refresh used is in the pipeline event log, under
  `event_type = 'planning_information'`.

**How the silver diff works.** On every update, `AUTO CDC FROM SNAPSHOT` reads the whole
`*_snapshot` view and compares it with the target, key by key:
- a new key is inserted,
- a key with any changed non-key column is updated,
- a key that disappeared is deleted,
- an identical row is not written.

So the read is a full scan of the active data (about 92k rows), while the write is proportional to
what changed. `version` is not a silver column, so re-ingesting identical content writes nothing.

**`AUTO CDC` vs. a hand-written `MERGE`.** A `MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE` over
the same snapshot would produce the same table. Gold could still refresh incrementally, because Enzyme
works from any Delta source with row tracking. I chose `AUTO CDC` because it keeps silver inside the
pipeline, which gives:
- dependency ordering,
- expectations and event-log metrics,
- checkpointing,
- key handling, with no `MERGE` statement to keep in sync with the snapshot's columns.

A separate `MERGE` job would only make sense if something outside the pipeline had to write to silver.

**Why not CDF-based `AUTO CDC`.** Bronze is append-only, loaded from files:
- A republished file arrives as a new set of inserts, not as updates.
- A file deleted upstream produces no event at all.
- Which version is live is recorded in `active_files.json`, not in bronze's change feed.

The source publishes snapshots, so a snapshot diff is the correct way to derive changes.

**How gold knows what changed.** Silver has row tracking and deletion vectors enabled. On each
refresh, Enzyme reads silver's row-level changes since gold's last refresh. It then chooses
`NO_OP`, an incremental technique, or `COMPLETE_RECOMPUTE`, based on the query shape and cost. If
silver were a materialized view, it would mostly recompute in full, for three reasons:
- it joins on a moving `active_files` set,
- it de-duplicates across files,
- it must delete rows.

Every refresh would then rewrite all of silver and push gold into full recomputes as well.

**Gold guard.** `gold_population_stats` has `EXPECT (years = 6) ON VIOLATION FAIL UPDATE`. If a year
goes missing upstream, the refresh fails and the previous correct result stays. Otherwise the query
would quietly publish a plausible stddev over five years.

**Verified on the workspace** (the schema was dropped and rebuilt from scratch, 2026-11-09 data):

| Table | Rows |
|---|---|
| `bronze_bls_data` | 138,489 |
| `silver_bls_observations` | 92,328 (no duplicate keys) |
| `silver_bls_series` | 337 |
| `silver_population` | 11 |
| `gold_best_year_by_series` | 283 |
| `gold_series_population` | 39 |
| `gold_population_stats` | 6 years |

- **First run:** silver's `MERGE` inserted 92,328 rows.
- **Second run, with no upstream change:** silver's `MERGE` read 0 source rows and inserted, updated
  and deleted 0 rows. All three gold tables were planned as `NO_OP`.

**Workspace screenshots.** These were taken on 2026-11-07, before silver moved to `AUTO CDC`, so the
pipeline graph still shows the silver tables as materialized views. The gold answers are the same in
the current design.

Pipeline run: bronze → silver → gold, all flows completed and all expectations met.

![Pipeline graph](docs/images/pipeline_graph.png)

The pipeline succeeded. The job's red run is the `ingest` task: Free Edition blocks outbound access to
bls.gov, so I ran ingestion locally and uploaded the files.

![Jobs and pipelines](docs/images/jobs_and_pipelines.png)

The resulting schema has 10 pipeline tables and the `raw` Volume.

![Catalog tables](docs/images/catalog_tables.png)

Q1: `gold_population_stats`

![Q1 result](docs/images/q1_population_stats.png)

Q2: `gold_best_year_by_series` (283 series)

![Q2 result](docs/images/q2_best_year_count.png)

Q3: `gold_series_population` (39 rows; population is null for years outside ACS 1-year coverage)

![Q3 result](docs/images/q3_series_population.png)

**Gold: SQL vs. PySpark.**
- **Q1 and Q3: SQL is primary.** Each is a filter plus an aggregate or a join, which reads most
  clearly as SQL.
- **Q2: PySpark is primary.** "Top-1 per group" is a window over an aggregate, which is easier to
  compose and comment step by step in PySpark.
- Each question also has an alternative in the other language (`alternatives.py`,
  `alternatives.sql`). These are documentation only and not deployed. I checked locally that each pair
  returns identical rows.

**Data decisions, based on the provider docs:**
- Q2 excludes `Q05`. `pr.period` defines it as the annual average, so including it would double count.
- 54 series publish only annual averages, so 283 of the 337 series appear in Q2.
- Q2 ties go to the earliest year. The label is BLS's `series_title`.
- Q1 reports the sample stddev, plus the population stddev for reference. The update fails if any of
  the six years is missing.
- Q3 uses a LEFT JOIN, because ACS 1-year data doesn't exist before 2013 or for 2020.

## Trade-offs

What I'd do differently for a real client:
- **Data volume.** Silver writes only the changes, but it still reads the active snapshot on every
  update to compute the diff. At larger scale I'd switch to historical snapshots keyed on the ingest
  version: the source function returns a new snapshot only when the ingest version changes, so a run
  with no upstream change skips the read and the diff entirely. I'd also expire old raw versions
  with a retention job.
- **Schema drift.** Today any drift fails fast. I'd add a rescued-data column plus alerts, so additive
  changes don't block the pipeline.
- **Access control.** Use separate schemas per layer, give consumers grants on gold only, and run jobs
  as a service principal.
- **Monitoring.** Alert on expectation metrics from the pipeline event log, send job failure
  notifications, and add a freshness check against the BLS release calendar.
- **Cost.** Run serverless on a daily or release-aligned schedule. A run with no upstream changes
  downloads nothing and ingests nothing.
- **Egress.** Free Edition blocks outbound access to `download.bls.gov`, so I ran ingestion locally
  and uploaded the files. On a paid workspace the bundled job runs end to end.

## AI usage

I used Devin (an AI coding assistant) to:
- scaffold the bundle, ingestion script and pipeline files,
- look up current Databricks APIs (the `pyspark.pipelines` module, DAB resource fields, Auto Loader
  options),
- write a throwaway local PySpark harness that ran the pipeline code against the real downloaded data.

How I validated and corrected its output:
- **I steered the design through three rounds.**
  1. The first draft was over-engineered.
  2. The simpler second draft re-read every file on each refresh.
  3. The final design uses streaming bronze over immutable file versions.
- **I rejected full recompute in silver.** An earlier version made silver materialized views and
  argued that recomputing them was cheap. I changed silver to `AUTO CDC FROM SNAPSHOT`, so it applies
  only inserts, updates and deletes, and kept materialized views for gold.
- **I tested change handling.** I simulated an upstream change to `pr.series`. Bronze held both
  versions and silver showed only the active one.
- **I fixed portability issues.** `QUALIFY` failed on open-source Spark, so I rewrote it as a
  `row_number()` subquery.
- **I checked the data semantics against the sources.** The Q05 annual average and the overlapping data
  files came from reading `pr.txt` and the raw files, not from the AI's assumptions.
- **I verified the answers by hand.** For example, I computed the Q1 mean from the six population
  values, and the workspace results match the local run.
- **I checked incrementality on the workspace rather than trusting the docs.** I used the Delta
  history (`MERGE` metrics) and the event log (`planning_information`).

## Retrospective

The data semantics were harder to get right than the code:
- an annual-average period mixed in with the quarters,
- two overlapping data files,
- space-padded fields and CRLF line endings,
- a deprecated DataUSA endpoint.

The other hard part was making re-runs correct when files change or disappear upstream, given how
Auto Loader tracks files. That led to the versioned-path design.
