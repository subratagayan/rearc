# PROCESS

## Architecture

**Ingestion (`src/ingest.py`).** The script reads the BLS directory listing on every run, so no
filenames are hardcoded. A small state file (`_state/bls_pr.json` in the Volume) stores each file's
listed timestamp and size. Each run:

- skips unchanged files (they aren't downloaded),
- downloads new or changed files and overwrites the copy in the Volume,
- deletes files that disappeared upstream.

The Volume is therefore always an exact mirror of the source, and re-running the script is safe. If
the listing parses as empty, the script refuses to continue instead of deleting everything. BLS
returns 403 to anonymous robots (see https://www.bls.gov/bls/pss.htm), so every request sends
`User-Agent: rearc-data-quest/1.0 (contact: <email>)`. Requests are spaced out, and 429/5xx responses
get a backoff that respects `Retry-After`. The DataUSA population JSON is fetched from the Tesseract
API (`honolulu-api.datausa.io`) and rewritten only when its content hash changes.

**Bronze (Python).** Bronze reads the files exactly as published: tab-delimited, strings only. Two
things count as the expected schema check:

- Columns are selected by name. If BLS or DataUSA renames a column, the update fails instead of
  loading bad data.
- Expectations drop rows with null keys.

Data files are found by the glob `*.data.*`, so a new data partition gets picked up without code
changes.

**Silver (SQL).** Silver trims the space-padded BLS fields, types them, and checks year, period and
value with expectations. `value` is stored as `DECIMAL(18,3)` so sums are exact. `pr.data.0.Current`
and `pr.data.1.AllData` overlap, so silver keeps one row per `(series_id, year, period)`.

**Gold.**

- **Q1 and Q3: SQL is primary.** Both are a filter plus an aggregate or join, which reads most
  clearly in SQL.
- **Q2: PySpark is primary.** The window "top-1 per group" logic is easier to compose and comment in
  PySpark.

Each question also has an alternative in the other language, materialised as `alt_*` tables. I
checked locally that each pair returns identical rows.

**Decisions taken after reading the provider docs:**

- **Q05 is excluded from Q2.** `pr.period` defines `Q05` as the *Annual Average*. Summing it with
  Q01–Q04 would double count.
- **54 series drop out of Q2.** These manufacturing series publish only annual averages and have no
  quarters to sum, which leaves 283 of the 337 series.
- **Ties go to the earliest year.**
- **The label is `series_title`.** It comes from `pr.series`, and it is BLS's own human-readable
  description, e.g. "Employment for the Manufacturing sector, % Change same quarter 1 year ago".
- **Q1 reports sample stddev as the headline.** The population variant is included too. The update
  fails if any of the six years is missing.
- **Q3 uses a LEFT JOIN.** ACS 1-year data doesn't exist before 2013 or for 2020.

**Re-runs.** The tables are materialized views over the mirrored files. Changes and deletions
upstream therefore flow through automatically, and recomputing about 140k rows costs very little.
Nothing is downloaded twice.

**Materialized views vs. tables.** I chose materialized views for bronze/silver/gold rather than
managed tables for two reasons. First, re-runs: a materialized view is recomputed from its query, so
when ingest lands a new file version (or removes one) the next `REFRESH` picks it up with no merge or
dedup logic of my own — the `active_files` join makes "which version is live" declarative. A managed
table would need explicit MERGE/upsert code and a way to retract stale rows, which is exactly where
re-run bugs hide. Second, cost: at this scale (~140k observations) a full refresh is trivially cheap,
so we get correctness without incremental bookkeeping. The trade-off is that every refresh recomputes
the whole layer; with tables plus Auto Loader checkpoints you'd pay that bookkeeping once and get
cheaper incremental refreshes, which is the right call at larger volume — noted in Trade-offs.

## Trade-offs

- **Volume.** At larger scale I'd land immutable, versioned files and read them with Auto Loader
  into streaming bronze tables. Each file version would then be processed exactly once, and a
  manifest would identify the active versions.
- **Schema drift.** Today any drift fails fast. I'd add a rescued-data column and alerts so additive
  changes don't block the pipeline.
- **Access control.** Use separate schemas or catalogs per layer, give readers grants on gold only,
  and run jobs as a service principal.
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

- **The first draft was over-engineered**, with content-addressed versions, manifests and a parity
  table. I asked for a simpler version focused on the brief.
- **`QUALIFY` failed on open-source Spark**, so I rewrote it as a portable `row_number()` subquery.
- **Q05 and the overlapping data files** were found by inspecting the raw files and `pr.txt` rather
  than assuming.
- **Answers were checked by hand** against the raw data, e.g. the Q1 mean from the six population
  values.

## Retrospective

The hardest part was the data semantics, not the code:

- the annual-average period hidden among the quarters,
- the two overlapping data files,
- padded fields and CRLF line endings,
- the deprecated DataUSA endpoint.

The other hard part was making re-runs correct when files change or disappear.
