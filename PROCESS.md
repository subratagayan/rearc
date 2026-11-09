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

**Silver (SQL, materialized views).**
- Silver joins bronze to `active_files`, so replaced and deleted files drop out. Their rows stay in
  bronze for audit.
- It trims the space-padded fields, casts types, and validates year, period and value with
  expectations.
- `value` is `DECIMAL(18,3)`, so sums are exact.
- `pr.data.0.Current` and `pr.data.1.AllData` overlap, so silver keeps one row per
  `(series_id, year, period)`.

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
- **Data volume.** Replace the silver MV with `AUTO CDC FROM SNAPSHOT`, so unchanged rows are never
  recomputed. Expire old raw versions with a retention job.
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
- **I tested change handling.** I simulated an upstream change to `pr.series`. Bronze held both
  versions and silver showed only the active one.
- **I fixed portability issues.** `QUALIFY` failed on open-source Spark, so I rewrote it as a
  `row_number()` subquery.
- **I checked the data semantics against the sources.** The Q05 annual average and the overlapping data
  files came from reading `pr.txt` and the raw files, not from the AI's assumptions.
- **I verified the answers by hand.** For example, I computed the Q1 mean from the six population
  values, and the workspace results match the local run.

## Retrospective

The data semantics were harder to get right than the code:
- an annual-average period mixed in with the quarters,
- two overlapping data files,
- space-padded fields and CRLF line endings,
- a deprecated DataUSA endpoint.

The other hard part was making re-runs correct when files change or disappear upstream, given how
Auto Loader tracks files. That led to the versioned-path design.
