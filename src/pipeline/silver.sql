-- Silver: only the currently active file versions, trimmed, typed, de-duplicated.
-- Joining bronze to active_files is what makes upstream changes and deletions flow through: a replaced
-- version or a removed file is simply no longer active.

-- pr.data.0.Current and pr.data.1.AllData overlap, so keep one row per (series_id, year, period).
-- DECIMAL keeps sums exact (BLS stores values to at most 3 decimals).
CREATE OR REFRESH MATERIALIZED VIEW silver_bls_observations (
  CONSTRAINT valid_year EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_period EXPECT (period RLIKE '^Q0[1-5]$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'BLS PR observations, one row per series_id/year/period. Q05 = annual average (see pr.period).'
AS
SELECT series_id, year, period, value, footnote_codes, file_name
FROM (
  SELECT
    b.series_id,
    try_cast(b.year AS INT) AS year,
    b.period,
    try_cast(b.value AS DECIMAL(18, 3)) AS value,
    nullif(b.footnote_codes, '') AS footnote_codes,
    b.file_name,
    row_number() OVER (PARTITION BY b.series_id, b.year, b.period ORDER BY b.file_name DESC) AS rn
  FROM bronze_bls_data b
  JOIN active_files a ON a.source = 'bls_pr' AND b.file_name = a.file_name AND b.version = a.version
  WHERE NOT b.is_header
)
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver_bls_series (
  CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'BLS PR series catalogue with the human-readable series_title.'
AS
SELECT DISTINCT
  b.series_id,
  b.series_title,
  b.sector_code,
  b.measure_code,
  b.duration_code,
  b.seasonal,
  try_cast(b.begin_year AS INT) AS begin_year,
  try_cast(b.end_year AS INT) AS end_year
FROM bronze_bls_series b
JOIN active_files a ON a.source = 'bls_pr' AND b.file_name = a.file_name AND b.version = a.version
WHERE NOT b.is_header;

CREATE OR REFRESH MATERIALIZED VIEW silver_population (
  CONSTRAINT valid_population EXPECT (population > 0) ON VIOLATION DROP ROW
)
COMMENT 'US population by year (ACS 1-year estimate, Census table B01003, via DataUSA).'
AS
SELECT DISTINCT
  b.nation_id,
  b.nation,
  b.year,
  CAST(b.population AS BIGINT) AS population
FROM bronze_population b
JOIN active_files a ON a.source = 'datausa' AND b.version = a.version;
