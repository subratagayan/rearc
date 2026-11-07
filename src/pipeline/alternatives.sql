-- Documented alternative (Spark SQL) for Q2, whose primary version is in gold.py.
CREATE OR REFRESH MATERIALIZED VIEW alt_best_year_by_series_sql
COMMENT 'Q2 alternative in Spark SQL - must match gold_best_year_by_series.'
AS
WITH yearly AS (
  SELECT series_id, year, sum(value) AS value
  FROM silver_bls_observations
  WHERE period <> 'Q05'
  GROUP BY series_id, year
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY series_id ORDER BY value DESC, year) AS rn
  FROM yearly
),
best AS (
  SELECT series_id, year, value FROM ranked WHERE rn = 1
)
SELECT b.series_id, s.series_title, b.year, b.value
FROM best b
LEFT JOIN silver_bls_series s ON b.series_id = s.series_id;
