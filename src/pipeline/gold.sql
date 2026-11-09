-- Gold (primary = Spark SQL) for Q1 and Q3. PySpark alternatives live in alternatives.py.
-- ${silver_schema} / ${gold_schema} are catalog.schema values from the pipeline configuration.

-- Q1: mean and standard deviation of US population, 2013-2018 inclusive.
-- stddev() in Spark is the sample standard deviation; the population variant is included as well.
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_population_stats (
  CONSTRAINT all_six_years_present EXPECT (years = 6) ON VIOLATION FAIL UPDATE
)
COMMENT 'Q1: mean / stddev of annual US population 2013-2018.'
AS
SELECT
  count(*) AS years,
  avg(population) AS mean_population,
  stddev_samp(population) AS stddev_population,
  stddev_pop(population) AS stddev_population_pop
FROM ${silver_schema}.silver_population
WHERE year BETWEEN 2013 AND 2018;

-- Q3: PRS30006032 / Q01 value per year, with that year's population where available (LEFT JOIN).
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_series_population
COMMENT 'Q3: PRS30006032 Q01 value per year joined to US population.'
AS
SELECT o.series_id, o.year, o.period, o.value, p.population
FROM ${silver_schema}.silver_bls_observations o
LEFT JOIN ${silver_schema}.silver_population p ON o.year = p.year
WHERE o.series_id = 'PRS30006032' AND o.period = 'Q01';
