"""Silver: incremental insert/update/delete with AUTO CDC FROM SNAPSHOT (SCD type 1 streaming tables).

BLS and DataUSA publish full snapshots, not change events. Each `*_snapshot` view is the current snapshot:
bronze rows from the active file versions only, trimmed, typed and de-duplicated to one row per key.
On every pipeline update AUTO CDC FROM SNAPSHOT diffs that snapshot against the previous one and applies
only the differences to the target table:
    new key -> INSERT, changed values -> UPDATE, key gone (row or whole file removed upstream) -> DELETE.
Unchanged rows are never rewritten, so the gold materialized views only see the rows that changed.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER = spark.conf.get("silver_schema")  # noqa: F821 - catalog.schema; bronze is the pipeline default

# Row tracking + deletion vectors let the gold materialized views refresh incrementally.
TABLE_PROPERTIES = {"delta.enableRowTracking": "true", "delta.enableDeletionVectors": "true"}


def active(source: str):
    """File versions that are currently live, according to ingest.py."""
    return spark.read.table("active_files").where(F.col("source") == source).select("file_name", "version")  # noqa: F821


def silver_table(name: str, keys: list[str], comment: str) -> None:
    target = f"{SILVER}.{name}"
    dp.create_streaming_table(target, comment=comment, table_properties=TABLE_PROPERTIES)
    dp.create_auto_cdc_from_snapshot_flow(
        target=target, source=f"{name}_snapshot", keys=keys, stored_as_scd_type=1
    )


@dp.temporary_view()
@dp.expect_all_or_drop({
    "valid_year": "year IS NOT NULL",
    "valid_period": "period RLIKE '^Q0[1-5]$'",
    "valid_value": "value IS NOT NULL",
})
def silver_bls_observations_snapshot():
    rows = (
        spark.read.table("bronze_bls_data")  # noqa: F821
        .where(~F.col("is_header"))
        .join(active("bls_pr"), ["file_name", "version"])
        .select(
            "series_id",
            F.expr("try_cast(year AS INT)").alias("year"),
            "period",
            # DECIMAL keeps sums exact (BLS stores values to at most 3 decimals).
            F.expr("try_cast(value AS DECIMAL(18, 3))").alias("value"),
            F.expr("nullif(footnote_codes, '')").alias("footnote_codes"),
            "file_name",
        )
    )
    # pr.data.0.Current and pr.data.1.AllData overlap; AUTO CDC needs one row per key.
    w = Window.partitionBy("series_id", "year", "period").orderBy(F.desc("file_name"))
    return rows.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


silver_table(
    "silver_bls_observations",
    ["series_id", "year", "period"],
    "BLS PR observations, one row per series_id/year/period. Q05 = annual average (see pr.period).",
)


@dp.temporary_view()
@dp.expect_or_drop("series_id_not_null", "series_id IS NOT NULL AND series_id <> ''")
def silver_bls_series_snapshot():
    return (
        spark.read.table("bronze_bls_series")  # noqa: F821
        .where(~F.col("is_header"))
        .join(active("bls_pr"), ["file_name", "version"])
        .select(
            "series_id", "series_title", "sector_code", "measure_code", "duration_code", "seasonal",
            F.expr("try_cast(begin_year AS INT)").alias("begin_year"),
            F.expr("try_cast(end_year AS INT)").alias("end_year"),
        )
        .dropDuplicates(["series_id"])
    )


silver_table("silver_bls_series", ["series_id"], "BLS PR series catalogue with the human-readable series_title.")


@dp.temporary_view()
@dp.expect_or_drop("valid_population", "population > 0")
def silver_population_snapshot():
    return (
        spark.read.table("bronze_population")  # noqa: F821
        .join(active("datausa").select("version"), "version")
        .select("nation_id", "nation", "year", F.col("population").cast("bigint").alias("population"))
        .dropDuplicates(["nation_id", "year"])
    )


silver_table(
    "silver_population",
    ["nation_id", "year"],
    "US population by year (ACS 1-year estimate, Census table B01003, via DataUSA).",
)
