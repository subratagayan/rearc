"""Bronze: incremental ingestion of raw file versions with Auto Loader (streaming tables).

Auto Loader checkpoints which files it has already read, so each file version landed by ingest.py is
processed exactly once - the cost of a refresh is proportional to what changed, not to the total history.

Expected schema is enforced on the way in: a header that doesn't match the documented columns FAILS the
update (silently mis-parsing positional data would be worse), and rows with null keys are DROPPED.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW = spark.conf.get("raw_root")  # noqa: F821 - `spark` is provided by the pipeline runtime

DATA_COLUMNS = ["series_id", "year", "period", "value", "footnote_codes"]
SERIES_COLUMNS = [
    "series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal", "base_year",
    "footnote_codes", "series_title", "begin_year", "begin_period", "end_year", "end_period",
]
POPULATION_SCHEMA = "data ARRAY<STRUCT<`Nation ID`: STRING, Nation: STRING, Year: INT, Population: DOUBLE>>"


def auto_loader(fmt: str):
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt)  # noqa: F821


def read_bls(file_glob: str, columns: list[str]):
    """BLS files are tab-delimited, header first, with space-padded fields (see pr.txt)."""
    lines = auto_loader("text").load(f"{RAW}/bls/pr/{file_glob}/*/*").where("trim(value) <> ''")
    fields = F.transform(F.split("value", "\t"), lambda f: F.trim(f))
    path = F.col("_metadata.file_path")
    return lines.select(
        *[F.get(fields, i).alias(c) for i, c in enumerate(columns)],
        (F.get(fields, 0) == columns[0]).alias("is_header"),
        (fields == F.array(*[F.lit(c) for c in columns])).alias("header_ok"),
        F.size(fields).alias("field_count"),
        F.regexp_extract(path, r"/bls/pr/([^/]+)/([^/]+)/[^/]+$", 1).alias("file_name"),
        F.regexp_extract(path, r"/bls/pr/([^/]+)/([^/]+)/[^/]+$", 2).alias("version"),
    )


@dp.table(comment="Raw BLS observations from every version of every pr.data.* file.")
@dp.expect_or_fail("header_matches_expected_schema", "NOT is_header OR header_ok")
@dp.expect_all_or_drop({
    "expected_field_count": "is_header OR field_count = 5",
    "series_id_not_null": "is_header OR coalesce(series_id, '') <> ''",
    "year_not_null": "is_header OR coalesce(year, '') <> ''",
    "period_not_null": "is_header OR coalesce(period, '') <> ''",
})
def bronze_bls_data():
    return read_bls("*.data.*", DATA_COLUMNS)


@dp.table(comment="Raw BLS series catalogue (every version of pr.series).")
@dp.expect_or_fail("header_matches_expected_schema", "NOT is_header OR header_ok")
@dp.expect_or_drop("series_id_not_null", "is_header OR coalesce(series_id, '') <> ''")
def bronze_bls_series():
    return read_bls("*.series", SERIES_COLUMNS)


@dp.table(comment="Raw DataUSA population records, one row per record per fetched version.")
@dp.expect_all_or_drop({
    "nation_id_not_null": "nation_id IS NOT NULL",
    "year_not_null": "year IS NOT NULL",
    "population_not_null": "population IS NOT NULL",
})
def bronze_population():
    return (
        auto_loader("json")
        .option("multiLine", "true")
        .schema(POPULATION_SCHEMA)
        .load(f"{RAW}/datausa/population/*/*")
        .select(F.explode("data").alias("r"), F.col("_metadata.file_path").alias("path"))
        .select(
            F.col("r.`Nation ID`").alias("nation_id"),
            F.col("r.Nation").alias("nation"),
            F.col("r.Year").alias("year"),
            F.col("r.Population").alias("population"),
            F.regexp_extract("path", r"/datausa/population/([^/]+)/", 1).alias("version"),
        )
    )


@dp.materialized_view(comment="Which version of each source file is currently live (written by ingest.py).")
def active_files():
    return spark.read.schema(  # noqa: F821
        "source STRING, file_name STRING, version STRING, modified STRING, size BIGINT, sha256 STRING, path STRING"
    ).json(f"{RAW}/_state/active_files.json")
