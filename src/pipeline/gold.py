"""Gold (primary = PySpark) for Q2. The Spark SQL alternative lives in alternatives.sql."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(comment="Q2: for every series, the year with the largest sum of quarterly values.")
def gold_best_year_by_series():
    # Q05 is the annual average (pr.period), not a quarter - including it would double count.
    quarterly = spark.read.table("silver_bls_observations").where(F.col("period") != "Q05")  # noqa: F821
    yearly = quarterly.groupBy("series_id", "year").agg(F.sum("value").alias("value"))
    # Ties go to the earliest year so the answer is deterministic.
    best = (
        yearly.withColumn("rank", F.row_number().over(Window.partitionBy("series_id").orderBy(F.desc("value"), "year")))
        .where("rank = 1")
        .drop("rank")
    )
    series = spark.read.table("silver_bls_series").select("series_id", "series_title")  # noqa: F821
    return best.join(series, "series_id", "left").select("series_id", "series_title", "year", "value")
