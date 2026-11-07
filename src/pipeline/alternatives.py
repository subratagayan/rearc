"""Documented alternatives (PySpark) for Q1 and Q3, whose primary versions are in gold.sql.

They run in the pipeline too, so both implementations can be compared side by side.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Q1 alternative in PySpark - must match gold_population_stats.")
def alt_population_stats_py():
    return (
        spark.read.table("silver_population")  # noqa: F821
        .where(F.col("year").between(2013, 2018))
        .agg(
            F.count("*").alias("years"),
            F.avg("population").alias("mean_population"),
            F.stddev_samp("population").alias("stddev_population"),
            F.stddev_pop("population").alias("stddev_population_pop"),
        )
    )


@dp.materialized_view(comment="Q3 alternative in PySpark - must match gold_series_population.")
def alt_series_population_py():
    obs = spark.read.table("silver_bls_observations").where(  # noqa: F821
        (F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01")
    )
    pop = spark.read.table("silver_population").select("year", "population")  # noqa: F821
    return obs.join(pop, "year", "left").select("series_id", "year", "period", "value", "population")
