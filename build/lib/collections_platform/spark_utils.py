"""Spark helpers: session, skew mitigation, Delta writes."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .observability import log_event


def get_spark(app_name: str = "collections_platform") -> SparkSession:
    """On Databricks this returns the existing session; locally it builds one.

    `getOrCreate` is what makes the same code runnable in a pytest fixture and on
    a cluster without a branch.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


# ---------------------------------------------------------------------------
# Skew
# ---------------------------------------------------------------------------
def measure_skew(df: DataFrame, key: str, top_n: int = 10) -> list[tuple[str, int]]:
    """Answer 'is this key skewed' with numbers before reaching for a fix.

    In collections, skew is structural, not accidental: one bought portfolio can
    hold 40% of all cases, and a handful of clients generate most of the servicing
    volume. Expect it and design for it.
    """
    rows = (
        df.groupBy(key)
        .count()
        .orderBy(F.col("count").desc())
        .limit(top_n)
        .collect()
    )
    result = [(str(r[key]), int(r["count"])) for r in rows]
    log_event("skew.measured", key=key, top_keys=result)
    return result


def salt(df: DataFrame, key: str, n_salts: int = 16) -> DataFrame:
    """Add a salt to break a hot key across `n_salts` reducers.

    IMPORTANT: try AQE first. `spark.sql.adaptive.skewJoin.enabled=true` splits
    skewed partitions automatically and costs you nothing. Salting is the manual
    fallback for the cases AQE does not catch -- notably skewed *aggregations*
    and window functions, where AQE's skew handling does not apply the way it
    does to sort-merge joins.

    Salting also changes your result cardinality, so it always needs a second
    aggregation pass. That is the cost.
    """
    return df.withColumn("_salt", (F.rand() * n_salts).cast("int")).withColumn(
        "_salted_key", F.concat_ws("#", F.col(key), F.col("_salt"))
    )


def explode_salt(df: DataFrame, key: str, n_salts: int = 16) -> DataFrame:
    """Explode the small side of a salted join so every salt value has a match."""
    return df.withColumn(
        "_salt", F.explode(F.array(*[F.lit(i) for i in range(n_salts)]))
    ).withColumn("_salted_key", F.concat_ws("#", F.col(key), F.col("_salt")))


def aggregate_skewed(df: DataFrame, key: str, agg_col: str, n_salts: int = 16) -> DataFrame:
    """Two-stage aggregation: partial per salted key, then final per real key.

    This is the pattern AQE will not do for you.
    """
    salted = salt(df, key, n_salts)
    partial = salted.groupBy("_salted_key", key).agg(F.sum(agg_col).alias("_partial"))
    return partial.groupBy(key).agg(F.sum("_partial").alias(agg_col))


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------
def write_delta(
    df: DataFrame,
    table: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Managed UC table write.

    Deliberately NOT partitioning by default. Partitioning a 50 GB table by day
    gives you thousands of tiny files and a slower table than no partitioning at
    all. Under ~1 TB, liquid clustering or plain Delta with Z-order beats manual
    partitioning almost every time.
    """
    writer = df.write.format("delta").mode(mode).option("mergeSchema", "false")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table)
    log_event("write.delta", table=table, mode=mode, partition_by=partition_by or [])
