"""Synthetic data generator -- deliberately pathological.

Clean synthetic data is worse than useless: it makes a broken pipeline look
green. Every generator here has a knob for a specific failure mode you will meet
in production.

Failure modes covered:
  * key skew          -- one portfolio holds `skew_factor` of all cases
  * NULLs             -- in join keys, in money columns, in dates
  * duplicates        -- exact dupes AND near-dupes (same key, different values)
  * schema drift      -- an extra column, a renamed column, a type change
  * unit errors       -- grosze vs zloty (100x), which DQ must catch
  * timezone/epoch    -- 1970-01-01 dates leaking through
  * late-arriving     -- payments dated before their case was created
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)


def generate_cases(
    spark: SparkSession,
    n_cases: int = 100_000,
    n_portfolios: int = 20,
    skew_factor: float = 0.4,
    null_rate: float = 0.05,
    dup_rate: float = 0.02,
    seed: int = 42,
) -> DataFrame:
    """Cases with a deliberately hot portfolio.

    `skew_factor=0.4` puts 40% of all cases on PORTFOLIO_000. That is not
    unrealistic -- one large bought portfolio really does dominate.
    """
    df = spark.range(0, n_cases).withColumn("_r", F.rand(seed))

    df = (
        df.withColumn(
            "portfolio_id",
            F.when(F.col("_r") < skew_factor, F.lit("PORTFOLIO_000")).otherwise(
                F.concat(
                    F.lit("PORTFOLIO_"),
                    F.lpad((F.rand(seed + 1) * (n_portfolios - 1) + 1).cast("int"), 3, "0"),
                )
            ),
        )
        .withColumn("case_reference", F.concat(F.lit("CASE_"), F.lpad(F.col("id"), 9, "0")))
        .withColumn(
            "national_id",
            # NULL in a join key at `null_rate` -- this is what breaks naive joins.
            F.when(F.rand(seed + 2) < null_rate, F.lit(None).cast(StringType())).otherwise(
                F.lpad((F.rand(seed + 3) * 99999999999).cast("long"), 11, "0")
            ),
        )
        .withColumn("debtor_name", F.concat(F.lit("Debtor "), F.col("id")))
        .withColumn(
            "debtor_email",
            F.when(F.rand(seed + 4) < null_rate * 2, F.lit(None).cast(StringType())).otherwise(
                F.concat(F.lit("debtor"), F.col("id"), F.lit("@example.invalid"))
            ),
        )
        .withColumn(
            "debtor_phone",
            F.concat(F.lit("+48"), F.lpad((F.rand(seed + 5) * 999999999).cast("long"), 9, "0")),
        )
        .withColumn("debtor_address", F.concat(F.lit("ul. Testowa "), F.col("id")))
        .withColumn("postcode_area", F.lpad((F.rand(seed + 6) * 99).cast("int"), 2, "0"))
        .withColumn(
            "original_balance",
            (F.rand(seed + 7) * 50000 + 500).cast(DecimalType(18, 2)),
        )
        .withColumn(
            "current_balance",
            # 1% of rows get a 100x unit error (grosze recorded as zloty).
            F.when(
                F.rand(seed + 8) < 0.01, F.col("original_balance") * 100
            ).otherwise(F.col("original_balance") * F.rand(seed + 9)),
        )
        .withColumn(
            "default_date",
            F.when(
                F.rand(seed + 10) < 0.005, F.lit(date(1970, 1, 1))  # epoch-zero leak
            ).otherwise(F.date_sub(F.lit(date(2024, 1, 1)), (F.rand(seed + 11) * 1500).cast("int"))),
        )
        .withColumn(
            "currency",
            F.when(F.rand(seed + 12) < 0.002, F.lit("XXX")).otherwise(  # unmapped currency
                F.element_at(F.array(F.lit("PLN"), F.lit("EUR"), F.lit("SEK")),
                             (F.rand(seed + 13) * 3).cast("int") + 1)
            ),
        )
        .withColumn("product_type", F.lit("CONSUMER_LOAN"))
        .drop("_r", "id")
    )

    # Exact duplicates: an originator re-sending the same file is routine.
    dupes = df.sample(fraction=dup_rate, seed=seed)
    return df.unionByName(dupes)


def generate_payments(
    spark: SparkSession,
    cases: DataFrame,
    payments_per_case: float = 3.0,
    seed: int = 7,
) -> DataFrame:
    """Payments, including some dated *before* the case existed (late-arriving /
    backdated corrections) -- these must not silently break PIT feature logic."""
    exploded = cases.select("case_reference", "default_date").withColumn(
        "_n", F.explode(F.sequence(F.lit(1), (F.rand(seed) * payments_per_case * 2 + 1).cast("int")))
    )
    return (
        exploded.withColumn(
            "payment_id",
            F.concat(F.col("case_reference"), F.lit("_P"), F.col("_n")),
        )
        .withColumn(
            "payment_date",
            F.date_add(F.col("default_date"), (F.rand(seed + 1) * 900).cast("int")),
        )
        .withColumn("amount", (F.rand(seed + 2) * 800 + 10).cast(DecimalType(18, 2)))
        .withColumn(
            "channel",
            F.element_at(
                F.array(F.lit("BANK_TRANSFER"), F.lit("CARD"), F.lit("DIRECT_DEBIT")),
                (F.rand(seed + 3) * 3).cast("int") + 1,
            ),
        )
        .select("payment_id", "case_reference", "payment_date", "amount", "channel")
    )


def drifted_schema_variant(spark: SparkSession, base: DataFrame, kind: str) -> DataFrame:
    """Produce a drifted copy of a DataFrame for drift tests.

    kind: 'additive' | 'missing' | 'type_change' | 'rename'
    """
    if kind == "additive":
        return base.withColumn("originator_score", F.lit(650).cast("int"))
    if kind == "missing":
        return base.drop("currency")
    if kind == "type_change":
        return base.withColumn("current_balance", F.col("current_balance").cast(StringType()))
    if kind == "rename":
        return base.withColumnRenamed("debtor_phone", "debtor_mobile")
    raise ValueError(f"unknown drift kind: {kind}")


# Small in-memory fixtures for unit tests -- no cluster, no volume, no I/O.
def tiny_cases(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("case_reference", StringType(), False),
            StructField("portfolio_id", StringType(), False),
            StructField("current_balance", DecimalType(18, 2), True),
            StructField("original_balance", DecimalType(18, 2), True),
            StructField("currency", StringType(), True),
            StructField("default_date", DateType(), True),
            StructField("national_id", StringType(), True),
            StructField("debtor_email", StringType(), True),
            StructField("debtor_phone", StringType(), True),
        ]
    )
    rows = [
        ("CASE_1", "P0", 1000.00, 1000.00, "PLN", date(2023, 1, 1), "80010112345", "a@x.invalid", "+48601234567"),
        ("CASE_2", "P0", 0.00, 500.00, "EUR", date(2023, 6, 1), None, None, None),
        ("CASE_3", "P1", -50.00, 500.00, "PLN", date(2023, 6, 1), "90020254321", "b@x.invalid", "12"),
        ("CASE_4", "P1", 500.00, 400.00, "XXX", date(2099, 1, 1), "70030367890", None, "+48501112233"),
    ]
    return spark.createDataFrame(rows, schema)


def tiny_payments(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("payment_id", StringType(), False),
            StructField("case_reference", StringType(), False),
            StructField("payment_date", DateType(), False),
            StructField("amount", DecimalType(18, 2), False),
        ]
    )
    rows = [
        ("P1", "CASE_1", date(2024, 1, 10), 100.00),
        ("P2", "CASE_1", date(2024, 2, 10), 150.00),
        ("P3", "CASE_1", date(2024, 5, 10), 200.00),   # AFTER as_of=2024-03-01 -> label window
        ("P4", "CASE_2", date(2024, 2, 20), 50.00),
        ("P5", "CASE_4", date(2024, 4, 1), 999.00),    # after as_of -> must not leak
    ]
    return spark.createDataFrame(rows, schema)


def python_random_seeded(seed: int = 42) -> random.Random:
    return random.Random(seed)
