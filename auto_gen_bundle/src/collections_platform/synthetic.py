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
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from .schemas import PAYMENT_RAW, PORTFOLIO_CASE_RAW


def generate_cases(
    spark: SparkSession,
    n_cases: int = 100_000,
    n_portfolios: int = 20,
    skew_factor: float = 0.4,
    serviced_share: float = 0.3,
    null_rate: float = 0.05,
    dup_rate: float = 0.02,
    seed: int = 42,
) -> DataFrame:
    """Cases with a deliberately hot portfolio, covering BOTH fact domains.

    `skew_factor=0.4` puts 40% of all cases on PORTFOLIO_000. That is not
    unrealistic -- one large bought portfolio really does dominate.

    `serviced_share=0.3` makes the last 30% third-party placements: they carry a
    `client_id`, a `placed_date` and a `first_contact_date`, and their
    `portfolio_id` is a placement batch we never bought. That is what lets the
    Investing and Servicing facts be built from one case table without either
    domain borrowing the other's rows -- the Investing join drops placements
    (no matching portfolio -> no months_on_book), and the Servicing build
    filters on `client_id IS NOT NULL`.
    """
    owned_share = 1.0 - serviced_share
    df = spark.range(0, n_cases).withColumn("_r", F.rand(seed))

    df = (
        df.withColumn(
            "portfolio_id",
            F.when(F.col("_r") < skew_factor, F.lit("PORTFOLIO_000"))
            .when(
                F.col("_r") < owned_share,
                F.concat(
                    F.lit("PORTFOLIO_"),
                    F.lpad((F.rand(seed + 1) * (n_portfolios - 1) + 1).cast("int"), 3, "0"),
                ),
            )
            .otherwise(
                F.concat(
                    F.lit("PLACEMENT_"),
                    F.lpad((F.rand(seed + 14) * 5).cast("int"), 3, "0"),
                )
            ),
        )
        .withColumn(
            "client_id",
            F.when(
                F.col("_r") >= owned_share,
                F.concat(
                    F.lit("CLIENT_"),
                    F.element_at(
                        F.array(F.lit("A"), F.lit("B"), F.lit("C"), F.lit("D"), F.lit("E")),
                        (F.rand(seed + 15) * 5).cast("int") + 1,
                    ),
                ),
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
            "date_of_birth",
            F.when(F.rand(seed + 16) < null_rate, F.lit(None).cast(DateType())).otherwise(
                F.date_sub(F.lit(date(1990, 1, 1)), (F.rand(seed + 17) * 14000).cast("int"))
            ),
        )
        .withColumn(
            "current_balance",
            # 1% of rows get a 100x unit error (grosze recorded as zloty).
            F.when(F.rand(seed + 8) < 0.01, F.col("original_balance") * 100)
            .otherwise(F.col("original_balance") * F.rand(seed + 9))
            .cast(DecimalType(18, 2)),
        )
        .withColumn(
            "default_date",
            F.when(
                F.rand(seed + 10) < 0.005, F.lit(date(1970, 1, 1))  # epoch-zero leak
            ).otherwise(F.date_sub(F.lit(date(2024, 1, 1)), (F.rand(seed + 11) * 1500).cast("int"))),
        )
        # Servicing-only dates. NULL on owned cases, which is what makes
        # `sla_met` three-valued rather than a silent False.
        .withColumn(
            "placed_date",
            F.when(
                F.col("client_id").isNotNull(),
                F.date_add(F.col("default_date"), (F.rand(seed + 18) * 370 + 30).cast("int")),
            ),
        )
        .withColumn(
            "first_contact_date",
            # 10% of placements were never contacted -> SLA is PENDING, not BREACHED.
            F.when(
                F.col("placed_date").isNotNull() & (F.rand(seed + 19) >= 0.10),
                F.date_add(F.col("placed_date"), (F.rand(seed + 20) * 60).cast("int")),
            ),
        )
        .withColumn(
            "currency",
            F.when(F.rand(seed + 12) < 0.002, F.lit("XXX")).otherwise(  # unmapped currency
                F.element_at(F.array(F.lit("PLN"), F.lit("EUR"), F.lit("SEK")),
                             (F.rand(seed + 13) * 3).cast("int") + 1)
            ),
        )
        .withColumn("product_type", F.lit("CONSUMER_LOAN"))
        # Emit exactly the contract, in contract order. A generator that drifts
        # from PORTFOLIO_CASE_RAW would make `enforce_drift_policy` fail the
        # ingest job -- which is the correct behaviour, and a slow way to find
        # out you edited only one of the two.
        .select(*[f.name for f in PORTFOLIO_CASE_RAW.fields])
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
        .select(*[f.name for f in PAYMENT_RAW.fields])
    )


# ---------------------------------------------------------------------------
# Reference data
#
# These three are NOT generated into the landing volume: they are reference /
# contract data, not an originator feed. `seed_reference_data` writes them
# straight into the tables `bootstrap` declared, with the declared types.
# ---------------------------------------------------------------------------
def generate_portfolios(spark: SparkSession, n_portfolios: int = 20) -> DataFrame:
    """Bought NPL portfolios. Purchase dates are spread so `months_on_book`
    actually varies -- a curve where every portfolio sits at the same month
    proves nothing."""
    return (
        spark.range(0, n_portfolios)
        .withColumn(
            "portfolio_id", F.concat(F.lit("PORTFOLIO_"), F.lpad(F.col("id"), 3, "0"))
        )
        # Bought before the payment history starts, otherwise every payment is
        # a "pre-purchase" row and the fact table comes out empty.
        .withColumn(
            "purchase_date",
            F.date_add(F.lit(date(2020, 1, 1)), (F.col("id") * 45).cast("int")),
        )
        .withColumn(
            "gross_face_value",
            ((F.col("id") + 1) * 2_500_000).cast(DecimalType(18, 2)),
        )
        .withColumn(
            "purchase_price",
            (F.col("gross_face_value") * F.lit(0.11)).cast(DecimalType(18, 2)),
        )
        .withColumn("seller_name", F.concat(F.lit("Originator "), F.col("id")))
        .withColumn(
            "country_code",
            F.element_at(
                F.array(F.lit("PL"), F.lit("SE"), F.lit("DE")),
                (F.col("id") % 3 + 1).cast("int"),
            ),
        )
        .select(
            "portfolio_id",
            "purchase_date",
            "purchase_price",
            "gross_face_value",
            "seller_name",
            "country_code",
        )
    )


def generate_forecast_curve(
    spark: SparkSession, n_portfolios: int = 20, max_months: int = 84
) -> DataFrame:
    """Underwriting recovery curve: cumulative % of face value expected by month N.

    Monotonic and flattening, like a real curve. Capped below 1.0 -- an NPL
    portfolio bought at 11 cents does not recover 100% of face value.
    """
    return (
        spark.range(0, n_portfolios)
        .withColumn(
            "portfolio_id", F.concat(F.lit("PORTFOLIO_"), F.lpad(F.col("id"), 3, "0"))
        )
        .withColumn("months_on_book", F.explode(F.sequence(F.lit(0), F.lit(max_months))))
        .withColumn(
            "forecast_pct",
            # 0 -> 0, then asymptotic towards ~0.30 of face value.
            (F.lit(0.30) * (F.lit(1) - F.exp(-F.col("months_on_book") / F.lit(24.0))))
            .cast(DecimalType(9, 6)),
        )
        .select("portfolio_id", "months_on_book", "forecast_pct")
    )


def generate_client_contracts(spark: SparkSession) -> DataFrame:
    """SCD2 servicing contracts: every client renegotiated on 2024-01-01.

    Two rows per client on purpose. A flat current-state table would let
    `transform_servicing` restate 2023 revenue at the 2024 rate and nobody would
    notice until audit -- see tests/test_transforms.py.
    """
    schema = StructType(
        [
            StructField("client_id", StringType(), False),
            StructField("commission_rate", DecimalType(9, 6), False),
            StructField("sla_target_days", IntegerType(), True),
            StructField("valid_from", DateType(), False),
            StructField("valid_to", DateType(), True),
        ]
    )
    old_rate = {"A": "0.150000", "B": "0.140000", "C": "0.180000", "D": "0.120000", "E": "0.160000"}
    new_rate = {"A": "0.100000", "B": "0.130000", "C": "0.150000", "D": "0.110000", "E": "0.125000"}
    cutover = date(2024, 1, 1)
    rows = []
    for suffix, rate in old_rate.items():
        client = f"CLIENT_{suffix}"
        rows.append((client, Decimal(rate), 30, date(2020, 1, 1), cutover))
        rows.append((client, Decimal(new_rate[suffix]), 21, cutover, None))
    return spark.createDataFrame(rows, schema)


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
    # Decimal, not float: Spark 4 (serverless environment 5) rejects a Python
    # float for a DecimalType field outright instead of silently converting it.
    rows = [
        ("CASE_1", "P0", Decimal("1000.00"), Decimal("1000.00"), "PLN", date(2023, 1, 1),
         "80010112345", "a@x.invalid", "+48601234567"),
        ("CASE_2", "P0", Decimal("0.00"), Decimal("500.00"), "EUR", date(2023, 6, 1),
         None, None, None),
        ("CASE_3", "P1", Decimal("-50.00"), Decimal("500.00"), "PLN", date(2023, 6, 1),
         "90020254321", "b@x.invalid", "12"),
        ("CASE_4", "P1", Decimal("500.00"), Decimal("400.00"), "XXX", date(2099, 1, 1),
         "70030367890", None, "+48501112233"),
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
        ("P1", "CASE_1", date(2024, 1, 10), Decimal("100.00")),
        ("P2", "CASE_1", date(2024, 2, 10), Decimal("150.00")),
        # AFTER as_of=2024-03-01 -> label window
        ("P3", "CASE_1", date(2024, 5, 10), Decimal("200.00")),
        ("P4", "CASE_2", date(2024, 2, 20), Decimal("50.00")),
        ("P5", "CASE_4", date(2024, 4, 1), Decimal("999.00")),   # after as_of -> must not leak
    ]
    return spark.createDataFrame(rows, schema)


def python_random_seeded(seed: int = 42) -> random.Random:
    return random.Random(seed)
