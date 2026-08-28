"""Propensity-to-pay features, built point-in-time correct.

This module deliberately ships TWO implementations of the same feature set so the
difference is visible and testable.

  build_features_naive()  -- the version that gets written first, everywhere.
  build_features_pit()    -- the version that survives contact with production.

They differ by one predicate. That predicate is the whole job.

------------------------------------------------------------------------------
Why leakage is *especially* nasty in collections
------------------------------------------------------------------------------
The label horizon is long. "Did this debtor pay within 90 days of as_of_date?"
means the label window is [T, T+90]. Any feature that aggregates payments without
an upper bound of T will include payments from inside the label window, so the
model learns "people who paid, paid". Offline AUC goes to 0.95, production lift
is zero, and nobody can explain why.

The tell is always the same: a model that is far too good and a feature named
something like `total_paid` or `last_payment_amount` sitting at the top of the
importance list.

------------------------------------------------------------------------------
The second trap: as_of_date must be a parameter
------------------------------------------------------------------------------
If the code calls current_date() internally you cannot backfill, you cannot
reproduce yesterday's training set, and your training/serving skew is unbounded.
as_of_date is passed in from the job parameter -- see resources/pipeline.job.yml.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LABEL_HORIZON_DAYS = 90


# ---------------------------------------------------------------------------
# MID VERSION -- keep it, and keep the test that proves it is wrong.
# ---------------------------------------------------------------------------
def build_features_naive(payments: DataFrame, cases: DataFrame, as_of: date) -> DataFrame:
    """Aggregates over the whole payment history with no time bound.

    Looks correct. Passes every schema/null test. Leaks the label.
    """
    agg = payments.groupBy("case_reference").agg(
        F.count("*").alias("payment_count"),
        F.sum("amount").alias("total_paid"),
        F.max("payment_date").alias("last_payment_date"),
    )
    return cases.join(agg, on="case_reference", how="left").withColumn(
        "as_of_date", F.lit(as_of)
    )


# ---------------------------------------------------------------------------
# SENIOR VERSION
# ---------------------------------------------------------------------------
def build_features_pit(payments: DataFrame, cases: DataFrame, as_of: date) -> DataFrame:
    """Every aggregate is bounded by `payment_date < as_of`.

    Strictly `<`, not `<=`: a payment posted on the as-of date is, in a nightly
    batch, information you did not have when the decision was made. Off-by-one on
    this boundary is a real, subtle leak.
    """
    as_of_col = F.lit(as_of)

    # 1. Bound the fact table FIRST. Filtering before the aggregation also means
    #    Spark can push the predicate down to the Delta scan and skip files
    #    entirely via min/max stats -- correctness and performance agree here.
    history = payments.filter(F.col("payment_date") < as_of_col)

    agg = history.groupBy("case_reference").agg(
        F.count("*").alias("payments_count_lifetime"),
        F.sum("amount").alias("amount_paid_lifetime"),
        F.max("payment_date").alias("last_payment_date"),
        F.min("payment_date").alias("first_payment_date"),
        # Recency-weighted windows: the last 30/90 days carry most of the signal.
        F.sum(
            F.when(F.col("payment_date") >= F.date_sub(as_of_col, 30), F.col("amount")).otherwise(0)
        ).alias("amount_paid_30d"),
        F.sum(
            F.when(F.col("payment_date") >= F.date_sub(as_of_col, 90), F.col("amount")).otherwise(0)
        ).alias("amount_paid_90d"),
        F.countDistinct(F.trunc(F.col("payment_date"), "month")).alias("distinct_months_paid"),
    )

    # 2. Payment regularity: is this a payment plan holding, or sporadic payments?
    #    Computed from bounded history only.
    gap_window = Window.partitionBy("case_reference").orderBy("payment_date")
    gaps = (
        history.withColumn(
            "prev_payment_date", F.lag("payment_date").over(gap_window)
        )
        .withColumn("gap_days", F.datediff(F.col("payment_date"), F.col("prev_payment_date")))
        .groupBy("case_reference")
        .agg(
            F.avg("gap_days").alias("avg_days_between_payments"),
            F.stddev_samp("gap_days").alias("stddev_days_between_payments"),
        )
    )

    # 3. Case attributes must also be as-of. `cases` here is expected to be an
    #    SCD2 dimension; taking the row valid at as_of, not the current row.
    cases_as_of = (
        cases.filter(
            (F.col("valid_from") <= as_of_col)
            & (F.coalesce(F.col("valid_to"), F.lit(date(9999, 12, 31))) > as_of_col)
        )
        if "valid_from" in cases.columns
        else cases
    )

    return (
        cases_as_of.join(agg, on="case_reference", how="left")
        .join(gaps, on="case_reference", how="left")
        .withColumn("as_of_date", as_of_col)
        # Fill only where zero is the semantically correct value. A case with no
        # payments genuinely has paid 0 -- but `days_since_last_payment` is
        # unknown, not 0, and coalescing it to 0 would tell the model this
        # debtor paid today.
        .fillna(
            {
                "payments_count_lifetime": 0,
                "amount_paid_lifetime": 0,
                "amount_paid_30d": 0,
                "amount_paid_90d": 0,
                "distinct_months_paid": 0,
            }
        )
        .withColumn(
            "days_since_last_payment",
            F.datediff(as_of_col, F.col("last_payment_date")),  # stays NULL if never paid
        )
        .withColumn("has_ever_paid", F.col("payments_count_lifetime") > 0)
        .drop("first_payment_date")
    )


def build_label(payments: DataFrame, cases: DataFrame, as_of: date) -> DataFrame:
    """Label: did the case pay anything in [as_of, as_of + 90d)?

    Note the window is strictly forward-looking and disjoint from the feature
    window. Writing them in the same file, next to each other, is intentional --
    it makes an overlap obvious in code review.
    """
    as_of_col = F.lit(as_of)
    horizon_end = F.date_add(as_of_col, LABEL_HORIZON_DAYS)

    future = payments.filter(
        (F.col("payment_date") >= as_of_col) & (F.col("payment_date") < horizon_end)
    )
    paid = future.groupBy("case_reference").agg(
        F.sum("amount").alias("amount_paid_in_horizon")
    )
    return (
        cases.select("case_reference")
        .join(paid, on="case_reference", how="left")
        .withColumn("label_paid_90d", F.coalesce(F.col("amount_paid_in_horizon"), F.lit(0)) > 0)
        .select("case_reference", "label_paid_90d")
    )


def build_training_set(payments: DataFrame, cases: DataFrame, as_of: date) -> DataFrame:
    """Features + label joined on the same as-of date."""
    return build_features_pit(payments, cases, as_of).join(
        build_label(payments, cases, as_of), on="case_reference", how="inner"
    )
