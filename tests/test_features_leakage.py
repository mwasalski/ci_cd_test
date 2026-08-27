"""Leakage tests -- the most valuable file in this repo.

The core idea, and it generalises to any point-in-time feature build:

    Build features at T.
    Insert a new fact dated AFTER T.
    Rebuild features at T.
    The two feature sets MUST be byte-identical.

If they differ, the feature build can see the future. This test catches leakage
that no amount of eyeballing the SQL will.

The same file runs that test against the naive implementation and asserts it
FAILS -- so the test itself is proven to have teeth. A leakage test that passes
against a known-leaky implementation is worse than no test, because it is
reassuring and wrong.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from collections_platform.features import (
    build_features_naive,
    build_features_pit,
    build_label,
    build_training_set,
)

AS_OF = date(2024, 3, 1)

PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("case_reference", StringType(), False),
        StructField("payment_date", DateType(), False),
        StructField("amount", DecimalType(18, 2), False),
    ]
)


@pytest.fixture
def cases(spark):
    return spark.createDataFrame(
        [("CASE_1", "P0"), ("CASE_2", "P0"), ("CASE_3", "P1")],
        "case_reference string, portfolio_id string",
    )


@pytest.fixture
def payments_before(spark):
    """Payments strictly before AS_OF."""
    return spark.createDataFrame(
        [
            ("P1", "CASE_1", date(2024, 1, 10), 100.00),
            ("P2", "CASE_1", date(2024, 2, 10), 150.00),
            ("P3", "CASE_2", date(2023, 12, 1), 50.00),
            # CASE_3 has never paid.
        ],
        PAYMENT_SCHEMA,
    )


def _feature_rows(df):
    """Deterministic, comparable snapshot of the feature output."""
    cols = sorted(c for c in df.columns if c not in {"as_of_date"})
    return [tuple(r) for r in df.select(*cols).orderBy("case_reference").collect()]


# ---------------------------------------------------------------------------
# THE test
# ---------------------------------------------------------------------------
def test_pit_features_are_immune_to_future_payments(spark, cases, payments_before):
    baseline = _feature_rows(build_features_pit(payments_before, cases, AS_OF))

    future = spark.createDataFrame(
        [
            ("P99", "CASE_1", date(2024, 5, 1), 9999.00),   # inside the label window
            ("P98", "CASE_3", date(2024, 3, 15), 500.00),   # first ever payment, after T
            ("P97", "CASE_2", AS_OF, 1.00),                 # exactly ON the boundary
        ],
        PAYMENT_SCHEMA,
    )
    with_future = build_features_pit(payments_before.unionByName(future), cases, AS_OF)

    assert _feature_rows(with_future) == baseline, (
        "Feature values changed when future payments were added -> the feature "
        "build can see past as_of_date."
    )


def test_naive_features_do_leak(spark, cases, payments_before):
    """Proves the test above has teeth by running it against the known-bad version."""
    baseline = _feature_rows(build_features_naive(payments_before, cases, AS_OF))
    future = spark.createDataFrame(
        [("P99", "CASE_1", date(2024, 5, 1), 9999.00)], PAYMENT_SCHEMA
    )
    with_future = _feature_rows(
        build_features_naive(payments_before.unionByName(future), cases, AS_OF)
    )
    assert with_future != baseline, "naive build was expected to leak but did not"


def test_boundary_is_strictly_less_than(spark, cases, payments_before):
    """A payment dated exactly AS_OF must be excluded from features.

    In a nightly batch you do not have today's payments when scoring. Off-by-one
    here is a genuine leak, and it is invisible in aggregate metrics.
    """
    on_boundary = spark.createDataFrame(
        [("PB", "CASE_1", AS_OF, 777.00)], PAYMENT_SCHEMA
    )
    out = build_features_pit(payments_before.unionByName(on_boundary), cases, AS_OF)
    row = out.filter(F.col("case_reference") == "CASE_1").collect()[0]
    assert float(row["amount_paid_lifetime"]) == 250.00, "boundary payment leaked into features"


def test_never_paid_case_has_null_recency_not_zero(spark, cases, payments_before):
    """`days_since_last_payment` for a case that never paid must be NULL.

    Coalescing it to 0 tells the model 'paid today', which is the exact opposite
    of the truth. This is the most common fillna() bug in feature engineering.
    """
    out = build_features_pit(payments_before, cases, AS_OF)
    row = out.filter(F.col("case_reference") == "CASE_3").collect()[0]
    assert row["days_since_last_payment"] is None
    assert row["payments_count_lifetime"] == 0     # count of zero IS correct
    assert row["has_ever_paid"] is False


# ---------------------------------------------------------------------------
# Label
# ---------------------------------------------------------------------------
def test_label_window_is_forward_only(spark, cases, payments_before):
    """Payments before as_of must never set the label to True."""
    labels = build_label(payments_before, cases, AS_OF).collect()
    assert all(not r["label_paid_90d"] for r in labels), (
        "a payment before as_of set the forward-looking label"
    )


def test_label_respects_horizon_end(spark, cases, payments_before):
    """A payment at as_of + 91 days is outside the 90-day horizon."""
    late = spark.createDataFrame(
        [("PL", "CASE_1", date(2024, 6, 15), 100.00)], PAYMENT_SCHEMA   # ~106 days out
    )
    labels = {
        r["case_reference"]: r["label_paid_90d"]
        for r in build_label(payments_before.unionByName(late), cases, AS_OF).collect()
    }
    assert labels["CASE_1"] is False


def test_feature_and_label_windows_are_disjoint(spark, cases, payments_before):
    """End-to-end: a single payment inside the label window must move the label
    and must NOT move any feature."""
    mid_horizon = spark.createDataFrame(
        [("PM", "CASE_3", date(2024, 4, 1), 300.00)], PAYMENT_SCHEMA
    )
    all_payments = payments_before.unionByName(mid_horizon)

    ts = build_training_set(all_payments, cases, AS_OF)
    row = ts.filter(F.col("case_reference") == "CASE_3").collect()[0]

    assert row["label_paid_90d"] is True, "label should see the horizon payment"
    assert row["payments_count_lifetime"] == 0, "features must NOT see the horizon payment"
    assert float(row["amount_paid_lifetime"]) == 0.0
