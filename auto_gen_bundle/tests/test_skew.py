"""Skew tests.

These are not correctness tests -- they are *shape* tests. They prove the
generator really produces skew and that the salted aggregation returns the same
numbers as the plain one. The performance claim itself belongs in a benchmark on
a real cluster, not in a unit test on local[2].
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from collections_platform.spark_utils import aggregate_skewed, measure_skew, salt
from collections_platform.synthetic import generate_cases


@pytest.fixture(scope="module")
def skewed_cases(spark):
    # Small enough to run in a unit suite, skewed enough to be measurable.
    return generate_cases(spark, n_cases=5_000, n_portfolios=10, skew_factor=0.4).cache()


def test_generator_actually_produces_skew(skewed_cases):
    """Guard the guard: if the generator stops producing skew, every skew test
    below silently becomes vacuous."""
    top = measure_skew(skewed_cases, "portfolio_id", top_n=3)
    total = skewed_cases.count()
    assert top[0][0] == "PORTFOLIO_000"
    assert top[0][1] / total > 0.30, f"expected a hot key, got {top}"


def test_salting_spreads_the_hot_key(skewed_cases):
    salted = salt(skewed_cases, "portfolio_id", n_salts=16)
    hot = salted.filter(F.col("portfolio_id") == "PORTFOLIO_000")
    assert hot.select("_salted_key").distinct().count() == 16


def test_salted_aggregation_matches_plain_aggregation(skewed_cases):
    """The only thing that actually matters: the optimisation must not change
    the answer."""
    plain = (
        skewed_cases.groupBy("portfolio_id")
        .agg(F.sum("current_balance").alias("current_balance"))
        .orderBy("portfolio_id")
        .collect()
    )
    salted = (
        aggregate_skewed(skewed_cases, "portfolio_id", "current_balance", n_salts=16)
        .orderBy("portfolio_id")
        .collect()
    )
    assert len(plain) == len(salted)
    for p, s in zip(plain, salted, strict=True):
        assert p["portfolio_id"] == s["portfolio_id"]
        assert p["current_balance"] == s["current_balance"]


def test_null_key_is_visible_in_skew_measurement(spark):
    """NULL is a hot key too, and it is the one people forget to look for."""
    df = spark.createDataFrame(
        [(None,)] * 100 + [("A",)] * 5, "portfolio_id string"
    )
    top = measure_skew(df, "portfolio_id", top_n=2)
    assert top[0][0] == "None" and top[0][1] == 100
