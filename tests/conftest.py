"""Shared fixtures.

The session-scoped Spark fixture is the single biggest lever on test speed:
creating a SparkSession costs ~5-15s, so a function-scoped fixture turns a
40-test suite into a 7-minute one.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("collections_platform_tests")
        # 200 shuffle partitions on a 4-row DataFrame means 200 empty tasks per
        # shuffle. This one line is usually a 5-10x speedup on a unit suite.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        # Left OFF deliberately, to match the DBR 15.4 default. Turning it ON
        # locally would make a bad cast raise instead of returning NULL -- which
        # is better behaviour, but it would mean your tests exercise semantics
        # your cluster does not have. Change it in both places or neither.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def pepper() -> str:
    return "test-pepper-not-a-real-secret"
