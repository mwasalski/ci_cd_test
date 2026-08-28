"""Schema-drift tests.

`detect_drift` is a pure function over two StructTypes, so most of these run
without touching Spark at all -- milliseconds, not seconds.
"""

from __future__ import annotations

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from collections_platform.ingest import (
    SchemaDriftError,
    detect_drift,
    enforce_drift_policy,
)
from collections_platform.schemas import PORTFOLIO_CASE_RAW

BASE = StructType(
    [
        StructField("case_reference", StringType()),
        StructField("portfolio_id", StringType()),
        StructField("current_balance", StringType()),
    ]
)


def test_no_drift():
    report = detect_drift(BASE, BASE)
    assert not report.is_breaking
    assert report.added == [] and report.missing == []


def test_additive_whitelisted_passes():
    actual = StructType(BASE.fields + [StructField("originator_score", StringType())])
    enforce_drift_policy(detect_drift(actual, BASE))  # must not raise


def test_additive_unwhitelisted_fails():
    """A new column nobody has looked at is not automatically safe -- it might be
    PII, it might be the column that changes the grain."""
    actual = StructType(BASE.fields + [StructField("mystery_column", StringType())])
    with pytest.raises(SchemaDriftError, match="unwhitelisted"):
        enforce_drift_policy(detect_drift(actual, BASE))


def test_missing_column_fails():
    actual = StructType([f for f in BASE.fields if f.name != "current_balance"])
    with pytest.raises(SchemaDriftError, match="disappeared"):
        enforce_drift_policy(detect_drift(actual, BASE))


def test_type_change_fails():
    """The dangerous one: mergeSchema would happily accept this and every
    downstream decimal comparison would start lying."""
    actual = StructType(
        [
            StructField("case_reference", StringType()),
            StructField("portfolio_id", StringType()),
            StructField("current_balance", IntegerType()),
        ]
    )
    report = detect_drift(actual, BASE)
    assert report.type_changed == [("current_balance", "string", "int")]
    with pytest.raises(SchemaDriftError, match="type changed"):
        enforce_drift_policy(report)


def test_rename_is_reported_as_missing_plus_added():
    """A rename is indistinguishable from drop+add. Only a human can tell, which
    is exactly why this must fail loudly instead of being auto-handled."""
    actual = StructType(
        [
            StructField("case_reference", StringType()),
            StructField("portfolio_id", StringType()),
            StructField("balance_current", StringType()),   # renamed
        ]
    )
    report = detect_drift(actual, BASE)
    assert report.missing == ["current_balance"]
    assert report.added == ["balance_current"]
    with pytest.raises(SchemaDriftError):
        enforce_drift_policy(report)


def test_column_order_change_is_not_drift():
    """Reordering columns is not a contract change -- do not fail on it, or you
    will train people to ignore drift alerts."""
    reordered = StructType(list(reversed(BASE.fields)))
    assert not detect_drift(reordered, BASE).is_breaking


def test_real_schema_against_itself():
    assert not detect_drift(PORTFOLIO_CASE_RAW, PORTFOLIO_CASE_RAW).is_breaking
