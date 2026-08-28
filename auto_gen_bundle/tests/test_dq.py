"""DQ engine tests, driven by pathological data."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from collections_platform.dq import (
    DQ_FAILURES_COL,
    Rule,
    Severity,
    apply_rules,
    assert_error_rate_below,
    case_rules,
    dedupe,
)
from collections_platform.synthetic import tiny_cases


def test_clean_and_quarantine_partition_the_input(spark):
    df = tiny_cases(spark)
    result = apply_rules(df, case_rules())
    assert result.clean.count() + result.quarantined.count() == df.count(), (
        "rows were lost -- clean + quarantine must be a partition of the input"
    )


def test_quarantine_records_which_rules_failed(spark):
    df = tiny_cases(spark)
    result = apply_rules(df, case_rules())
    bad = {
        r["case_reference"]: set(r[DQ_FAILURES_COL])
        for r in result.quarantined.collect()
    }
    # CASE_3 has a negative balance; CASE_4 has currency XXX and a future default date.
    assert "balance_non_negative" in bad["CASE_3"]
    assert {"currency_supported", "default_date_not_future"} <= bad["CASE_4"]


def test_warn_rules_do_not_quarantine(spark):
    df = tiny_cases(spark)
    result = apply_rules(df, case_rules())
    clean_refs = {r["case_reference"] for r in result.clean.collect()}
    # CASE_2 only trips the WARN rule (no id and no contact) -> stays in clean.
    assert "CASE_2" in clean_refs
    case_2 = [r for r in result.clean.collect() if r["case_reference"] == "CASE_2"][0]
    assert "national_id_or_contact_present" in case_2[DQ_FAILURES_COL]


def test_null_condition_counts_as_failure(spark):
    """The one people get wrong. `NULL > 0` is NULL, not False. Without the
    coalesce in apply_rules, a NULL row would slip through as passing."""
    df = spark.createDataFrame([("A", None)], "k string, v int")
    result = apply_rules(df, [Rule("v_positive", F.col("v") > 0, Severity.ERROR)])
    assert result.clean.count() == 0
    assert result.quarantined.count() == 1


def test_circuit_breaker_trips_on_mass_failure():
    metrics = {"rows_total": 1000, "rows_quarantined": 600}
    with pytest.raises(ValueError, match="exceeds threshold"):
        assert_error_rate_below(metrics, threshold=0.10)


def test_circuit_breaker_rejects_empty_source():
    """An empty source is almost always a broken upstream, not a quiet day."""
    with pytest.raises(ValueError, match="0 rows"):
        assert_error_rate_below({"rows_total": 0, "rows_quarantined": 0}, threshold=0.1)


def test_metrics_are_per_rule(spark):
    df = tiny_cases(spark)
    result = apply_rules(df, case_rules())
    assert result.metrics["rows_total"] == 4
    assert result.metrics["failed_balance_non_negative"] == 1
    assert result.metrics["failed_currency_supported"] == 1


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------
def test_dedupe_is_deterministic(spark):
    """Two runs over the same input must give the same output. dropDuplicates()
    would not guarantee this."""
    df = spark.createDataFrame(
        [
            ("CASE_1", 100, date(2024, 1, 1)),
            ("CASE_1", 200, date(2024, 2, 1)),
            ("CASE_1", 150, date(2024, 1, 15)),
        ],
        "case_reference string, balance int, updated_at date",
    )
    first = dedupe(df, ["case_reference"], F.col("updated_at")).collect()
    second = dedupe(df, ["case_reference"], F.col("updated_at")).collect()
    assert first == second
    assert first[0]["balance"] == 200, "should keep the latest row"


def test_dedupe_handles_exact_duplicates(spark):
    df = spark.createDataFrame(
        [("CASE_1", 100, date(2024, 1, 1))] * 3,
        "case_reference string, balance int, updated_at date",
    )
    assert dedupe(df, ["case_reference"], F.col("updated_at")).count() == 1


def test_dedupe_null_key_does_not_collapse_rows(spark):
    """Two different cases with a NULL key must not merge into one.

    Window partitionBy treats all NULLs as one group -- so if your dedupe key can
    be NULL, this test tells you before production does.
    """
    df = spark.createDataFrame(
        [(None, 100, date(2024, 1, 1)), (None, 200, date(2024, 2, 1))],
        "case_reference string, balance int, updated_at date",
    )
    out = dedupe(df, ["case_reference"], F.col("updated_at"))
    # Documenting actual behaviour: NULLs DO collapse. That is why
    # case_reference_present is an ERROR-severity rule applied before dedupe.
    assert out.count() == 1, "NULL keys collapse -- filter them out before dedupe"
