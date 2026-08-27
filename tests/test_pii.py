"""PII tests.

The two that matter most:
  * test_spark_hmac_matches_python_hmac -- proves the hand-rolled Spark HMAC is
    actually HMAC. Hand-rolled crypto without an equivalence test is how you end
    up with pseudonyms that are reversible.
  * test_gold_guard_blocks_raw_pii -- the guard that turns a compliance incident
    into a failed job.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest
from pyspark.sql import functions as F

from collections_platform.pii import (
    PII_REGISTRY,
    PiiTreatment,
    apply_pii_policy,
    assert_no_raw_pii,
    assert_registry_covers,
    mask_last4,
    pseudonymise_col_hmac,
    pseudonymise_value,
)


# --------------------------------------------------------------------------
# Pure python -- no Spark, runs in microseconds
# --------------------------------------------------------------------------
def test_pseudonym_is_deterministic(pepper):
    assert pseudonymise_value("80010112345", pepper) == pseudonymise_value("80010112345", pepper)


def test_pseudonym_is_normalised(pepper):
    """Whitespace and case must not create two identities for one person."""
    assert pseudonymise_value("  ABC123 ", pepper) == pseudonymise_value("abc123", pepper)


def test_pseudonym_changes_with_pepper():
    """Key rotation must actually change the output, otherwise rotation is theatre."""
    assert pseudonymise_value("80010112345", "k1") != pseudonymise_value("80010112345", "k2")


@pytest.mark.parametrize("value", [None, "", "   "])
def test_pseudonym_null_stays_null(value, pepper):
    """A missing ID must NOT collapse into a shared bucket -- otherwise every
    debtor with no national_id becomes one 'super-debtor' and aggregates lie."""
    assert pseudonymise_value(value, pepper) is None


def test_pseudonym_is_not_a_bare_hash(pepper):
    """Guard against someone 'simplifying' this back to sha256(value)."""
    bare = hashlib.sha256("80010112345".encode()).hexdigest()
    assert pseudonymise_value("80010112345", pepper) != f"px_{bare[:32]}"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("+48 601 234 567", "***4567"),
        ("601234567", "***4567"),
        ("12", "***"),
        (None, None),
    ],
)
def test_mask_last4(raw, expected):
    assert mask_last4(raw) == expected


# --------------------------------------------------------------------------
# Spark layer
# --------------------------------------------------------------------------
def test_spark_hmac_matches_python_hmac(spark, pepper):
    """Equivalence test for the hand-rolled HMAC in native Spark expressions."""
    values = ["80010112345", "abc", "a@b.invalid"]
    df = spark.createDataFrame([(v,) for v in values], "v string")
    got = (
        df.withColumn("p", pseudonymise_col_hmac(F.col("v"), pepper))
        .select("v", "p")
        .collect()
    )
    for row in got:
        expected_digest = hmac.new(
            pepper.encode(), row["v"].strip().lower().encode(), hashlib.sha256
        ).hexdigest()
        assert row["p"] == f"px_{expected_digest[:32]}", f"mismatch for {row['v']}"


def test_apply_policy_removes_raw_columns(spark, pepper):
    df = spark.createDataFrame(
        [("CASE_1", "80010112345", "Jan Kowalski", "+48601234567", "a@x.invalid", "ul. X 1")],
        "case_reference string, national_id string, debtor_name string, "
        "debtor_phone string, debtor_email string, debtor_address string",
    )
    out = apply_pii_policy(df, pepper)

    assert "national_id" not in out.columns
    assert "national_id_pseudonym" in out.columns
    assert "debtor_name" not in out.columns          # redacted
    assert "debtor_address" not in out.columns       # redacted
    assert "debtor_phone_masked" in out.columns
    assert out.collect()[0]["debtor_phone_masked"] == "***4567"


def test_apply_policy_is_idempotent(spark, pepper):
    df = spark.createDataFrame([("CASE_1", "80010112345")], "case_reference string, national_id string")
    once = apply_pii_policy(df, pepper)
    twice = apply_pii_policy(once, pepper)
    assert once.columns == twice.columns
    assert once.collect() == twice.collect()


def test_gold_guard_blocks_raw_pii(spark):
    df = spark.createDataFrame([("CASE_1", "80010112345")], "case_reference string, national_id string")
    with pytest.raises(ValueError, match="Raw PII columns"):
        assert_no_raw_pii(df, "gold.fct_test")


def test_gold_guard_passes_after_policy(spark, pepper):
    df = spark.createDataFrame([("CASE_1", "80010112345")], "case_reference string, national_id string")
    assert_no_raw_pii(apply_pii_policy(df, pepper), "gold.fct_test")  # must not raise


def test_registry_catches_new_pii_column():
    """The schema-drift + PII intersection: an originator adds `debtor_mobile`."""
    with pytest.raises(ValueError, match="debtor_mobile"):
        assert_registry_covers(["case_reference", "debtor_mobile"])


def test_registry_allows_known_safe():
    assert_registry_covers(["case_reference", "portfolio_id"], known_safe={"portfolio_id"})


def test_every_registry_entry_has_a_reason():
    """Documentation enforced by a test. A KEEP with no reason is a decision
    nobody made."""
    for name, rule in PII_REGISTRY.items():
        assert rule.reason.strip(), f"{name} has no reason"
        if rule.treatment is PiiTreatment.KEEP:
            assert len(rule.reason) > 10, f"{name}: KEEP needs a real justification"
