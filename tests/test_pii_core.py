"""Pure-Python PII tests -- no SparkSession, no fixtures, milliseconds.

Keep the rules that carry compliance weight here. They run in any CI, on any
machine, before anyone waits for a cluster.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from collections_platform.pii_core import (
    PII_REGISTRY,
    PiiTreatment,
    assert_columns_free_of_raw_pii,
    assert_registry_covers,
    hmac_pads,
    mask_last4,
    pseudonymise_value,
)


def test_pseudonym_is_deterministic():
    assert pseudonymise_value("80010112345", "k") == pseudonymise_value("80010112345", "k")


def test_pseudonym_is_normalised():
    """Whitespace and case must not create two identities for one person."""
    assert pseudonymise_value("  ABC123 ", "k") == pseudonymise_value("abc123", "k")


def test_pseudonym_changes_with_pepper():
    """Key rotation must actually change output, otherwise rotation is theatre."""
    assert pseudonymise_value("80010112345", "k1") != pseudonymise_value("80010112345", "k2")


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_id_stays_missing(value):
    """A NULL identifier must NOT collapse into a shared bucket, or every debtor
    without a national_id becomes one 'super-debtor' and aggregates lie."""
    assert pseudonymise_value(value, "k") is None


def test_pseudonym_is_not_a_bare_hash():
    """Guard against someone 'simplifying' this back to sha256(value)."""
    bare = hashlib.sha256(b"80010112345").hexdigest()
    assert pseudonymise_value("80010112345", "k") != f"px_{bare[:32]}"


def test_hmac_pads_reproduce_reference_hmac():
    """The pads used by the Spark implementation must reconstruct real HMAC."""
    pepper, msg = "some-pepper", b"80010112345"
    ipad, opad = hmac_pads(pepper)
    inner = hashlib.sha256(bytes.fromhex(ipad) + msg).digest()
    manual = hashlib.sha256(bytes.fromhex(opad) + inner).hexdigest()
    reference = hmac.new(pepper.encode(), msg, hashlib.sha256).hexdigest()
    assert manual == reference


def test_hmac_pads_handle_long_pepper():
    """A pepper longer than the 64-byte block must be hashed down first -- the
    edge case a hand-rolled HMAC usually gets wrong."""
    pepper = "x" * 200
    ipad, opad = hmac_pads(pepper)
    inner = hashlib.sha256(bytes.fromhex(ipad) + b"abc").digest()
    manual = hashlib.sha256(bytes.fromhex(opad) + inner).hexdigest()
    assert manual == hmac.new(pepper.encode(), b"abc", hashlib.sha256).hexdigest()


@pytest.mark.parametrize(
    "raw,expected",
    [("+48 601 234 567", "***4567"), ("601234567", "***4567"), ("12", "***"), (None, None)],
)
def test_mask_last4(raw, expected):
    assert mask_last4(raw) == expected


def test_registry_catches_new_pii_column():
    """The drift x PII intersection: an originator adds `debtor_mobile`."""
    with pytest.raises(ValueError, match="debtor_mobile"):
        assert_registry_covers(["case_reference", "debtor_mobile"])


def test_registry_allows_known_safe():
    assert_registry_covers(["case_reference", "portfolio_id"], known_safe={"portfolio_id"})


def test_gold_guard_rejects_raw_pii():
    with pytest.raises(ValueError, match="Raw PII columns"):
        assert_columns_free_of_raw_pii(["case_reference", "national_id"], "gold.fct_test")


def test_gold_guard_accepts_pseudonyms():
    assert_columns_free_of_raw_pii(["case_reference", "national_id_pseudonym"], "gold.fct_test")


def test_every_registry_entry_has_a_reason():
    """Documentation enforced by a test. A KEEP with no reason is a decision
    nobody actually made."""
    for name, rule in PII_REGISTRY.items():
        assert rule.reason.strip(), f"{name} has no reason"
        if rule.treatment is PiiTreatment.KEEP:
            assert len(rule.reason) > 10, f"{name}: KEEP needs a real justification"
