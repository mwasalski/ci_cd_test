"""Pure-Python PII core -- deliberately imports nothing from pyspark.

Why the split: this module can be imported and tested without a SparkSession, so
the rules that carry the actual compliance weight run in milliseconds, in any CI,
on any machine. `pii.py` is the thin Spark shell on top.

That is the general pattern worth stealing: push logic into pure functions,
keep the DataFrame layer as thin as you can. It is the difference between a
7-second test suite and a 7-minute one.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass
from enum import Enum


class PiiTreatment(str, Enum):
    PSEUDONYMISE = "pseudonymise"
    REDACT = "redact"
    MASK_LAST4 = "mask_last4"
    KEEP = "keep"  # explicit "this is fine in clear" -- forces a decision


@dataclass(frozen=True, slots=True)
class PiiColumn:
    name: str
    treatment: PiiTreatment
    reason: str


# The registry IS the contract. One list a reviewer can read to know what happens
# to every personal field.
PII_REGISTRY: dict[str, PiiColumn] = {
    c.name: c
    for c in [
        PiiColumn("national_id", PiiTreatment.PSEUDONYMISE, "join key across portfolios"),
        PiiColumn("debtor_email", PiiTreatment.PSEUDONYMISE, "contactability features"),
        PiiColumn("debtor_phone", PiiTreatment.MASK_LAST4, "agents need last 4 to verify identity"),
        PiiColumn("debtor_name", PiiTreatment.REDACT, "no analytical use in gold"),
        PiiColumn("debtor_address", PiiTreatment.REDACT, "no analytical use in gold"),
        PiiColumn("postcode_area", PiiTreatment.KEEP, "first 2 chars only, geo segmentation"),
        PiiColumn("date_of_birth", PiiTreatment.REDACT, "age band is derived before redaction"),
    ]
}

# Columns that must never appear in a gold table under their raw name.
FORBIDDEN_IN_GOLD = frozenset(
    name for name, c in PII_REGISTRY.items() if c.treatment is not PiiTreatment.KEEP
)

_NON_ALNUM = re.compile(r"[^A-Za-z0-9]")


def pseudonymise_value(value: str | None, pepper: str) -> str | None:
    """Deterministic, keyed pseudonym. Same input + same pepper -> same output.

    Returns None for None/blank so a missing identifier stays missing instead of
    becoming a shared bucket -- otherwise every NULL national_id collapses into
    one 'super-debtor' and your aggregates silently lie.
    """
    if value is None:
        return None
    normalised = value.strip().lower()
    if not normalised:
        return None
    digest = hmac.new(pepper.encode(), normalised.encode(), hashlib.sha256).hexdigest()
    return f"px_{digest[:32]}"


def mask_last4(value: str | None) -> str | None:
    """`+48 601 234 567` -> `***4567`. Enough for a human to verify identity."""
    if value is None:
        return None
    cleaned = _NON_ALNUM.sub("", value)
    if len(cleaned) < 4:
        return "***"
    return f"***{cleaned[-4:]}"


def hmac_pads(pepper: str) -> tuple[str, str]:
    """Derive the HMAC inner/outer pads as hex, for the Spark-native implementation.

    Kept here (pure) so the Spark version and the Python version provably share
    one definition instead of drifting apart.
    """
    block_size = 64
    key = pepper.encode()
    if len(key) > block_size:
        key = hashlib.sha256(key).digest()
    key = key.ljust(block_size, b"\x00")
    return (
        bytes(b ^ 0x36 for b in key).hex(),  # ipad
        bytes(b ^ 0x5C for b in key).hex(),  # opad
    )


def assert_registry_covers(columns: list[str], known_safe: set[str] | None = None) -> None:
    """Fail on a column that *looks* like PII but has no registry entry.

    This catches schema drift from an originator adding `debtor_mobile` next
    quarter. Heuristic and deliberately noisy -- a false positive costs one line
    in the registry, a false negative costs a regulator conversation.
    """
    known_safe = known_safe or set()
    suspicious_tokens = ("name", "email", "phone", "mobile", "address", "birth", "pesel", "nip")
    unregistered = [
        c
        for c in columns
        if c not in PII_REGISTRY
        and c not in known_safe
        and any(tok in c.lower() for tok in suspicious_tokens)
    ]
    if unregistered:
        raise ValueError(
            f"Columns {sorted(unregistered)} look like PII but are not in PII_REGISTRY. "
            f"Add an explicit treatment (KEEP is allowed, but state a reason)."
        )


def assert_columns_free_of_raw_pii(columns: list[str], table_name: str = "<unknown>") -> None:
    """Column-name-only guard. The DataFrame version lives in pii.py."""
    leaked = sorted(set(columns) & FORBIDDEN_IN_GOLD)
    if leaked:
        raise ValueError(
            f"Raw PII columns {leaked} would be written to {table_name}. "
            f"Call apply_pii_policy() first, or add an explicit KEEP rule with a reason."
        )
