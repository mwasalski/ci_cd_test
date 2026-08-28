"""PII handling -- Spark layer.

The pure-Python rules (registry, pseudonymise, mask, guards) live in `pii_core`
so they are testable with no SparkSession. This module is the DataFrame shell.

Three separate concerns that people constantly conflate:

  1. PSEUDONYMISATION  - replace an identifier with a stable surrogate so you can
     still join and aggregate. Reversible only with the key. Still personal data
     under GDPR Art. 4(5) -- pseudonymised is NOT anonymised.
  2. MASKING           - show a partial value to a human (`***4567`). A
     display-layer concern, enforced with a Unity Catalog column mask
     (sql/uc_governance.sql), not in the pipeline.
  3. REDACTION         - drop the value entirely. Irreversible. Use for anything
     you have no lawful basis to keep.

The mistake to avoid: plain `sha2(national_id)`. A Polish PESEL has ~11 digits of
heavily constrained structure, so the whole value space is brute-forceable on a
laptop and a bare hash is reversible in practice. HMAC with a secret pepper is
not, as long as the pepper stays in a secret scope.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .observability import log_event
from .pii_core import (
    FORBIDDEN_IN_GOLD,
    PII_REGISTRY,
    PiiColumn,
    PiiTreatment,
    assert_columns_free_of_raw_pii,
    assert_registry_covers,
    hmac_pads,
    mask_last4,
    pseudonymise_value,
)

__all__ = [
    "FORBIDDEN_IN_GOLD",
    "PII_REGISTRY",
    "PiiColumn",
    "PiiTreatment",
    "apply_pii_policy",
    "assert_no_raw_pii",
    "assert_registry_covers",
    "mask_last4",
    "mask_last4_col",
    "pseudonymise_col_hmac",
    "pseudonymise_value",
]


def pseudonymise_col_hmac(col: Column, pepper: str) -> Column:
    """HMAC-SHA256 built from Spark natives: H(k XOR opad || H(k XOR ipad || m)).

    Spark has no built-in `hmac()`, so this is the honest way to get a keyed hash
    without a Python UDF. Staying in native expressions keeps the work inside the
    JVM (roughly 10-40x faster than a row-at-a-time UDF on real volumes) and lets
    Catalyst move the projection around.

    The pads come from `pii_core.hmac_pads`, the same function the pure-Python
    implementation uses, and `tests/test_pii.py` asserts the two produce identical
    output. A hand-rolled crypto primitive without an equivalence test is a
    liability, not a feature.

    Trade-off: the derived pads end up in the query plan. Acceptable inside a job,
    but do not log the plan and do not hand `EXPLAIN` to analysts.
    """
    ipad, opad = hmac_pads(pepper)

    normalised = F.lower(F.trim(col))
    inner = F.sha2(F.concat(F.unhex(F.lit(ipad)), F.encode(normalised, "UTF-8")), 256)
    outer = F.sha2(F.concat(F.unhex(F.lit(opad)), F.unhex(inner)), 256)

    return F.when(
        normalised.isNull() | (normalised == F.lit("")),
        F.lit(None).cast(StringType()),
    ).otherwise(F.concat(F.lit("px_"), F.substring(outer, 1, 32)))


def mask_last4_col(col: Column) -> Column:
    cleaned = F.regexp_replace(col, "[^A-Za-z0-9]", "")
    return F.when(col.isNull(), F.lit(None).cast(StringType())).otherwise(
        F.when(F.length(cleaned) < 4, F.lit("***")).otherwise(
            F.concat(F.lit("***"), F.substring(cleaned, -4, 4))
        )
    )


def apply_pii_policy(df: DataFrame, pepper: str) -> DataFrame:
    """Apply the registry to whatever PII columns are present in `df`.

    Idempotent and order-independent: safe to call twice, and safe on a DataFrame
    that carries only some of the registered columns.
    """
    applied: list[str] = []
    out = df
    for name, rule in PII_REGISTRY.items():
        if name not in out.columns:
            continue
        if rule.treatment is PiiTreatment.PSEUDONYMISE:
            out = out.withColumn(
                f"{name}_pseudonym", pseudonymise_col_hmac(F.col(name), pepper)
            ).drop(name)
        elif rule.treatment is PiiTreatment.MASK_LAST4:
            out = out.withColumn(f"{name}_masked", mask_last4_col(F.col(name))).drop(name)
        elif rule.treatment is PiiTreatment.REDACT:
            out = out.drop(name)
        applied.append(f"{name}:{rule.treatment.value}")

    log_event("pii.policy_applied", columns=applied)
    return out


def assert_no_raw_pii(df: DataFrame, table_name: str = "<unknown>") -> None:
    """Guard to call immediately before every gold write.

    A test asserts this; so does the job itself. Belt and braces is correct here
    -- PII landing in a table analysts can query is not a bug you fix next
    sprint, it is an incident with a regulator attached.
    """
    assert_columns_free_of_raw_pii(df.columns, table_name)
