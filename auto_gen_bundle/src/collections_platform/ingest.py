"""Bronze ingest with explicit schema-drift policy.

The drift question is the one that separates a mid answer from a senior one.

  mid    : `.option("mergeSchema", "true")` and move on.
  senior : classify the drift, then decide per class.
             - ADDITIVE   (new column)        -> allow if whitelisted, log, keep the value
             - MISSING    (column disappeared)-> FAIL. Downstream contracts depend on it.
             - TYPE CHANGE                    -> FAIL. A silent string->int cast loses rows.
             - RENAME                         -> looks like MISSING + ADDITIVE. Only a human
                                                 can tell; that is why it must fail loudly.

`mergeSchema=true` handles exactly one of those four correctly.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .observability import log_event
from .schemas import OPTIONAL_INBOUND_COLUMNS


class SchemaDriftError(Exception):
    """Raised for drift we refuse to auto-handle."""


@dataclass(frozen=True, slots=True)
class DriftReport:
    added: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    type_changed: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def is_breaking(self) -> bool:
        return bool(self.missing or self.type_changed)


def detect_drift(actual: StructType, expected: StructType) -> DriftReport:
    """Pure function over two schemas -- no Spark session needed, so it is trivially
    unit-testable and runs in milliseconds."""
    actual_map = {f.name: f.dataType.simpleString() for f in actual.fields}
    expected_map = {f.name: f.dataType.simpleString() for f in expected.fields}

    added = sorted(set(actual_map) - set(expected_map))
    missing = sorted(set(expected_map) - set(actual_map))
    type_changed = sorted(
        (name, expected_map[name], actual_map[name])
        for name in set(actual_map) & set(expected_map)
        if actual_map[name] != expected_map[name]
    )
    return DriftReport(added=added, missing=missing, type_changed=type_changed)


def enforce_drift_policy(
    report: DriftReport, allowed_new_columns: frozenset[str] = OPTIONAL_INBOUND_COLUMNS
) -> None:
    unexpected_new = sorted(set(report.added) - allowed_new_columns)

    log_event(
        "ingest.drift_detected",
        added=report.added,
        missing=report.missing,
        type_changed=[list(t) for t in report.type_changed],
        unexpected_new=unexpected_new,
    )

    problems: list[str] = []
    if report.missing:
        problems.append(f"columns disappeared: {report.missing}")
    if report.type_changed:
        problems.append(
            "type changed: "
            + ", ".join(f"{n} {old}->{new}" for n, old, new in report.type_changed)
        )
    if unexpected_new:
        problems.append(
            f"unwhitelisted new columns: {unexpected_new} "
            f"(add to OPTIONAL_INBOUND_COLUMNS after a human looks at them)"
        )
    if problems:
        raise SchemaDriftError("; ".join(problems))


def read_landing(spark: SparkSession, path: str) -> DataFrame:
    """Read landed CSV as ALL STRINGS, with the column names taken from the header.

    Two things are deliberate here:

    * **No `.schema(expected)`.** Handing Spark the expected schema makes drift
      undetectable -- the DataFrame then matches the contract *by construction*,
      whatever the file contained, and CSV maps by position, so one inserted
      column silently shifts every value one place to the left. Reading the
      header and comparing it to the contract is the only way `detect_drift`
      can ever fire.
    * **No `inferSchema`.** Inference makes the schema a function of this
      batch's data. Types are applied by `conform()`, explicitly, one step later.

    Audit columns are added *after* the drift check (see `entrypoints`), because
    our own `_ingested_at` would otherwise show up as originator drift.
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .load(path)
    )


def conform(df: DataFrame, casts: dict[str, str]) -> DataFrame:
    """Cast landed strings to the platform's types, failing soft per value.

    `try_cast`, not `cast`: serverless runs with ANSI mode on, where a plain
    cast of `'12,50'` to a decimal raises and takes the entire job down for one
    bad row. try_cast yields NULL instead, and the DQ rules -- which are
    NULL-safe on purpose -- route that row to quarantine with the rule name
    attached. One bad value costs you one quarantined row, not a red job.
    """
    out = df
    converted: list[str] = []
    for name, target_type in casts.items():
        if name not in out.columns:
            continue
        out = out.withColumn(name, F.col(name).try_cast(target_type))
        converted.append(f"{name}:{target_type}")
    log_event("ingest.conformed", columns=converted)
    return out


def add_audit_columns(df: DataFrame, batch_id: str, source_file: bool = True) -> DataFrame:
    """Lineage columns on every silver/gold table.

    `_source_file` is the one people skip and then regret: when a single
    originator file is bad, this is what lets you delete exactly those rows
    instead of reloading the whole partition. It only exists for file-based
    reads, hence the flag -- a gold table derived from other tables gets
    `_ingested_at` and `_batch_id` only.
    """
    out = df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_batch_id", F.lit(batch_id)
    )
    if source_file:
        out = out.withColumn("_source_file", F.col("_metadata.file_path"))
    return out
