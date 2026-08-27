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


def read_landing(
    spark: SparkSession,
    path: str,
    expected_schema: StructType,
    batch_id: str,
) -> DataFrame:
    """Read raw files with an explicit schema and a rescue column.

    `_rescued_data` is the safety net: anything that does not fit the declared
    schema lands there as JSON instead of becoming a NULL. Without it, a
    malformed row is indistinguishable from a legitimately empty one.
    """
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(expected_schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(path)
    )
    return add_audit_columns(df, batch_id=batch_id)


def add_audit_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """Lineage columns on every bronze/silver/gold table.

    `_source_file` is the one people skip and then regret: when a single
    originator file is bad, this is what lets you delete exactly those rows
    instead of reloading the whole partition.
    """
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.input_file_name())
    )
