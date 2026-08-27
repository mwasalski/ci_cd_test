"""Data-quality rules with quarantine.

Design choices worth defending:

* A failing row is NOT dropped. It is routed to a quarantine table with the list
  of rules it broke. Dropping rows silently is how a pipeline "succeeds" while
  losing 12% of a portfolio.
* Severity is per-rule. WARN rows continue downstream; ERROR rows are quarantined.
  A single global "fail the job" switch is too blunt -- one bad phone number
  should not stop a EUR 40m portfolio from loading.
* Rules are data (a list of objects), not code branches. That makes them
  enumerable, testable one-by-one, and printable in a run report.

Why not Great Expectations / DLT expectations? DLT expectations are the right
answer *if* you are already in a DLT pipeline -- they give you the quarantine and
the metrics for free. This module exists because plain job/wheel tasks have no
equivalent, and because it is ~80 lines you fully control.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .observability import log_event

DQ_FAILURES_COL = "_dq_failures"


class Severity(str, Enum):
    WARN = "warn"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class Rule:
    name: str
    # `condition` is the PASS condition: True means the row is good.
    condition: Column
    severity: Severity = Severity.ERROR
    description: str = ""


@dataclass(frozen=True, slots=True)
class DqResult:
    clean: DataFrame
    quarantined: DataFrame
    metrics: dict[str, int]


def apply_rules(df: DataFrame, rules: list[Rule]) -> DqResult:
    """Evaluate all rules in a single pass and split the DataFrame.

    Single pass matters: a naive implementation filters once per rule, which
    re-scans the source N times. On a 200M-row bronze table that is the
    difference between 2 minutes and 20.
    """
    if not rules:
        empty = df.limit(0).withColumn(DQ_FAILURES_COL, F.array().cast("array<string>"))
        return DqResult(df, empty, {})

    # NULL-safe: `condition` returning NULL (e.g. NULL > 0) must count as a
    # failure, not vanish. coalesce(cond, false) is the fix people forget.
    failure_flags = [
        F.when(~F.coalesce(r.condition, F.lit(False)), F.lit(r.name)) for r in rules
    ]
    with_failures = df.withColumn(
        DQ_FAILURES_COL, F.array_compact(F.array(*failure_flags))
    )

    error_rule_names = [r.name for r in rules if r.severity is Severity.ERROR]
    if error_rule_names:
        has_error = F.size(F.array_intersect(
            F.col(DQ_FAILURES_COL), F.array(*[F.lit(n) for n in error_rule_names])
        )) > 0
    else:
        has_error = F.lit(False)

    with_failures = with_failures.withColumn("_dq_has_error", has_error).cache()

    clean = with_failures.filter(~F.col("_dq_has_error")).drop("_dq_has_error")
    quarantined = with_failures.filter(F.col("_dq_has_error")).drop("_dq_has_error")

    metrics = _collect_metrics(with_failures, rules)
    log_event("dq.applied", **metrics)
    return DqResult(clean=clean, quarantined=quarantined, metrics=metrics)


def _collect_metrics(df: DataFrame, rules: list[Rule]) -> dict[str, int]:
    """One aggregation for all rule counts. Do not loop `.count()` per rule."""
    agg_exprs = [F.count(F.lit(1)).alias("rows_total")]
    agg_exprs += [
        F.sum(F.array_contains(F.col(DQ_FAILURES_COL), F.lit(r.name)).cast("int")).alias(
            f"failed_{r.name}"
        )
        for r in rules
    ]
    agg_exprs.append(F.sum(F.col("_dq_has_error").cast("int")).alias("rows_quarantined"))
    row = df.agg(*agg_exprs).collect()[0].asDict()
    return {k: int(v or 0) for k, v in row.items()}


def assert_error_rate_below(metrics: dict[str, int], threshold: float) -> None:
    """Circuit breaker. Quarantining 0.5% of rows is Tuesday; quarantining 60%
    means the originator changed their file format and you should stop, not
    quietly publish a fact table that is missing most of the portfolio.
    """
    total = metrics.get("rows_total", 0)
    if total == 0:
        raise ValueError("DQ ran on 0 rows -- empty source is almost never intentional.")
    rate = metrics.get("rows_quarantined", 0) / total
    if rate > threshold:
        raise ValueError(
            f"Quarantine rate {rate:.2%} exceeds threshold {threshold:.2%}. "
            f"Metrics: {metrics}"
        )


# ---------------------------------------------------------------------------
# Domain rules
# ---------------------------------------------------------------------------
def case_rules() -> list[Rule]:
    return [
        Rule(
            "case_reference_present",
            F.col("case_reference").isNotNull() & (F.length(F.trim(F.col("case_reference"))) > 0),
            Severity.ERROR,
            "A case with no reference cannot be joined to payments -- unusable.",
        ),
        Rule(
            "balance_non_negative",
            F.col("current_balance") >= 0,
            Severity.ERROR,
            "Negative balance means an overpayment posted to the wrong case.",
        ),
        Rule(
            "balance_not_above_original",
            F.col("current_balance") <= F.col("original_balance") * F.lit(3),
            Severity.WARN,
            "Interest/fees can exceed principal, but 3x suggests a unit error (grosze vs zloty).",
        ),
        Rule(
            "currency_supported",
            F.col("currency").isin("PLN", "EUR", "SEK", "GBP"),
            Severity.ERROR,
            "An unmapped currency silently breaks every FX-converted aggregate.",
        ),
        Rule(
            "default_date_not_future",
            F.col("default_date") <= F.current_date(),
            Severity.ERROR,
            "Future default date = a clock/timezone bug at the originator.",
        ),
        Rule(
            "national_id_or_contact_present",
            F.col("national_id").isNotNull()
            | F.col("debtor_email").isNotNull()
            | F.col("debtor_phone").isNotNull(),
            Severity.WARN,
            "No identifier and no contact route -- the case is not collectable.",
        ),
    ]


def payment_rules() -> list[Rule]:
    return [
        Rule("payment_amount_positive", F.col("amount") > 0, Severity.ERROR),
        Rule(
            "payment_date_not_future",
            F.col("payment_date") <= F.current_date(),
            Severity.ERROR,
        ),
        Rule(
            "payment_date_plausible",
            F.col("payment_date") >= F.lit("2000-01-01").cast("date"),
            Severity.WARN,
            "1970-01-01 is an epoch-zero default leaking through, not a real payment.",
        ),
    ]


def dedupe(df: DataFrame, keys: list[str], order_by: Column) -> DataFrame:
    """Keep the latest row per key.

    dropDuplicates(keys) is NOT equivalent: it keeps an arbitrary row, so two runs
    over the same input can produce different output. Non-determinism in a
    dedupe is a genuinely nasty bug to chase.
    """
    from pyspark.sql import Window

    w = Window.partitionBy(*keys).orderBy(order_by.desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
