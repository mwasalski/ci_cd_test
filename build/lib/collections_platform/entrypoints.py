"""Wheel entry points referenced by python_wheel_task in the DAB.

Each function is a thin shell: parse -> call library -> write. The logic lives in
importable modules so tests never have to invoke an entry point.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone

from .config import parse_args
from .observability import log_event, timed
from .spark_utils import get_spark, write_delta


def _batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _get_pepper(scope: str, key: str = "pii_pepper") -> str:
    """Read the pseudonymisation pepper from a Databricks secret scope.

    Never a literal, never an env var baked into the wheel, never a job parameter
    (job parameters are visible in the run UI and in the API response).
    """
    try:
        from databricks.sdk.runtime import dbutils  # available on the cluster

        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:  # pragma: no cover - local dev path
        raise RuntimeError(
            f"Could not read secret {scope}/{key}. On Databricks: "
            f"`databricks secrets create-scope {scope}` then put the pepper in it."
        ) from exc


def bootstrap_catalog(argv: list[str] | None = None) -> None:
    """Create catalog/schemas/volume/reference tables. Idempotent, run first."""
    import argparse

    from .bootstrap import BootstrapPlan, bootstrap, verify

    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--managed-location", default=None)
    # Default OFF: creating a catalog needs metastore privilege the pipeline SP
    # should not hold. Pass --create-catalog only when bootstrapping your own
    # workspace, where you are the metastore admin anyway.
    p.add_argument("--create-catalog", action="store_true")
    ns, _ = p.parse_known_args(argv)

    spark = get_spark()
    plan = BootstrapPlan(
        catalog=ns.catalog,
        managed_location=ns.managed_location,
        create_catalog=ns.create_catalog,
    )
    with timed("bootstrap", catalog=ns.catalog):
        bootstrap(spark, plan)
        verify(spark, plan)   # never trust CREATE IF NOT EXISTS without checking


def apply_governance(argv: list[str] | None = None) -> None:
    """Render sql/uc_governance.sql for this target and execute it.

    `${catalog}` in the SQL file is substituted HERE, not by the Databricks CLI.
    Bundle variable interpolation only applies to the bundle YAML -- it never
    reaches a file the YAML merely points at. See sql_runner.py.
    """
    import argparse

    from .sql_runner import execute_file

    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bronze-schema", default="bronze")
    p.add_argument("--gold-schema", default="gold")
    p.add_argument("--ops-schema", default="ops")
    p.add_argument("--sql-path", required=True, help="Workspace path to uc_governance.sql")
    ns, _ = p.parse_known_args(argv)

    spark = get_spark()
    with timed("apply_governance", catalog=ns.catalog):
        execute_file(
            spark,
            ns.sql_path,
            {
                "catalog": ns.catalog,
                "bronze_schema": ns.bronze_schema,
                "gold_schema": ns.gold_schema,
                "ops_schema": ns.ops_schema,
            },
        )


def ingest_portfolio(argv: list[str] | None = None) -> None:
    from .dq import apply_rules, assert_error_rate_below, case_rules, dedupe
    from .ingest import detect_drift, enforce_drift_policy, read_landing
    from .pii import apply_pii_policy, assert_registry_covers
    from .schemas import PORTFOLIO_CASE_RAW
    from pyspark.sql import functions as F

    cfg = parse_args(argv)
    spark = get_spark()
    batch = _batch_id()

    with timed("ingest", batch_id=batch):
        raw = read_landing(spark, cfg.landing_path, PORTFOLIO_CASE_RAW, batch)

        # Drift check before anything else touches the data.
        report = detect_drift(raw.schema, PORTFOLIO_CASE_RAW)
        enforce_drift_policy(report)
        assert_registry_covers(raw.columns, known_safe={"case_reference", "portfolio_id"})

        deduped = dedupe(raw, keys=["case_reference"], order_by=F.col("_ingested_at"))
        result = apply_rules(deduped, case_rules())
        assert_error_rate_below(result.metrics, threshold=0.10)

        pepper = _get_pepper(cfg.pii_scope)

        # PII policy is applied to BOTH branches. A quarantined row is still
        # personal data -- and the quarantine table is exactly the one people
        # forget, because it is "just the rejects". It is also the table an
        # engineer is most likely to `SELECT *` from while debugging.
        clean = apply_pii_policy(result.clean, pepper)
        quarantined = apply_pii_policy(result.quarantined, pepper)

        write_delta(clean, str(cfg.table(cfg.schema, "cases")), mode="overwrite")
        write_delta(
            quarantined,
            str(cfg.table(cfg.ops_schema, "cases_quarantine")),
            mode="append",
        )


def build_investing(argv: list[str] | None = None) -> None:
    from .pii import assert_no_raw_pii
    from .transform_investing import build_investing_performance

    cfg = parse_args(argv)
    spark = get_spark()
    with timed("build_investing"):
        out = build_investing_performance(
            payments=spark.table(str(cfg.table(cfg.source_schema, "payments"))),
            cases=spark.table(str(cfg.table(cfg.source_schema, "cases"))),
            portfolios=spark.table(str(cfg.table(cfg.source_schema, "portfolios"))),
            forecast_curve=spark.table(str(cfg.table(cfg.source_schema, "forecast_curve"))),
        )
        target = str(cfg.table(cfg.target_schema, "fct_investing_performance"))
        assert_no_raw_pii(out, target)
        write_delta(out, target)


def build_servicing(argv: list[str] | None = None) -> None:
    from .pii import assert_no_raw_pii
    from .transform_servicing import build_servicing_performance

    cfg = parse_args(argv)
    spark = get_spark()
    with timed("build_servicing"):
        out = build_servicing_performance(
            payments=spark.table(str(cfg.table(cfg.source_schema, "payments"))),
            cases=spark.table(str(cfg.table(cfg.source_schema, "cases"))),
            client_contracts=spark.table(str(cfg.table(cfg.source_schema, "client_contracts"))),
        )
        target = str(cfg.table(cfg.target_schema, "fct_servicing_performance"))
        assert_no_raw_pii(out, target)
        write_delta(out, target)


def build_features(argv: list[str] | None = None) -> None:
    from .features import build_training_set

    cfg = parse_args(argv)
    if cfg.as_of_date is None:
        raise SystemExit("--as-of-date is required: features must be reproducible.")
    spark = get_spark()
    with timed("build_features", as_of=cfg.as_of_date.isoformat()):
        out = build_training_set(
            payments=spark.table(str(cfg.table(cfg.source_schema, "payments"))),
            cases=spark.table(str(cfg.table(cfg.source_schema, "cases"))),
            as_of=cfg.as_of_date,
        )
        write_delta(out, str(cfg.table(cfg.target_schema, "propensity_training_set")))


def train_propensity(argv: list[str] | None = None) -> None:
    from .train import train_propensity as _train

    cfg = parse_args(argv)
    spark = get_spark()
    as_of = cfg.as_of_date or date.today()
    _train(
        spark=spark,
        training_set=spark.table(str(cfg.table(cfg.target_schema, "propensity_training_set"))),
        as_of=as_of,
        model_name=f"{cfg.catalog}.{cfg.target_schema}.propensity_to_pay",
        experiment_path="/Shared/collections_platform/propensity",
    )


def seed_synthetic(argv: list[str] | None = None) -> None:
    import argparse

    from .synthetic import generate_cases, generate_payments

    p = argparse.ArgumentParser()
    p.add_argument("--landing-path", required=True)
    p.add_argument("--n-cases", type=int, default=100_000)
    p.add_argument("--skew-factor", type=float, default=0.4)
    ns, _ = p.parse_known_args(argv)

    spark = get_spark()
    cases = generate_cases(spark, n_cases=ns.n_cases, skew_factor=ns.skew_factor)
    payments = generate_payments(spark, cases)
    cases.write.mode("overwrite").option("header", "true").csv(f"{ns.landing_path}/cases")
    payments.write.mode("overwrite").option("header", "true").csv(f"{ns.landing_path}/payments")
    log_event("seed.done", n_cases=ns.n_cases, path=ns.landing_path)


def run_tests(argv: list[str] | None = None) -> None:
    """Run pytest from inside a Databricks job so the suite executes against the
    real DBR (same Spark build, same Java, same Delta version) rather than a
    local pyspark that only approximates it."""
    import argparse

    import pytest

    p = argparse.ArgumentParser()
    p.add_argument("--path", default="tests")
    p.add_argument("--markers", default="not smoke")
    ns, _ = p.parse_known_args(argv)

    exit_code = pytest.main([ns.path, "-m", ns.markers, "-v", "--tb=short"])
    if exit_code != 0:
        raise SystemExit(f"pytest failed with exit code {exit_code}")


def run_smoke(argv: list[str] | None = None) -> None:
    """Post-deploy smoke test. Deliberately narrow and fast.

    It checks the things that break when a deploy goes wrong -- tables exist,
    they are fresh, contracts hold, no PII leaked -- and nothing else. A smoke
    test that takes 40 minutes is a smoke test nobody runs.
    """
    import argparse

    from .pii import FORBIDDEN_IN_GOLD

    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--gold-schema", default="gold")
    p.add_argument("--max-staleness-hours", type=int, default=26)
    ns, _ = p.parse_known_args(argv)

    spark = get_spark()
    failures: list[str] = []
    expected_tables = ["fct_investing_performance", "fct_servicing_performance"]

    for table in expected_tables:
        fqn = f"{ns.catalog}.{ns.gold_schema}.{table}"
        try:
            df = spark.table(fqn)
        except Exception as exc:
            failures.append(f"{fqn}: not readable ({type(exc).__name__})")
            continue

        if df.limit(1).count() == 0:
            failures.append(f"{fqn}: empty")

        leaked = sorted(set(df.columns) & FORBIDDEN_IN_GOLD)
        if leaked:
            failures.append(f"{fqn}: RAW PII PRESENT {leaked}")

        staleness = spark.sql(
            f"SELECT max(_ingested_at) AS m FROM {fqn}"
        ).collect()[0]["m"]
        if staleness is None:
            failures.append(f"{fqn}: no _ingested_at values")
        else:
            age_h = (datetime.now(timezone.utc) - staleness.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            if age_h > ns.max_staleness_hours:
                failures.append(f"{fqn}: stale by {age_h:.1f}h")

    log_event("smoke.done", failures=failures, tables=expected_tables)
    if failures:
        print("SMOKE FAILURES:\n  " + "\n  ".join(failures), file=sys.stderr)
        raise SystemExit(1)
    print("smoke ok")
