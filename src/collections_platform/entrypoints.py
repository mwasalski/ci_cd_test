"""Wheel entry points referenced by python_wheel_task in the DAB.

Each function is a thin shell: parse -> call library -> write. The logic lives in
importable modules so tests never have to invoke an entry point.
"""

from __future__ import annotations

import base64
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

    Two lookups because there are two runtimes: `databricks.sdk.runtime` is the
    in-job one, and the WorkspaceClient path covers a serverless job where the
    runtime shim is not injected. Both end at the same secret.
    """
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:  # pragma: no cover - depends on runtime
        pass

    try:
        from databricks.sdk import WorkspaceClient

        secret = WorkspaceClient().secrets.get_secret(scope=scope, key=key)
        return base64.b64decode(secret.value).decode()
    except Exception as exc:  # pragma: no cover - local dev path
        raise RuntimeError(
            f"Could not read secret {scope}/{key}. Create it once with:\n"
            f"  databricks secrets create-scope {scope}\n"
            f"  databricks secrets put-secret {scope} {key}\n"
            f"(Free Edition supports workspace secret scopes; this is a one-time step.)"
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
    p.add_argument("--silver-schema", default="silver")
    p.add_argument("--gold-schema", default="gold")
    p.add_argument("--ops-schema", default="ops")
    # Principals are parameters, not literals in the SQL: Free Edition has no
    # account groups, so dev grants to the deploying user while prod names a
    # group. Same file, same statements, one variable.
    p.add_argument("--analyst-principal", default="data-analysts")
    p.add_argument("--ops-group", default="collections-ops")
    p.add_argument("--engineer-group", default="data-engineers")
    p.add_argument("--sql-path", required=True, help="Workspace path to uc_governance.sql")
    ns, _ = p.parse_known_args(argv)

    spark = get_spark()
    with timed("apply_governance", catalog=ns.catalog):
        execute_file(
            spark,
            ns.sql_path,
            {
                "catalog": ns.catalog,
                "silver_schema": ns.silver_schema,
                "gold_schema": ns.gold_schema,
                "ops_schema": ns.ops_schema,
                "analyst_principal": ns.analyst_principal,
                "ops_group": ns.ops_group,
                "engineer_group": ns.engineer_group,
            },
        )


def ingest_portfolio(argv: list[str] | None = None) -> None:
    """Landing volume -> conformed cases + quarantine.

    `--landing-path` must point at the *cases* directory, not at the volume
    root: one reader, one contract, one file layout. Pointing it at the root
    would also pick up the payments files, whose header does not match this
    contract -- which the drift check would (correctly, but confusingly) fail on.
    """
    from pyspark.sql import functions as F

    from .dq import apply_rules, assert_error_rate_below, case_rules, dedupe
    from .ingest import (
        add_audit_columns,
        conform,
        detect_drift,
        enforce_drift_policy,
        read_landing,
    )
    from .pii import apply_pii_policy, assert_registry_covers
    from .schemas import CASE_CASTS, PORTFOLIO_CASE_RAW

    cfg = parse_args(argv)
    spark = get_spark()
    batch = _batch_id()

    with timed("ingest_cases", batch_id=batch, path=cfg.landing_path):
        raw = read_landing(spark, cfg.landing_path)

        # Drift check before anything else touches the data, and before the
        # audit columns exist -- ours are not the originator's drift.
        report = detect_drift(raw.schema, PORTFOLIO_CASE_RAW)
        enforce_drift_policy(report)
        assert_registry_covers(
            raw.columns,
            known_safe={"case_reference", "portfolio_id", "client_id"},
        )

        typed = add_audit_columns(conform(raw, CASE_CASTS), batch_id=batch)
        deduped = dedupe(typed, keys=["case_reference"], order_by=F.col("_ingested_at"))
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


def ingest_payments(argv: list[str] | None = None) -> None:
    """Same shape as the case ingest: contract, drift, cast, DQ, quarantine.

    Payments carry no PII of their own, so there is no pepper here -- but the
    quarantine and the drift policy are identical. A payments feed that silently
    loses rows is exactly as damaging as a case feed that does.
    """
    from pyspark.sql import functions as F

    from .dq import apply_rules, assert_error_rate_below, dedupe, payment_rules
    from .ingest import (
        add_audit_columns,
        conform,
        detect_drift,
        enforce_drift_policy,
        read_landing,
    )
    from .schemas import PAYMENT_CASTS, PAYMENT_RAW

    cfg = parse_args(argv)
    spark = get_spark()
    batch = _batch_id()

    with timed("ingest_payments", batch_id=batch, path=cfg.landing_path):
        raw = read_landing(spark, cfg.landing_path)
        enforce_drift_policy(detect_drift(raw.schema, PAYMENT_RAW))

        typed = add_audit_columns(conform(raw, PAYMENT_CASTS), batch_id=batch)
        deduped = dedupe(typed, keys=["payment_id"], order_by=F.col("_ingested_at"))
        result = apply_rules(deduped, payment_rules())
        assert_error_rate_below(result.metrics, threshold=0.10)

        write_delta(result.clean, str(cfg.table(cfg.schema, "payments")), mode="overwrite")
        write_delta(
            result.quarantined,
            str(cfg.table(cfg.ops_schema, "payments_quarantine")),
            mode="append",
        )


def build_investing(argv: list[str] | None = None) -> None:
    from .ingest import add_audit_columns
    from .pii import assert_no_raw_pii
    from .transform_investing import build_investing_performance

    cfg = parse_args(argv)
    spark = get_spark()
    batch = _batch_id()
    with timed("build_investing", batch_id=batch):
        out = build_investing_performance(
            payments=spark.table(str(cfg.table(cfg.source_schema, "payments"))),
            cases=spark.table(str(cfg.table(cfg.source_schema, "cases"))),
            portfolios=spark.table(str(cfg.table(cfg.source_schema, "portfolios"))),
            forecast_curve=spark.table(str(cfg.table(cfg.source_schema, "forecast_curve"))),
        )
        target = str(cfg.table(cfg.target_schema, "fct_investing_performance"))
        assert_no_raw_pii(out, target)
        # A gold table with no `_ingested_at` cannot be freshness-checked, and
        # the smoke job's staleness test is the thing that catches a pipeline
        # that "succeeded" without writing anything new.
        write_delta(add_audit_columns(out, batch_id=batch, source_file=False), target)


def build_servicing(argv: list[str] | None = None) -> None:
    from .ingest import add_audit_columns
    from .pii import assert_no_raw_pii
    from .transform_servicing import build_servicing_performance

    cfg = parse_args(argv)
    spark = get_spark()
    batch = _batch_id()
    with timed("build_servicing", batch_id=batch):
        out = build_servicing_performance(
            payments=spark.table(str(cfg.table(cfg.source_schema, "payments"))),
            cases=spark.table(str(cfg.table(cfg.source_schema, "cases"))),
            client_contracts=spark.table(str(cfg.table(cfg.source_schema, "client_contracts"))),
        )
        target = str(cfg.table(cfg.target_schema, "fct_servicing_performance"))
        assert_no_raw_pii(out, target)
        write_delta(add_audit_columns(out, batch_id=batch, source_file=False), target)


def build_features(argv: list[str] | None = None) -> None:
    from .features import build_training_set
    from .ingest import add_audit_columns

    cfg = parse_args(argv)
    if cfg.as_of_date is None:
        raise SystemExit("--as-of-date is required: features must be reproducible.")
    spark = get_spark()
    batch = _batch_id()
    with timed("build_features", as_of=cfg.as_of_date.isoformat(), batch_id=batch):
        out = build_training_set(
            payments=spark.table(str(cfg.table(cfg.source_schema, "payments"))),
            cases=spark.table(str(cfg.table(cfg.source_schema, "cases"))),
            as_of=cfg.as_of_date,
        )
        write_delta(
            add_audit_columns(out, batch_id=batch, source_file=False),
            str(cfg.table(cfg.target_schema, "propensity_training_set")),
        )


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
    """Write the pathological originator feed into the landing volume, and the
    reference data into the tables `bootstrap` declared.

    Reference data is written as TABLES, not as files: it is contract data we
    own, not an originator feed. Routing it through the landing volume would
    imply a drift policy and a quarantine it does not need.
    """
    import argparse

    from .synthetic import (
        generate_cases,
        generate_client_contracts,
        generate_forecast_curve,
        generate_payments,
        generate_portfolios,
    )

    p = argparse.ArgumentParser()
    p.add_argument("--landing-path", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--ref-schema", default="silver", help="Where the reference tables live")
    p.add_argument("--n-cases", type=int, default=100_000)
    p.add_argument("--n-portfolios", type=int, default=20)
    p.add_argument("--skew-factor", type=float, default=0.4)
    ns, _ = p.parse_known_args(argv)

    spark = get_spark()
    with timed("seed", n_cases=ns.n_cases, path=ns.landing_path):
        cases = generate_cases(
            spark,
            n_cases=ns.n_cases,
            n_portfolios=ns.n_portfolios,
            skew_factor=ns.skew_factor,
        ).cache()
        payments = generate_payments(spark, cases)

        # A single CSV part per feed: the landing volume is standing in for an
        # SFTP drop, and 200 part-files is not what an originator sends.
        cases.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{ns.landing_path}/cases"
        )
        payments.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{ns.landing_path}/payments"
        )

        ref = f"{ns.catalog}.{ns.ref_schema}"
        write_delta(generate_portfolios(spark, ns.n_portfolios), f"{ref}.portfolios")
        write_delta(generate_forecast_curve(spark, ns.n_portfolios), f"{ref}.forecast_curve")
        write_delta(generate_client_contracts(spark), f"{ref}.client_contracts")
        cases.unpersist()

    log_event("seed.done", n_cases=ns.n_cases, path=ns.landing_path, reference_schema=ref)


def run_tests(argv: list[str] | None = None) -> None:
    """Run pytest from inside a Databricks job so the suite executes against the
    real runtime (same Spark build, same Python, same Delta version) rather than
    a local pyspark that only approximates it.

    `-p no:cacheprovider`: the tests live under /Workspace, and pytest's
    `.pytest_cache` write there either fails or leaves droppings in the deployed
    bundle. The cache buys nothing in a one-shot job.
    """
    import argparse

    import pytest

    p = argparse.ArgumentParser()
    p.add_argument("--path", default="tests")
    p.add_argument("--markers", default="not smoke")
    ns, _ = p.parse_known_args(argv)

    exit_code = pytest.main(
        [ns.path, "-m", ns.markers, "-v", "--tb=short", "-p", "no:cacheprovider"]
    )
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
