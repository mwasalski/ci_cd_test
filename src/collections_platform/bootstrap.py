"""Idempotent bootstrap of catalog / schemas / volume / reference tables.

Two ways to run it:
  * `databricks bundle run bootstrap -t dev`  (as a job, on a cluster)
  * `databricks sql -f sql/00_bootstrap.sql`  (as plain SQL, from a warehouse)

They do the same thing. The SQL file is the readable one you show a reviewer;
this module is the one CI can call and assert on.

Design note worth defending: DDL is separated from the pipeline. The jobs that
move data never issue CREATE SCHEMA. That means the pipeline service principal
does not need CREATE privileges on the catalog, which in turn means a bug in a
transform cannot drop a schema. Least privilege is a lot easier to argue for
when the code layout already reflects it.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from .observability import log_event


@dataclass(frozen=True, slots=True)
class BootstrapPlan:
    catalog: str
    schemas: tuple[str, ...] = ("bronze", "silver", "gold", "ops")
    landing_schema: str = "bronze"
    landing_volume: str = "landing"
    managed_location: str | None = None
    create_catalog: bool = False


def bootstrap(spark: SparkSession, plan: BootstrapPlan) -> dict[str, list[str]]:
    """Create everything the pipeline assumes exists. Safe to run repeatedly.

    Returns what it executed, so a test or a CI step can assert on it instead of
    trusting a log line.
    """
    executed: list[str] = []

    def run(sql: str) -> None:
        spark.sql(sql)
        # Store the full normalised statement, not a truncated one. The return
        # value exists so tests can assert on it -- truncating it here silently
        # hides the tail of every CREATE TABLE, which is where the column types
        # and the SCD2 columns live.
        executed.append(" ".join(sql.split()))

    # Off by default. Creating a catalog needs metastore-level privilege; a
    # pipeline service principal should not have it. Flip this on only when you
    # are bootstrapping your own workspace.
    if plan.create_catalog:
        location = f" MANAGED LOCATION '{plan.managed_location}'" if plan.managed_location else ""
        run(
            f"CREATE CATALOG IF NOT EXISTS {plan.catalog}{location} "
            f"COMMENT 'Debt collection platform. Contains pseudonymised personal data.'"
        )

    for schema in plan.schemas:
        run(f"CREATE SCHEMA IF NOT EXISTS {plan.catalog}.{schema}")

    run(
        f"CREATE VOLUME IF NOT EXISTS "
        f"{plan.catalog}.{plan.landing_schema}.{plan.landing_volume}"
    )

    # Reference tables: explicit DDL, not inferred from whatever file landed
    # first. `client_contracts` in particular MUST be SCD2-shaped from day one --
    # retrofitting valid_from/valid_to after someone has already loaded a flat
    # current-state table means restating history.
    run(
        f"""CREATE TABLE IF NOT EXISTS {plan.catalog}.silver.portfolios (
              portfolio_id     STRING NOT NULL,
              purchase_date    DATE   NOT NULL,
              purchase_price   DECIMAL(18,2),
              gross_face_value DECIMAL(18,2),
              seller_name      STRING,
              country_code     STRING)"""
    )
    run(
        f"""CREATE TABLE IF NOT EXISTS {plan.catalog}.silver.client_contracts (
              client_id       STRING NOT NULL,
              commission_rate DECIMAL(9,6) NOT NULL,
              sla_target_days INT,
              valid_from      DATE NOT NULL,
              valid_to        DATE)"""
    )
    run(
        f"""CREATE TABLE IF NOT EXISTS {plan.catalog}.silver.forecast_curve (
              portfolio_id   STRING NOT NULL,
              months_on_book INT    NOT NULL,
              forecast_pct   DECIMAL(9,6) NOT NULL)"""
    )

    log_event("bootstrap.done", catalog=plan.catalog, statements=len(executed))
    return {"executed": executed}


def verify(spark: SparkSession, plan: BootstrapPlan) -> None:
    """Assert the bootstrap actually took effect.

    A `CREATE ... IF NOT EXISTS` that silently did nothing because you were
    pointed at the wrong catalog is a real and annoying failure mode. Check,
    do not assume.
    """
    # The catalog's OWN information_schema, not `system.information_schema`:
    # every UC catalog exposes it to anyone who can use the catalog, whereas the
    # system catalog needs a separate grant (and is not enabled on every
    # workspace -- Free Edition included).
    found = {
        r["schema_name"]
        for r in spark.sql(
            f"SELECT schema_name FROM {plan.catalog}.information_schema.schemata"
        ).collect()
    }
    missing = sorted(set(plan.schemas) - found)
    if missing:
        raise RuntimeError(f"Bootstrap incomplete in {plan.catalog}: missing schemas {missing}")

    volumes = {
        r["volume_name"]
        for r in spark.sql(
            f"SELECT volume_name FROM {plan.catalog}.information_schema.volumes"
        ).collect()
    }
    if plan.landing_volume not in volumes:
        raise RuntimeError(f"Volume {plan.landing_volume} missing in {plan.catalog}")

    log_event("bootstrap.verified", catalog=plan.catalog, schemas=sorted(found))
