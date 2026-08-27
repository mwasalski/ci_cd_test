"""Bootstrap tests.

These do NOT need Unity Catalog. They assert on the SQL the planner emits, which
is the part that can silently be wrong (wrong catalog interpolated, catalog
creation left switched on in prod). Whether Databricks then executes it correctly
is not our bug to test.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from collections_platform.bootstrap import BootstrapPlan, bootstrap


def _run(plan: BootstrapPlan) -> list[str]:
    spark = MagicMock()
    return bootstrap(spark, plan)["executed"]


def test_does_not_create_catalog_by_default():
    """The safety default. A pipeline that can CREATE CATALOG can also be the
    reason a catalog exists that nobody meant to create."""
    sql = _run(BootstrapPlan(catalog="dev_collections"))
    assert not any("CREATE CATALOG" in s for s in sql)


def test_creates_catalog_when_explicitly_asked():
    sql = _run(BootstrapPlan(catalog="dev_collections", create_catalog=True))
    assert any(s.startswith("CREATE CATALOG IF NOT EXISTS dev_collections") for s in sql)


def test_managed_location_is_included_when_given():
    sql = _run(
        BootstrapPlan(
            catalog="dev_collections",
            create_catalog=True,
            managed_location="abfss://uc@acct.dfs.core.windows.net/dev",
        )
    )
    assert any("MANAGED LOCATION 'abfss://uc@acct" in s for s in sql)


def test_all_schemas_are_created_in_the_right_catalog():
    plan = BootstrapPlan(catalog="prod_collections")
    sql = _run(plan)
    for schema in plan.schemas:
        assert any(
            f"CREATE SCHEMA IF NOT EXISTS prod_collections.{schema}" in s for s in sql
        ), f"{schema} not created"


def test_ops_schema_is_separate_from_the_clean_schemas():
    """Quarantine must not live next to analyst-facing tables."""
    plan = BootstrapPlan(catalog="c")
    assert "ops" in plan.schemas
    assert {"bronze", "silver", "gold"} <= set(plan.schemas)


def test_every_statement_is_idempotent():
    """Re-running bootstrap must never fail. If a statement lacks IF NOT EXISTS,
    the second run breaks and people stop running it."""
    sql = _run(BootstrapPlan(catalog="c", create_catalog=True))
    ddl = [s for s in sql if s.startswith("CREATE")]
    assert ddl, "bootstrap emitted no DDL"
    for statement in ddl:
        assert "IF NOT EXISTS" in statement, f"not idempotent: {statement}"


def test_client_contracts_is_scd2_shaped():
    """Retrofitting valid_from/valid_to after someone loaded a flat current-state
    table means restating history. Get it right at creation."""
    sql = " ".join(_run(BootstrapPlan(catalog="c")))
    assert "client_contracts" in sql
    assert "valid_from" in sql and "valid_to" in sql


def test_money_columns_are_decimal_not_double():
    sql = " ".join(_run(BootstrapPlan(catalog="c")))
    assert "DECIMAL(18,2)" in sql
    assert "DOUBLE" not in sql.upper()
