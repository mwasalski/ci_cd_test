"""Smoke tests -- run AFTER deploy, against a real catalog.

Marked `smoke` so the unit job skips them (`-m "not smoke"`).

What belongs here: existence, freshness, contract, PII. Anything that needs to be
fast and would break loudly if the deploy went wrong.

What does NOT belong here: business-logic correctness (that is a unit test) and
full-volume reconciliation (that is a scheduled DQ job).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from collections_platform.pii import FORBIDDEN_IN_GOLD

pytestmark = pytest.mark.smoke

CATALOG = os.environ.get("SMOKE_CATALOG", "dev_collections")
GOLD = os.environ.get("SMOKE_GOLD_SCHEMA", "gold")

GOLD_TABLES = ["fct_investing_performance", "fct_servicing_performance"]


@pytest.fixture(params=GOLD_TABLES)
def gold_table(request):
    return f"{CATALOG}.{GOLD}.{request.param}"


def test_table_exists_and_is_readable(spark, gold_table):
    spark.table(gold_table).limit(1).collect()


def test_table_is_not_empty(spark, gold_table):
    assert spark.table(gold_table).limit(1).count() == 1, f"{gold_table} is empty"


def test_no_raw_pii_in_gold(spark, gold_table):
    leaked = sorted(set(spark.table(gold_table).columns) & FORBIDDEN_IN_GOLD)
    assert not leaked, f"{gold_table} exposes raw PII: {leaked}"


def test_table_is_fresh(spark, gold_table):
    """26 hours, not 24: a daily job plus a retry must not page anyone."""
    latest = spark.sql(f"SELECT max(_ingested_at) AS m FROM {gold_table}").collect()[0]["m"]
    assert latest is not None, f"{gold_table} has no _ingested_at"
    age = datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)
    assert age < timedelta(hours=26), f"{gold_table} is stale by {age}"


def test_grain_is_unique(spark):
    """The contract that silently breaks after a join change: duplicated grain.
    Row counts still look plausible; every sum is inflated."""
    checks = {
        f"{CATALOG}.{GOLD}.fct_investing_performance": ["portfolio_id", "as_of_month"],
        f"{CATALOG}.{GOLD}.fct_servicing_performance": [
            "client_id",
            "case_reference",
            "as_of_month",
        ],
    }
    for table, keys in checks.items():
        key_list = ", ".join(keys)
        dupes = spark.sql(
            f"SELECT {key_list}, count(*) c FROM {table} "
            f"GROUP BY {key_list} HAVING count(*) > 1 LIMIT 5"
        ).collect()
        assert not dupes, f"{table} grain {keys} is not unique: {dupes}"


def test_no_negative_money(spark):
    table = f"{CATALOG}.{GOLD}.fct_investing_performance"
    bad = spark.sql(
        f"SELECT count(*) c FROM {table} WHERE erc_remaining < 0 OR collections_actual < 0"
    ).collect()[0]["c"]
    assert bad == 0, f"{bad} rows with negative money in {table}"
