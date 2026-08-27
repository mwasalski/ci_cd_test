"""Shared fixtures.

The session fixture has to serve three places this suite actually runs:

  1. inside the `unit_tests` serverless job  -- a Spark Connect session already
     exists; creating another one is not possible and `.master()` does not exist;
  2. locally via Databricks Connect          -- `DatabricksSession` talks to
     serverless, so the tests exercise the same engine the jobs will;
  3. locally with plain pyspark              -- the offline loop, `[local-spark]`.

Session-scoped, because building a session costs 5-15s and a function-scoped
fixture turns a 90-test suite into a coffee break.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

# Settings that must hold wherever the tests run. Anything not in this dict is
# left at the platform default on purpose -- a test that only passes because of
# a tuned conf is testing your conf.
_REQUIRED_CONF = {
    "spark.sql.session.timeZone": "UTC",
    # ON, matching serverless (environment version 5 / Spark 4). A bad cast
    # raises instead of returning NULL, which is exactly the behaviour the
    # ingest code is written against -- see ingest.conform() and its try_cast.
    "spark.sql.ansi.enabled": "true",
}

# Local-only speedups. 200 shuffle partitions on a 4-row DataFrame means 200
# empty tasks per shuffle; this is usually a 5-10x speedup on a unit suite.
_LOCAL_CONF = {
    "spark.sql.shuffle.partitions": "2",
    "spark.default.parallelism": "2",
    "spark.ui.enabled": "false",
}


def _build_session() -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:              # (1) running inside a Databricks job
        return active

    try:                                # (2) Databricks Connect, if configured
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless(True).getOrCreate()
    except Exception:
        pass

    builder = SparkSession.builder.master("local[2]").appName("collections_platform_tests")
    for key, value in {**_LOCAL_CONF, **_REQUIRED_CONF}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()        # (3) offline pyspark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = _build_session()
    for key, value in _REQUIRED_CONF.items():
        # Runtime confs, set after the fact: on an existing (Connect) session
        # the builder's config is not applied. Serverless allows this narrow
        # set; if a future runtime does not, the test run should say so rather
        # than quietly run under different semantics.
        session.conf.set(key, value)
    return session


@pytest.fixture
def pepper() -> str:
    return "test-pepper-not-a-real-secret"
