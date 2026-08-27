"""Typed configuration.

Why a dataclass instead of reading widget values inline: every job parameter
becomes a single validated object that tests can construct without a cluster.
`--catalog=foo` typo'd as `--catalgo=foo` fails at parse time, not three tasks later.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class TableRef:
    """Fully-qualified Unity Catalog table reference."""

    catalog: str
    schema: str
    table: str

    def __str__(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True, slots=True)
class JobConfig:
    catalog: str
    schema: str = "bronze"
    source_schema: str = "silver"
    target_schema: str = "gold"
    landing_path: str = ""
    pii_scope: str = "collections"
    pii_key_name: str = "pii_pepper"
    as_of_date: date | None = None

    def table(self, schema: str, name: str) -> TableRef:
        return TableRef(self.catalog, schema, name)


def parse_args(argv: list[str] | None = None) -> JobConfig:
    """Parse Databricks python_wheel_task parameters into a JobConfig."""
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", default="bronze")
    p.add_argument("--source-schema", default="silver")
    p.add_argument("--target-schema", default="gold")
    p.add_argument("--landing-path", default="")
    p.add_argument("--pii-scope", default="collections")
    p.add_argument("--as-of-date", default=None)

    ns, _unknown = p.parse_known_args(argv)
    return JobConfig(
        catalog=ns.catalog,
        schema=ns.schema,
        source_schema=ns.source_schema,
        target_schema=ns.target_schema,
        landing_path=ns.landing_path,
        pii_scope=ns.pii_scope,
        as_of_date=date.fromisoformat(ns.as_of_date) if ns.as_of_date else None,
    )
