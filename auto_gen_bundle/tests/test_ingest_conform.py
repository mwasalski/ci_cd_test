"""Landing -> typed silver: the reader, the cast, and the round trip.

These are the tests that would have caught the two bugs that made the pipeline
un-runnable on serverless:

  * the reader was handed the expected schema, so CSV mapped by POSITION and
    drift could never be detected;
  * DQ compared landed strings to numbers, which is a NULL under legacy
    semantics and a hard error under ANSI -- the serverless default.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from collections_platform.dq import apply_rules, case_rules
from collections_platform.ingest import (
    SchemaDriftError,
    add_audit_columns,
    conform,
    detect_drift,
    enforce_drift_policy,
    read_landing,
)
from collections_platform.schemas import (
    CASE_CASTS,
    PAYMENT_CASTS,
    PAYMENT_RAW,
    PORTFOLIO_CASE_RAW,
)
from collections_platform.synthetic import generate_cases, generate_payments

CASE_COLUMNS = [f.name for f in PORTFOLIO_CASE_RAW.fields]


@pytest.fixture(scope="module")
def landed_cases(spark, tmp_path_factory):
    """A real CSV round trip: generate -> write with a header -> read back.

    Not a `createDataFrame` shortcut on purpose. The header/position bug only
    exists on the file boundary, so a test that skips the file cannot see it.
    """
    path = str(tmp_path_factory.mktemp("landing") / "cases")
    generate_cases(spark, n_cases=200, n_portfolios=5).write.mode("overwrite").option(
        "header", "true"
    ).csv(path)
    return read_landing(spark, path)


def test_generator_emits_exactly_the_contract(spark):
    """If the generator and the contract drift apart, every ingest run fails on
    drift -- an expensive way to find out you edited one of the two."""
    cases = generate_cases(spark, n_cases=10, n_portfolios=2)
    assert cases.columns == CASE_COLUMNS

    payments = generate_payments(spark, cases)
    assert payments.columns == [f.name for f in PAYMENT_RAW.fields]


def test_landed_data_is_all_strings_named_by_header(landed_cases):
    """The reader must not apply types, and must not reorder: names come from
    the header, so an originator adding a column shifts nothing."""
    assert set(landed_cases.columns) == set(CASE_COLUMNS)
    assert {f.dataType.simpleString() for f in landed_cases.schema.fields} == {"string"}


def test_round_trip_survives_the_drift_check(landed_cases):
    """The end-to-end version of the position-vs-header bug: reading back what
    we wrote must be reported as no drift at all."""
    report = detect_drift(landed_cases.schema, PORTFOLIO_CASE_RAW)
    assert not report.is_breaking
    assert report.added == [] and report.missing == []
    enforce_drift_policy(report)  # must not raise


def test_conform_types_the_columns_it_is_given(landed_cases):
    typed = conform(landed_cases, CASE_CASTS)
    types = {f.name: f.dataType.simpleString() for f in typed.schema.fields}
    assert types["current_balance"] == "decimal(18,2)"
    assert types["default_date"] == "date"
    assert types["placed_date"] == "date"
    # Untouched columns stay strings -- conform casts what it is told to, and
    # nothing else.
    assert types["case_reference"] == "string"


def test_conform_nulls_an_unparseable_value_instead_of_raising(spark):
    """The ANSI point. A plain cast would raise here and kill the job for one
    bad row; try_cast quarantines that row instead."""
    df = spark.createDataFrame(
        [("CASE_1", "1000.00"), ("CASE_2", "1 000,00 zl")],
        "case_reference string, current_balance string",
    )
    out = {r["case_reference"]: r["current_balance"] for r in conform(df, CASE_CASTS).collect()}
    assert out["CASE_1"] == Decimal("1000.00")
    assert out["CASE_2"] is None


def test_unparseable_money_is_quarantined_not_dropped(spark):
    """And the row does not vanish: it lands in quarantine with the rule name,
    because the DQ conditions are NULL-safe."""
    df = spark.createDataFrame(
        [
            ("CASE_1", "500.00", "1000.00", "PLN", "2023-01-01", "80010112345", None, None),
            ("CASE_2", "not-a-number", "1000.00", "PLN", "2023-01-01", "80010112345", None, None),
        ],
        "case_reference string, current_balance string, original_balance string, "
        "currency string, default_date string, national_id string, debtor_email string, "
        "debtor_phone string",
    )
    result = apply_rules(conform(df, CASE_CASTS), case_rules())
    assert result.clean.count() == 1
    quarantined = result.quarantined.collect()
    assert len(quarantined) == 1
    assert quarantined[0]["case_reference"] == "CASE_2"
    assert "balance_non_negative" in quarantined[0]["_dq_failures"]


def test_payment_casts_cover_the_payment_contract(spark):
    df = spark.createDataFrame(
        [("P1", "CASE_1", "2024-01-10", "100.00", "CARD")],
        "payment_id string, case_reference string, payment_date string, amount string, "
        "channel string",
    )
    row = conform(df, PAYMENT_CASTS).collect()[0]
    assert row["payment_date"] == date(2024, 1, 10)
    assert row["amount"] == Decimal("100.00")


def test_audit_columns_are_not_reported_as_originator_drift(landed_cases):
    """`_ingested_at` is ours. Adding it before the drift check would fail every
    single ingest run on an unwhitelisted new column."""
    audited = add_audit_columns(landed_cases, batch_id="20240101T000000Z")
    assert {"_ingested_at", "_batch_id", "_source_file"} <= set(audited.columns)

    with pytest.raises(SchemaDriftError, match="unwhitelisted"):
        enforce_drift_policy(detect_drift(audited.schema, PORTFOLIO_CASE_RAW))


def test_gold_writes_get_audit_columns_without_a_source_file(spark):
    """A derived table has no source file, but it still needs `_ingested_at` --
    that is what the smoke job's staleness check reads."""
    df = spark.createDataFrame([("P0",)], "portfolio_id string")
    out = add_audit_columns(df, batch_id="b1", source_file=False)
    assert "_source_file" not in out.columns
    assert out.collect()[0]["_batch_id"] == "b1"
