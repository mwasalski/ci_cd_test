"""Transform tests for the two fact domains.

The point of the first test is modelling, not code: Investing and Servicing must
not share a grain or a measure set.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from collections_platform.schemas import (
    FCT_INVESTING_PERFORMANCE,
    FCT_SERVICING_PERFORMANCE,
)
from collections_platform.transform_investing import build_investing_performance
from collections_platform.transform_servicing import build_servicing_performance


def test_fact_domains_do_not_share_measures():
    """A contract test on the model itself.

    ERC and purchase_price are meaningless in Servicing (we never owned the debt);
    commission is meaningless in Investing (we are the principal). If someone
    merges the two facts, this fails.
    """
    investing = {f.name for f in FCT_INVESTING_PERFORMANCE.fields}
    servicing = {f.name for f in FCT_SERVICING_PERFORMANCE.fields}

    assert {"erc_remaining", "purchase_price", "money_multiple"} <= investing
    assert not ({"erc_remaining", "purchase_price", "money_multiple"} & servicing)

    assert {"commission_rate", "commission_earned", "sla_met"} <= servicing
    assert not ({"commission_rate", "commission_earned", "sla_met"} & investing)

    # Different grain: Investing is portfolio-level, Servicing is case-level.
    assert "case_reference" not in investing
    assert "case_reference" in servicing


# ---------------------------------------------------------------------------
# Investing
# ---------------------------------------------------------------------------
def test_erc_is_floored_at_zero(spark):
    """An over-performing portfolio has zero remaining collections, not negative."""
    payments = spark.createDataFrame(
        [("PAY1", "CASE_1", date(2024, 1, 15), Decimal("5000.00"))],
        "payment_id string, case_reference string, payment_date date, amount decimal(18,2)",
    )
    cases = spark.createDataFrame([("CASE_1", "P0")], "case_reference string, portfolio_id string")
    portfolios = spark.createDataFrame(
        [("P0", date(2023, 1, 1), Decimal("1000.00"), Decimal("4000.00"))],
        "portfolio_id string, purchase_date date, purchase_price decimal(18,2), "
        "gross_face_value decimal(18,2)",
    )
    curve = spark.createDataFrame(
        [("P0", 12, Decimal("0.500000"))],
        "portfolio_id string, months_on_book int, forecast_pct decimal(9,6)",
    )

    out = build_investing_performance(payments, cases, portfolios, curve).collect()[0]
    # forecast total = 4000 * 0.5 = 2000; collected 5000 -> ERC must clamp to 0
    assert out["erc_remaining"] == Decimal("0.00")
    assert out["money_multiple"] == Decimal("5.000000")   # 5000 / 1000


def test_months_on_book_excludes_pre_purchase_payments(spark):
    """A payment dated before we bought the portfolio is a data error, not a
    month -1 data point."""
    payments = spark.createDataFrame(
        [
            ("PAY1", "CASE_1", date(2022, 6, 1), Decimal("100.00")),  # before purchase
            ("PAY2", "CASE_1", date(2023, 6, 1), Decimal("200.00")),
        ],
        "payment_id string, case_reference string, payment_date date, amount decimal(18,2)",
    )
    cases = spark.createDataFrame([("CASE_1", "P0")], "case_reference string, portfolio_id string")
    portfolios = spark.createDataFrame(
        [("P0", date(2023, 1, 1), Decimal("1000.00"), Decimal("4000.00"))],
        "portfolio_id string, purchase_date date, purchase_price decimal(18,2), "
        "gross_face_value decimal(18,2)",
    )
    curve = spark.createDataFrame(
        [], "portfolio_id string, months_on_book int, forecast_pct decimal(9,6)"
    )
    rows = build_investing_performance(payments, cases, portfolios, curve).collect()
    assert len(rows) == 1
    assert rows[0]["months_on_book"] == 5


# ---------------------------------------------------------------------------
# Servicing
# ---------------------------------------------------------------------------
def _servicing_inputs(spark):
    payments = spark.createDataFrame(
        [
            ("PAY1", "CASE_1", date(2023, 6, 15), Decimal("1000.00")),
            ("PAY2", "CASE_1", date(2024, 6, 15), Decimal("1000.00")),
        ],
        "payment_id string, case_reference string, payment_date date, amount decimal(18,2)",
    )
    cases = spark.createDataFrame(
        [("CASE_1", "CLIENT_A", date(2023, 1, 1), date(2023, 1, 20))],
        "case_reference string, client_id string, placed_date date, first_contact_date date",
    )
    # SCD2: rate cut from 15% to 10% at the start of 2024.
    contracts = spark.createDataFrame(
        [
            ("CLIENT_A", Decimal("0.150000"), 30, date(2020, 1, 1), date(2024, 1, 1)),
            ("CLIENT_A", Decimal("0.100000"), 30, date(2024, 1, 1), None),
        ],
        "client_id string, commission_rate decimal(9,6), sla_target_days int, "
        "valid_from date, valid_to date",
    )
    return payments, cases, contracts


def test_commission_uses_rate_valid_at_payment_time(spark):
    """The SCD2 point. Joining on client_id alone would restate 2023 revenue at
    the 2024 rate -- a silent, material error nobody notices until audit."""
    out = {
        r["as_of_month"]: r
        for r in build_servicing_performance(*_servicing_inputs(spark)).collect()
    }
    assert out[date(2023, 6, 1)]["commission_earned"] == Decimal("150.00")   # 15%
    assert out[date(2024, 6, 1)]["commission_earned"] == Decimal("100.00")   # 10%


def test_sla_pending_is_not_breached(spark):
    """A case never contacted is PENDING, not BREACHED. Collapsing the two
    overstates breaches and misprices the client KPI."""
    payments, cases, contracts = _servicing_inputs(spark)
    cases_no_contact = cases.withColumn("first_contact_date", F.lit(None).cast("date"))
    out = build_servicing_performance(payments, cases_no_contact, contracts).collect()
    assert all(r["sla_met"] == "PENDING" for r in out)


def test_sla_met_boundary_is_inclusive(spark):
    """Contacted exactly on the target day = MET, not BREACHED."""
    payments, cases, contracts = _servicing_inputs(spark)
    exact = cases.withColumn("first_contact_date", F.lit(date(2023, 1, 31)))  # 30 days
    out = build_servicing_performance(payments, exact, contracts).collect()
    assert all(r["sla_met"] == "MET" for r in out)
