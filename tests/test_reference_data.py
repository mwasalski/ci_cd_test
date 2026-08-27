"""Reference data is data too.

`seed_synthetic` writes these three tables straight into silver, which means a
mistake here shows up as an empty fact table two jobs later. Cheaper to assert
the shape now than to debug "the pipeline succeeded and gold is empty".
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from collections_platform.synthetic import (
    generate_cases,
    generate_client_contracts,
    generate_forecast_curve,
    generate_payments,
    generate_portfolios,
)
from collections_platform.transform_investing import build_investing_performance
from collections_platform.transform_servicing import build_servicing_performance


@pytest.fixture(scope="module")
def portfolios(spark):
    return generate_portfolios(spark, n_portfolios=5)


def test_scd2_windows_do_not_overlap(spark):
    """Two rows valid on the same day would double every commission. The SCD2
    join in transform_servicing has no defence against that -- the generator
    must not produce it."""
    contracts = generate_client_contracts(spark)
    overlaps = (
        contracts.alias("a")
        .join(
            contracts.alias("b"),
            (F.col("a.client_id") == F.col("b.client_id"))
            & (F.col("a.valid_from") < F.col("b.valid_from"))
            & (
                F.coalesce(F.col("a.valid_to"), F.lit(date(9999, 12, 31)))
                > F.col("b.valid_from")
            ),
        )
        .count()
    )
    assert overlaps == 0


def test_every_client_has_a_currently_valid_row(spark):
    contracts = generate_client_contracts(spark)
    open_rows = contracts.filter(F.col("valid_to").isNull())
    assert open_rows.count() == contracts.select("client_id").distinct().count()


def test_forecast_curve_is_monotonic_per_portfolio(portfolios, spark):
    """A recovery curve that dips means ERC goes UP as you collect."""
    curve = generate_forecast_curve(spark, n_portfolios=5)
    w = F.lag("forecast_pct").over(
        Window.partitionBy("portfolio_id").orderBy("months_on_book")
    )
    dips = curve.withColumn("prev", w).filter(F.col("forecast_pct") < F.col("prev")).count()
    assert dips == 0


def test_seeded_data_actually_produces_both_fact_domains(spark, portfolios):
    """The end-to-end shape check. Both facts must come out non-empty from one
    case feed -- that is the whole reason cases carry an optional client_id."""
    cases = generate_cases(spark, n_cases=500, n_portfolios=5, skew_factor=0.4)
    payments = generate_payments(spark, cases)

    investing = build_investing_performance(
        payments=payments,
        cases=cases,
        portfolios=portfolios,
        forecast_curve=generate_forecast_curve(spark, n_portfolios=5),
    )
    servicing = build_servicing_performance(
        payments=payments,
        cases=cases,
        client_contracts=generate_client_contracts(spark),
    )

    assert investing.count() > 0, "no Investing rows: check purchase_date vs payment_date"
    assert servicing.count() > 0, "no Servicing rows: check client_id generation"

    # And the domains stay disjoint: a placement is not a bought portfolio.
    assert investing.filter(F.col("portfolio_id").startswith("PLACEMENT_")).count() == 0
    assert servicing.filter(F.col("client_id").isNull()).count() == 0
