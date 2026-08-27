"""Investing domain: portfolios Intrum BUYS and collects on its own balance sheet.

Measures that only exist here: purchase price, ERC (estimated remaining
collections), money multiple, recovery curve position. There is no client and no
commission -- we are the principal, not the agent.

Grain: portfolio x as-of month.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_investing_performance(
    payments: DataFrame,
    cases: DataFrame,
    portfolios: DataFrame,
    forecast_curve: DataFrame,
) -> DataFrame:
    """
    payments      : payment_id, case_reference, payment_date, amount
    cases         : case_reference, portfolio_id, current_balance
    portfolios    : portfolio_id, purchase_date, purchase_price, gross_face_value
    forecast_curve: portfolio_id, months_on_book, forecast_pct  (underwriting curve)
    """
    case_to_portfolio = cases.select("case_reference", "portfolio_id")

    monthly = (
        payments.join(F.broadcast(case_to_portfolio), on="case_reference", how="inner")
        .withColumn("as_of_month", F.trunc(F.col("payment_date"), "month"))
        .groupBy("portfolio_id", "as_of_month")
        .agg(F.sum("amount").alias("collections_actual"))
    )

    # months_on_book is what makes two portfolios bought in different years
    # comparable. Comparing them by calendar month is the classic beginner error:
    # a 2019 portfolio at month 60 and a 2025 portfolio at month 3 are not peers.
    with_mob = (
        monthly.join(F.broadcast(portfolios), on="portfolio_id", how="left")
        .withColumn(
            "months_on_book",
            F.months_between(F.col("as_of_month"), F.trunc(F.col("purchase_date"), "month")).cast(
                "int"
            ),
        )
        .filter(F.col("months_on_book") >= 0)
    )

    # Cumulative collections over the life of the portfolio.
    # Window is bounded by the portfolio, so partition skew here maps directly to
    # portfolio-size skew -- see tests/test_skew.py.
    life_window = (
        Window.partitionBy("portfolio_id")
        .orderBy("as_of_month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    enriched = (
        with_mob.withColumn("collections_cumulative", F.sum("collections_actual").over(life_window))
        .join(F.broadcast(forecast_curve), on=["portfolio_id", "months_on_book"], how="left")
        .withColumn(
            "collections_forecast",
            (F.col("gross_face_value") * F.col("forecast_pct")).cast("decimal(18,2)"),
        )
        # ERC = what underwriting said the whole portfolio would return, minus
        # what we have actually banked. Floored at zero: a portfolio that
        # over-performed has no *negative* remaining collections.
        .withColumn(
            "erc_remaining",
            F.greatest(
                F.lit(0).cast("decimal(18,2)"),
                (F.col("gross_face_value") * F.col("forecast_pct") - F.col("collections_cumulative")
                 ).cast("decimal(18,2)"),
            ),
        )
        .withColumn(
            "recovery_rate_to_date",
            F.when(F.col("gross_face_value") > 0,
                   (F.col("collections_cumulative") / F.col("gross_face_value")).cast("decimal(9,6)"))
            .otherwise(F.lit(None).cast("decimal(9,6)")),
        )
        .withColumn(
            "money_multiple",
            F.when(F.col("purchase_price") > 0,
                   (F.col("collections_cumulative") / F.col("purchase_price")).cast("decimal(9,6)"))
            .otherwise(F.lit(None).cast("decimal(9,6)")),
        )
    )

    return enriched.select(
        "portfolio_id",
        "as_of_month",
        "months_on_book",
        "collections_actual",
        "collections_forecast",
        "erc_remaining",
        "recovery_rate_to_date",
        "purchase_price",
        "money_multiple",
    )
