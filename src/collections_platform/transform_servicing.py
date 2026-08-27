"""Servicing domain: collecting on behalf of a THIRD-PARTY client.

Measures that only exist here: commission rate and earned commission, SLA
targets, per-client KPIs. There is no purchase price and no ERC -- the debt was
never on our balance sheet, so "estimated remaining collections" is not our
number to report.

Grain: client x case x as-of month. Finer than Investing on purpose -- SLA is
contracted per case, so aggregating to portfolio level would destroy it.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_servicing_performance(
    payments: DataFrame,
    cases: DataFrame,
    client_contracts: DataFrame,
) -> DataFrame:
    """
    payments        : case_reference, payment_date, amount
    cases           : case_reference, client_id, placed_date, first_contact_date
    client_contracts: client_id, commission_rate, sla_target_days,
                      valid_from, valid_to   <- SCD2, and that matters below
    """
    # Only placed cases belong in the Servicing fact. A case we bought outright
    # has no client, and letting it through would produce a fact row with a NULL
    # client_id and a NULL commission -- i.e. an Investing case wearing a
    # Servicing costume, inflating every per-client aggregate that ignores NULLs.
    case_client = cases.select(
        "case_reference", "client_id", "placed_date", "first_contact_date"
    ).filter(F.col("client_id").isNotNull())

    monthly = (
        payments.join(F.broadcast(case_client), on="case_reference", how="inner")
        .withColumn("as_of_month", F.trunc(F.col("payment_date"), "month"))
        .groupBy("client_id", "case_reference", "as_of_month", "placed_date", "first_contact_date")
        .agg(F.sum("amount").alias("collections_actual"))
    )

    # SCD2 join: the commission rate that applied ON THE PAYMENT DATE, not the
    # rate that applies today. Joining on client_id alone is the single most
    # common way to silently restate historical revenue after a contract renegotiation.
    priced = monthly.join(
        F.broadcast(client_contracts),
        on=(monthly.client_id == client_contracts.client_id)
        & (monthly.as_of_month >= client_contracts.valid_from)
        & (monthly.as_of_month < F.coalesce(client_contracts.valid_to, F.lit("9999-12-31").cast("date"))),
        how="left",
    ).drop(client_contracts.client_id)

    return (
        priced.withColumn(
            "commission_earned",
            (F.col("collections_actual") * F.col("commission_rate")).cast("decimal(18,2)"),
        )
        .withColumn(
            "sla_actual_days",
            F.datediff(F.col("first_contact_date"), F.col("placed_date")).cast("int"),
        )
        .withColumn(
            "sla_met",
            # Three-valued on purpose: a case never contacted is not "failed",
            # it is "pending". Collapsing that into False overstates breaches.
            F.when(F.col("first_contact_date").isNull(), F.lit("PENDING"))
            .when(F.col("sla_actual_days") <= F.col("sla_target_days"), F.lit("MET"))
            .otherwise(F.lit("BREACHED")),
        )
        .select(
            "client_id",
            "case_reference",
            "as_of_month",
            "collections_actual",
            "commission_rate",
            "commission_earned",
            "sla_target_days",
            "sla_actual_days",
            "sla_met",
        )
    )
