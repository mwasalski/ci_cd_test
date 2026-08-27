"""Explicit schemas = data contracts.

Never `inferSchema` on an inbound originator file. Inference makes the schema a
function of *this batch's data*, so a month where every `interest_rate` happens to
be a whole number silently gives you a LongType column and your next append fails
-- or worse, succeeds with a bad cast.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Money is DecimalType, never DoubleType. 0.1 + 0.2 != 0.3 matters when the
# number is a debt balance that has to reconcile to a bank statement.
MONEY = DecimalType(18, 2)
RATE = DecimalType(9, 6)

# --------------------------------------------------------------------------
# Bronze: what the originator actually sends. Everything is a string except
# what we are contractually certain of -- cast in silver, where failures are
# visible and quarantinable instead of being a NULL in a load you never look at.
# --------------------------------------------------------------------------
PORTFOLIO_CASE_RAW = StructType(
    [
        StructField("case_reference", StringType(), nullable=False),
        StructField("portfolio_id", StringType(), nullable=False),
        StructField("national_id", StringType(), nullable=True),
        StructField("debtor_name", StringType(), nullable=True),
        StructField("debtor_email", StringType(), nullable=True),
        StructField("debtor_phone", StringType(), nullable=True),
        StructField("debtor_address", StringType(), nullable=True),
        StructField("postcode_area", StringType(), nullable=True),
        StructField("date_of_birth", StringType(), nullable=True),
        StructField("original_balance", StringType(), nullable=True),
        StructField("current_balance", StringType(), nullable=True),
        StructField("default_date", StringType(), nullable=True),
        StructField("product_type", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
    ]
)

# Columns an originator is allowed to add without breaking us (additive drift).
# Anything outside this and the base schema triggers a review, not a silent pass.
OPTIONAL_INBOUND_COLUMNS = frozenset(
    {"segment_code", "collection_status", "last_payment_date", "originator_score"}
)

PAYMENT_RAW = StructType(
    [
        StructField("payment_id", StringType(), nullable=False),
        StructField("case_reference", StringType(), nullable=False),
        StructField("payment_date", DateType(), nullable=False),
        StructField("amount", MONEY, nullable=False),
        StructField("channel", StringType(), nullable=True),
    ]
)

# --------------------------------------------------------------------------
# Gold: two fact domains. Different grain, different measures, different
# dimensions. This is the modelling point worth defending in an interview --
# forcing both into one `fct_collections` destroys the grain.
# --------------------------------------------------------------------------

# Grain: one row per portfolio per as-of month. Investing = we own the debt.
FCT_INVESTING_PERFORMANCE = StructType(
    [
        StructField("portfolio_id", StringType(), nullable=False),
        StructField("as_of_month", DateType(), nullable=False),
        StructField("months_on_book", IntegerType(), nullable=False),
        StructField("collections_actual", MONEY, nullable=False),
        StructField("collections_forecast", MONEY, nullable=True),
        StructField("erc_remaining", MONEY, nullable=True),   # estimated remaining collections
        StructField("recovery_rate_to_date", RATE, nullable=True),
        StructField("purchase_price", MONEY, nullable=True),
        StructField("money_multiple", RATE, nullable=True),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_batch_id", StringType(), nullable=False),
    ]
)

# Grain: one row per client per case per month. Servicing = we collect for someone else.
# Note there is NO purchase_price and NO ERC here: we never owned the debt.
FCT_SERVICING_PERFORMANCE = StructType(
    [
        StructField("client_id", StringType(), nullable=False),
        StructField("case_reference", StringType(), nullable=False),
        StructField("as_of_month", DateType(), nullable=False),
        StructField("collections_actual", MONEY, nullable=False),
        StructField("commission_rate", RATE, nullable=False),
        StructField("commission_earned", MONEY, nullable=False),
        StructField("sla_target_days", IntegerType(), nullable=True),
        StructField("sla_actual_days", IntegerType(), nullable=True),
        StructField("sla_met", StringType(), nullable=True),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_batch_id", StringType(), nullable=False),
    ]
)
