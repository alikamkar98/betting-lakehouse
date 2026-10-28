# Gold: star schema (fact_bet + dimensions) and the partner KPI table for Power BI.
# Gold is rebuilt from silver on every run (full refresh). It is small, and a rebuild is idempotent by design.
from pyspark.sql import functions as F

SETTLED_STATUSES = ["won", "lost", "void"]


def add_payout(bets):
    """won: stake x odds, lost: 0, void: stake back, open: not known yet (NULL)."""
    payout = (
        F.when(F.col("status") == "won", F.round(F.col("stake") * F.col("odds"), 2))
        .when(F.col("status") == "lost", F.lit(0))
        .when(F.col("status") == "void", F.col("stake"))
    )
    df = bets.withColumn("payout", payout.cast("decimal(12,2)"))
    df = df.withColumn("is_settled", F.col("status").isin(SETTLED_STATUSES))
    return df


def partner_daily_kpis(fact_bet):
    """One row per day and partner. GGR = settled stakes - payouts. Margin = GGR / settled stakes."""
    settled_stake = F.when(F.col("is_settled"), F.col("stake")).otherwise(F.lit(0))
    kpis = fact_bet.groupBy("bet_date", "partner_id").agg(
        F.count("bet_id").alias("bets"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum("stake").alias("turnover"),
        F.sum(settled_stake).alias("settled_turnover"),
        F.sum(F.coalesce(F.col("payout"), F.lit(0))).alias("payouts"),
    )
    kpis = kpis.withColumn("ggr", F.col("settled_turnover") - F.col("payouts"))
    kpis = kpis.withColumn("margin", F.round(F.col("ggr") / F.col("settled_turnover"), 4))
    return kpis


def build_gold(spark, schema):
    # Dimensions
    dim_partner = spark.table(schema + ".silver_partners")
    dim_partner.write.format("delta").mode("overwrite").saveAsTable(schema + ".dim_partner")

    dim_event = spark.table(schema + ".silver_events")
    dim_event = dim_event.withColumn("event_date", F.to_date("start_time"))
    dim_event.write.format("delta").mode("overwrite").saveAsTable(schema + ".dim_event")

    # Customer dimension keeps the SCD2 history. customer_key points to one version of a customer.
    dim_customer = spark.table(schema + ".silver_customers")
    customer_key = F.concat_ws("_", F.col("customer_id"), F.date_format("valid_from", "yyyyMMdd"))
    dim_customer = dim_customer.withColumn("customer_key", customer_key)
    dim_customer.write.format("delta").mode("overwrite").saveAsTable(schema + ".dim_customer")

    # Fact: grain = one row per bet (latest version).
    # Each bet joins the customer version that was valid when the bet was placed.
    bets = spark.table(schema + ".silver_bets").alias("b")
    customers = spark.table(schema + ".dim_customer").alias("c")
    join_condition = (
        (F.col("b.customer_id") == F.col("c.customer_id"))
        & (F.col("b.placed_at") >= F.col("c.valid_from"))
        & (F.col("b.placed_at") < F.col("c.valid_to"))
    )
    fact = bets.join(customers, join_condition, "left")
    fact = fact.select(
        F.col("b.bet_id"),
        F.to_date(F.col("b.placed_at")).alias("bet_date"),
        F.col("c.customer_key"),
        F.col("b.customer_id"),
        F.col("b.partner_id"),
        F.col("b.event_id"),
        F.col("b.market"),
        F.col("b.selection"),
        F.col("b.stake"),
        F.col("b.odds"),
        F.col("b.status"),
        F.col("b.placed_at"),
        F.col("b.updated_at"),
        F.col("b.batch_id"),
    )
    fact = add_payout(fact)
    fact.write.format("delta").mode("overwrite").saveAsTable(schema + ".fact_bet")

    # KPI table for Power BI
    fact_bet = spark.table(schema + ".fact_bet")
    kpis = partner_daily_kpis(fact_bet)
    kpis.write.format("delta").mode("overwrite").saveAsTable(schema + ".gold_partner_daily_kpis")

    return {
        "fact_bet": fact_bet.count(),
        "kpi_rows": spark.table(schema + ".gold_partner_daily_kpis").count(),
    }
