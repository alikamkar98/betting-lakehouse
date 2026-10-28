# Tests for the gold layer: payout rules and the KPI formulas
from decimal import Decimal

from pyspark.sql import functions as F

from betting_pipeline import gold


def make_bets(spark):
    # The example from the study guide: 4 bets of partner P-1 on one day
    rows = [
        ("B-1", "C-1", "P-1", "10.00", "2.00", "won"),
        ("B-2", "C-2", "P-1", "20.00", "1.50", "lost"),
        ("B-3", "C-1", "P-1", "5.00", "3.00", "void"),
        ("B-4", "C-3", "P-1", "15.00", "1.80", "open"),
    ]
    df = spark.createDataFrame(rows, ["bet_id", "customer_id", "partner_id", "stake", "odds", "status"])
    df = df.withColumn("stake", F.col("stake").cast("decimal(10,2)"))
    df = df.withColumn("odds", F.col("odds").cast("decimal(10,2)"))
    df = df.withColumn("bet_date", F.lit("2026-10-26").cast("date"))
    return df


def test_payout_rules(spark):
    fact = gold.add_payout(make_bets(spark))
    payouts = {}
    for row in fact.collect():
        payouts[row["bet_id"]] = row["payout"]

    assert payouts["B-1"] == Decimal("20.00")  # won: stake x odds
    assert payouts["B-2"] == Decimal("0.00")   # lost: nothing
    assert payouts["B-3"] == Decimal("5.00")   # void: stake back
    assert payouts["B-4"] is None              # open: not known yet


def test_partner_daily_kpis(spark):
    fact = gold.add_payout(make_bets(spark))
    kpi = gold.partner_daily_kpis(fact).first()

    assert kpi["bets"] == 4
    assert kpi["active_customers"] == 3
    assert kpi["turnover"] == Decimal("50.00")          # all stakes
    assert kpi["settled_turnover"] == Decimal("35.00")  # without the open bet
    assert kpi["payouts"] == Decimal("25.00")
    assert kpi["ggr"] == Decimal("10.00")               # 35 - 25
    assert float(kpi["margin"]) == 0.2857               # 10 / 35
