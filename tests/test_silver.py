# Tests for the silver layer with tiny hand-made DataFrames
from betting_pipeline import silver

BET_COLUMNS = ["bet_id", "customer_id", "partner_id", "event_id", "market", "selection",
               "stake", "odds", "status", "placed_at", "updated_at", "batch_id", "source_file"]


def make_bronze_bets(spark, rows):
    # Bronze stores everything as text, so the test data is text too
    return spark.createDataFrame(rows, BET_COLUMNS)


def bet(bet_id, customer_id="C-1", event_id="E-1", stake="10", odds="2.00", status="open",
        placed_at="2026-10-26 10:00:00", updated_at="2026-10-26 10:00:00"):
    return (bet_id, customer_id, "P-1", event_id, "match_winner", "home",
            stake, odds, status, placed_at, updated_at, "day1", "test.csv")


def test_quality_rules_give_the_right_reject_reason(spark):
    rows = [
        bet("B-1"),                                   # good
        bet("B-2", customer_id=None),                 # missing customer
        bet("B-3", stake="-5"),                       # negative stake
        bet("B-4", stake="abc"),                      # stake not a number
        bet("B-5", odds="1.00"),                      # odds too low
        bet("B-6", event_id="E-999"),                 # unknown event
        bet("B-7", placed_at="2026-13-45 25:61:00"),  # broken timestamp
    ]
    bronze_bets = make_bronze_bets(spark, rows)
    events = spark.createDataFrame([("E-1",)], ["event_id"])

    checked = silver.add_reject_reason(silver.cast_bets(bronze_bets), events)
    reasons = {}
    for row in checked.collect():
        reasons[row["bet_id"]] = row["reject_reason"]

    assert reasons["B-1"] is None
    assert reasons["B-2"] == "missing_customer_id"
    assert reasons["B-3"] == "invalid_stake"
    assert reasons["B-4"] == "invalid_stake"
    assert reasons["B-5"] == "invalid_odds"
    assert reasons["B-6"] == "unknown_event"
    assert reasons["B-7"] == "invalid_timestamp"


def test_latest_per_bet_keeps_newest_version_and_removes_duplicates(spark):
    rows = [
        bet("B-1", status="open", updated_at="2026-10-26 10:00:00"),
        bet("B-1", status="won", updated_at="2026-10-27 21:00:00"),
        bet("B-1", status="won", updated_at="2026-10-27 21:00:00"),  # exact duplicate
        bet("B-2", status="open"),
    ]
    typed = silver.cast_bets(make_bronze_bets(spark, rows))
    latest = silver.latest_per_bet(typed)

    assert latest.count() == 2
    b1 = latest.filter("bet_id = 'B-1'").first()
    assert b1["status"] == "won"


def test_merge_is_idempotent_and_updates_only_newer_rows(spark):
    table = "test_betting.silver_bets_merge_test"
    spark.sql("DROP TABLE IF EXISTS " + table)

    day1 = silver.cast_bets(make_bronze_bets(spark, [bet("B-1", status="open"), bet("B-2")]))
    silver.merge_bets(spark, day1, table)

    day2_rows = [bet("B-1", status="won", updated_at="2026-10-27 21:00:00"), bet("B-3")]
    day2 = silver.cast_bets(make_bronze_bets(spark, day2_rows))
    silver.merge_bets(spark, day2, table)
    count_after_day2 = spark.table(table).count()

    # Run day 2 again: nothing may change
    second_run = silver.merge_bets(spark, day2, table)
    count_after_repeat = spark.table(table).count()

    assert count_after_day2 == 3
    assert count_after_repeat == 3
    assert second_run["num_affected_rows"] == 0
    assert spark.table(table).filter("bet_id = 'B-1'").first()["status"] == "won"

    # An OLDER version must not overwrite a newer one
    old_row = silver.cast_bets(make_bronze_bets(spark, [bet("B-1", status="open")]))
    silver.merge_bets(spark, old_row, table)
    assert spark.table(table).filter("bet_id = 'B-1'").first()["status"] == "won"


def test_scd2_keeps_history_and_is_idempotent(spark):
    table = "test_betting.customers_scd2_test"
    spark.sql("DROP TABLE IF EXISTS " + table)
    columns = ["customer_id", "partner_id", "country", "risk_segment"]

    day1 = spark.createDataFrame([("C-1", "P-1", "AT", "low"), ("C-2", "P-1", "DE", "low")], columns)
    silver.scd2_customers(spark, day1, "2026-10-26", table)

    # Day 2: C-1 moves to Germany, C-2 unchanged
    day2 = spark.createDataFrame([("C-1", "P-1", "DE", "low"), ("C-2", "P-1", "DE", "low")], columns)
    silver.scd2_customers(spark, day2, "2026-10-27", table)
    silver.scd2_customers(spark, day2, "2026-10-27", table)  # repeat: must change nothing

    history = spark.table(table).filter("customer_id = 'C-1'").orderBy("valid_from").collect()
    assert len(history) == 2
    assert history[0]["country"] == "AT"
    assert history[0]["is_current"] is False
    assert history[1]["country"] == "DE"
    assert history[1]["is_current"] is True
    assert spark.table(table).count() == 3
