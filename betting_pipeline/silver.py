# Silver: correct types, quality rules (bad rows to quarantine), dedup, MERGE, SCD2 customers
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from betting_pipeline.common import merge_upsert, overwrite_batch

VALID_STATUSES = ["open", "won", "lost", "void"]


# ---------- Bets: pure functions (no tables, easy to test) ----------

def cast_bets(bronze_bets):
    """Convert text to real types. try_cast turns a bad value into NULL instead of crashing the job."""
    df = bronze_bets.select(
        F.col("bet_id"),
        F.col("customer_id"),
        F.col("partner_id"),
        F.col("event_id"),
        F.col("market"),
        F.col("selection"),
        F.expr("try_cast(stake AS DECIMAL(10,2))").alias("stake"),
        F.expr("try_cast(odds AS DECIMAL(10,2))").alias("odds"),
        F.lower(F.col("status")).alias("status"),
        F.expr("try_cast(placed_at AS TIMESTAMP)").alias("placed_at"),
        F.expr("try_cast(updated_at AS TIMESTAMP)").alias("updated_at"),
        F.col("batch_id"),
        F.col("source_file"),
    )
    return df


def add_reject_reason(bets, events):
    """Add a reject_reason column. The first rule that fails gives the reason. NULL means the row is good."""
    known_events = events.select(F.col("event_id").alias("known_event_id")).distinct()
    df = bets.join(known_events, bets["event_id"] == known_events["known_event_id"], "left")

    reason = (
        F.when(F.col("bet_id").isNull(), "missing_bet_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("stake").isNull() | (F.col("stake") <= 0), "invalid_stake")
        .when(F.col("odds").isNull() | (F.col("odds") < 1.01), "invalid_odds")
        .when(F.col("placed_at").isNull() | F.col("updated_at").isNull(), "invalid_timestamp")
        .when(F.col("status").isNull() | ~F.col("status").isin(VALID_STATUSES), "invalid_status")
        .when(F.col("known_event_id").isNull(), "unknown_event")
    )
    df = df.withColumn("reject_reason", reason)
    df = df.drop("known_event_id")
    return df


def latest_per_bet(bets):
    """Keep one row per bet: the newest version. Removes exact duplicates and keeps the latest settlement."""
    window = Window.partitionBy("bet_id").orderBy(F.col("updated_at").desc())
    df = bets.withColumn("version_rank", F.row_number().over(window))
    df = df.filter(F.col("version_rank") == 1)
    df = df.drop("version_rank")
    return df


# ---------- Bets: writing to tables ----------

def merge_bets(spark, latest_bets, table_name):
    """MERGE on bet_id: update only if the incoming row is newer, insert if the bet is new.
    Running the same batch twice changes nothing: that makes the load idempotent."""
    if not spark.catalog.tableExists(table_name):
        latest_bets.limit(0).write.format("delta").saveAsTable(table_name)
    latest_bets.createOrReplaceTempView("incoming_bets")
    result = spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING incoming_bets AS source
        ON target.bet_id = source.bet_id
        WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return result.first().asDict()


def load_bets(spark, schema, batch_id):
    bronze_bets = spark.table(schema + ".bronze_bets").filter(F.col("batch_id") == batch_id)
    events = spark.table(schema + ".silver_events")

    typed = cast_bets(bronze_bets)
    checked = add_reject_reason(typed, events)

    # Bad rows: to quarantine, with the reason and the original batch and file
    rejected = checked.filter(F.col("reject_reason").isNotNull())
    rejected = rejected.withColumn("quarantined_at", F.current_timestamp())
    overwrite_batch(spark, rejected, schema + ".quarantine_bets", batch_id)

    # Good rows: newest version per bet, then MERGE into silver
    good = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    latest = latest_per_bet(good)
    merge_result = merge_bets(spark, latest, schema + ".silver_bets")

    rows_in = bronze_bets.count()
    rows_rejected = spark.table(schema + ".quarantine_bets").filter(F.col("batch_id") == batch_id).count()
    rows_out = latest.count()
    return {
        "rows_in": rows_in,
        "rows_rejected": rows_rejected,
        "rows_out": rows_out,
        "merge": merge_result,
    }


# ---------- Small reference tables (SCD Type 1: just keep the latest) ----------

def load_partners(spark, schema, batch_id):
    df = spark.table(schema + ".bronze_partners").filter(F.col("batch_id") == batch_id)
    df = df.select("partner_id", "partner_name", "country").dropDuplicates(["partner_id"])
    return merge_upsert(spark, df, schema + ".silver_partners", "partner_id")


def load_events(spark, schema, batch_id):
    df = spark.table(schema + ".bronze_events").filter(F.col("batch_id") == batch_id)
    df = df.select(
        F.col("event_id"),
        F.col("sport"),
        F.col("home_team"),
        F.col("away_team"),
        F.expr("try_cast(start_time AS TIMESTAMP)").alias("start_time"),
    )
    df = df.dropDuplicates(["event_id"])
    return merge_upsert(spark, df, schema + ".silver_events", "event_id")


# ---------- Customers: SCD Type 2 (keep history) ----------

def scd2_customers(spark, incoming, batch_date, table_name):
    """Keep the history of customers with valid_from, valid_to and is_current.
    Step 1 closes the current row of customers that changed. Step 2 inserts a new current row
    for every customer that has no current row (new customers and the ones just closed)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            customer_id STRING,
            partner_id STRING,
            country STRING,
            risk_segment STRING,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN
        ) USING DELTA
    """)
    incoming.createOrReplaceTempView("incoming_customers")

    # Step 1: close the current row if something changed (<=> is "equal", also safe for NULLs)
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING incoming_customers AS source
        ON target.customer_id = source.customer_id AND target.is_current = true
        WHEN MATCHED AND (
            NOT (target.country <=> source.country)
            OR NOT (target.risk_segment <=> source.risk_segment)
            OR NOT (target.partner_id <=> source.partner_id)
        )
        THEN UPDATE SET target.is_current = false, target.valid_to = TIMESTAMP '{batch_date}'
    """)

    # Step 2: insert a new current row where none exists
    spark.sql(f"""
        INSERT INTO {table_name}
        SELECT s.customer_id, s.partner_id, s.country, s.risk_segment,
               TIMESTAMP '{batch_date}' AS valid_from,
               TIMESTAMP '9999-12-31' AS valid_to,
               true AS is_current
        FROM incoming_customers s
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} t
            WHERE t.customer_id = s.customer_id AND t.is_current = true
        )
    """)


def load_customers(spark, schema, batch_id, batch_date):
    df = spark.table(schema + ".bronze_customers").filter(F.col("batch_id") == batch_id)
    df = df.select("customer_id", "partner_id", "country", "risk_segment")
    df = df.filter(F.col("customer_id").isNotNull()).dropDuplicates(["customer_id"])
    scd2_customers(spark, df, batch_date, schema + ".silver_customers")


# ---------- The whole silver step ----------

def run_silver(spark, schema, batch_id, batch_date):
    load_partners(spark, schema, batch_id)
    load_events(spark, schema, batch_id)
    load_customers(spark, schema, batch_id, batch_date)
    return load_bets(spark, schema, batch_id)
