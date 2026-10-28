# Quality gate: checks after each run. If one fails, the job fails, so wrong numbers are never published silently.
from datetime import datetime

from pyspark.sql import functions as F


def count_sql(spark, query):
    return spark.sql(query).first()[0]


def run_checks(spark, schema, batch_id, max_reject_rate):
    results = []

    # 1. Silver must have exactly one row per bet
    duplicates = count_sql(spark, f"""
        SELECT COUNT(*) FROM (
            SELECT bet_id FROM {schema}.silver_bets GROUP BY bet_id HAVING COUNT(*) > 1
        )
    """)
    results.append(("no_duplicate_bet_id_in_silver", float(duplicates), duplicates == 0))

    # 2. Gold facts must have all keys to the dimensions
    null_keys = count_sql(spark, f"""
        SELECT COUNT(*) FROM {schema}.fact_bet
        WHERE customer_key IS NULL OR partner_id IS NULL OR event_id IS NULL
    """)
    results.append(("no_null_keys_in_fact_bet", float(null_keys), null_keys == 0))

    # 3. Each customer has exactly one current row in the SCD2 history
    multi_current = count_sql(spark, f"""
        SELECT COUNT(*) FROM (
            SELECT customer_id FROM {schema}.silver_customers
            WHERE is_current GROUP BY customer_id HAVING COUNT(*) > 1
        )
    """)
    results.append(("one_current_row_per_customer", float(multi_current), multi_current == 0))

    # 4. Rejection rate of this batch must stay below the limit
    bronze_rows = spark.table(schema + ".bronze_bets").filter(F.col("batch_id") == batch_id).count()
    rejected_rows = spark.table(schema + ".quarantine_bets").filter(F.col("batch_id") == batch_id).count()
    reject_rate = rejected_rows / bronze_rows
    results.append(("reject_rate_below_limit", round(reject_rate, 4), reject_rate <= max_reject_rate))

    # 5. Every silver bet must appear in gold (reconciliation between layers)
    silver_rows = spark.table(schema + ".silver_bets").count()
    gold_rows = spark.table(schema + ".fact_bet").count()
    results.append(("silver_and_gold_bet_counts_match", float(gold_rows - silver_rows), silver_rows == gold_rows))

    return results


def save_results(spark, schema, run_id, batch_id, results):
    checked_at = datetime.now()
    rows = []
    for name, value, passed in results:
        rows.append((run_id, batch_id, name, value, passed, checked_at))
    columns = "run_id STRING, batch_id STRING, check_name STRING, value DOUBLE, passed BOOLEAN, checked_at TIMESTAMP"
    df = spark.createDataFrame(rows, columns)
    df.write.format("delta").mode("append").saveAsTable(schema + ".ops_quality_results")


def failed_checks(results):
    failed = []
    for name, value, passed in results:
        if not passed:
            failed.append(name + " (value " + str(value) + ")")
    return failed
