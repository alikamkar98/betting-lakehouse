# Databricks notebook source
# Job task 1: landing CSV files -> bronze Delta tables (raw, as delivered, plus load metadata)

# COMMAND ----------
from datetime import datetime

from betting_pipeline import bronze, config, run_log

# Parameters come from the Job (batch_id, run_id). Defaults allow a manual run.
dbutils.widgets.text("batch_id", "day1")
dbutils.widgets.text("run_id", "manual")
batch_id = dbutils.widgets.get("batch_id")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------
started_at = datetime.now()
try:
    counts = bronze.load_batch(spark, config.LANDING, config.SCHEMA, batch_id)
    print(counts)
    bet_rows = counts["bets"]
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "bronze", started_at,
                     bet_rows, bet_rows, 0, "SUCCESS", str(counts))
except Exception as error:
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "bronze", started_at,
                     None, None, None, "FAILED", str(error)[:500])
    raise
