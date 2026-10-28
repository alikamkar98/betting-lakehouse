# Databricks notebook source
# Job task 2: bronze -> silver (types, quality rules, quarantine, dedup, MERGE, SCD2 customers)

# COMMAND ----------
from datetime import datetime

from betting_pipeline import config, run_log, silver

dbutils.widgets.text("batch_id", "day1")
dbutils.widgets.text("run_id", "manual")
batch_id = dbutils.widgets.get("batch_id")
run_id = dbutils.widgets.get("run_id")
batch_date = config.BATCH_DATES[batch_id]

# COMMAND ----------
started_at = datetime.now()
try:
    result = silver.run_silver(spark, config.SCHEMA, batch_id, batch_date)
    print(result)
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "silver", started_at,
                     result["rows_in"], result["rows_out"], result["rows_rejected"], "SUCCESS", str(result["merge"]))
except Exception as error:
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "silver", started_at,
                     None, None, None, "FAILED", str(error)[:500])
    raise
