# Databricks notebook source
# Job task 4: quality gate. Saves every check result; fails the job if any check fails.

# COMMAND ----------
from datetime import datetime

from betting_pipeline import config, quality, run_log

dbutils.widgets.text("batch_id", "day1")
dbutils.widgets.text("run_id", "manual")
batch_id = dbutils.widgets.get("batch_id")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------
started_at = datetime.now()
results = quality.run_checks(spark, config.SCHEMA, batch_id, config.MAX_REJECT_RATE)
quality.save_results(spark, config.SCHEMA, run_id, batch_id, results)
for name, value, passed in results:
    print(name, value, "PASSED" if passed else "FAILED")

failed = quality.failed_checks(results)
if len(failed) > 0:
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "quality", started_at,
                     len(results), len(results) - len(failed), len(failed), "FAILED", ", ".join(failed))
    raise Exception("Quality checks failed: " + ", ".join(failed))

run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "quality", started_at,
                 len(results), len(results), 0, "SUCCESS", "all checks passed")
