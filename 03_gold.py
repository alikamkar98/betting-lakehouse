# Databricks notebook source
# Job task 3: silver -> gold (star schema and the partner KPI table)

# COMMAND ----------
from datetime import datetime

from betting_pipeline import config, gold, run_log

dbutils.widgets.text("batch_id", "day1")
dbutils.widgets.text("run_id", "manual")
batch_id = dbutils.widgets.get("batch_id")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------
started_at = datetime.now()
try:
    result = gold.build_gold(spark, config.SCHEMA)
    print(result)
    silver_rows = spark.table(config.SCHEMA + ".silver_bets").count()
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "gold", started_at,
                     silver_rows, result["fact_bet"], 0, "SUCCESS", str(result))
except Exception as error:
    run_log.log_step(spark, config.SCHEMA, run_id, batch_id, "gold", started_at,
                     None, None, None, "FAILED", str(error)[:500])
    raise
