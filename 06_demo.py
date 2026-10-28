# Databricks notebook source
# Demo for the interview: what the pipeline produced, and the proof that it is idempotent and traceable.
# Every cell only reads; it changes nothing.

# COMMAND ----------
# MAGIC %md ## 1. Row counts per layer

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'bronze_bets' AS table_name, COUNT(*) AS row_count FROM workspace.betting.bronze_bets
# MAGIC UNION ALL SELECT 'quarantine_bets', COUNT(*) FROM workspace.betting.quarantine_bets
# MAGIC UNION ALL SELECT 'silver_bets', COUNT(*) FROM workspace.betting.silver_bets
# MAGIC UNION ALL SELECT 'fact_bet', COUNT(*) FROM workspace.betting.fact_bet

# COMMAND ----------
# MAGIC %md ## 2. Run log: one row per step per run (day1, day2, day2 again)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT run_id, batch_id, step, rows_in, rows_out, rows_rejected, status, message
# MAGIC FROM workspace.betting.ops_pipeline_runs
# MAGIC ORDER BY started_at

# COMMAND ----------
# MAGIC %md ## 3. Quarantine: every rejected row with its reason and source file

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT batch_id, reject_reason, COUNT(*) AS rows_rejected
# MAGIC FROM workspace.betting.quarantine_bets
# MAGIC GROUP BY batch_id, reject_reason
# MAGIC ORDER BY batch_id, reject_reason

# COMMAND ----------
# MAGIC %md ## 4. Idempotency: the second day2 MERGE inserted and updated 0 rows

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.betting.silver_bets

# COMMAND ----------
# MAGIC %md ## 5. Time travel: bet status after day 1 (version 1) versus now

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'after day1' AS moment, status, COUNT(*) AS bets
# MAGIC FROM workspace.betting.silver_bets VERSION AS OF 1
# MAGIC GROUP BY status
# MAGIC UNION ALL
# MAGIC SELECT 'now', status, COUNT(*)
# MAGIC FROM workspace.betting.silver_bets
# MAGIC GROUP BY status
# MAGIC ORDER BY moment, status

# COMMAND ----------
# MAGIC %md ## 6. SCD2: customers that changed keep both versions

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT customer_id, country, risk_segment, valid_from, valid_to, is_current
# MAGIC FROM workspace.betting.dim_customer
# MAGIC WHERE customer_id IN (SELECT customer_id FROM workspace.betting.dim_customer WHERE NOT is_current)
# MAGIC ORDER BY customer_id, valid_from

# COMMAND ----------
# MAGIC %md ## 7. Lineage: one gold bet traced back to bronze and the source file

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'gold' AS layer, bet_id, status, stake, CAST(NULL AS STRING) AS source_file, batch_id FROM workspace.betting.fact_bet WHERE bet_id = 'B-000001'
# MAGIC UNION ALL
# MAGIC SELECT 'silver', bet_id, status, stake, source_file, batch_id FROM workspace.betting.silver_bets WHERE bet_id = 'B-000001'
# MAGIC UNION ALL
# MAGIC SELECT 'bronze', bet_id, status, CAST(stake AS DECIMAL(10,2)), source_file, batch_id FROM workspace.betting.bronze_bets WHERE bet_id = 'B-000001'

# COMMAND ----------
# MAGIC %md ## 8. The partner KPI table (what Power BI would show)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM workspace.betting.gold_partner_daily_kpis ORDER BY bet_date, partner_id
