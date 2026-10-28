# Run log: one row per step per run in ops_pipeline_runs. This is our monitoring table.
from datetime import datetime

COLUMNS = (
    "run_id STRING, batch_id STRING, step STRING, started_at TIMESTAMP, finished_at TIMESTAMP, "
    "rows_in BIGINT, rows_out BIGINT, rows_rejected BIGINT, status STRING, message STRING"
)


def log_step(spark, schema, run_id, batch_id, step, started_at, rows_in, rows_out, rows_rejected, status, message=""):
    finished_at = datetime.now()
    row = (run_id, batch_id, step, started_at, finished_at, rows_in, rows_out, rows_rejected, status, message)
    df = spark.createDataFrame([row], COLUMNS)
    df.write.format("delta").mode("append").saveAsTable(schema + ".ops_pipeline_runs")
