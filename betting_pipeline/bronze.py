# Bronze: copy the landing files into Delta tables, exactly as delivered (all columns as text)
from pyspark.sql import functions as F

from betting_pipeline.common import overwrite_batch

FILES = ["partners", "customers", "events", "bets"]


def load_file(spark, landing, schema, batch_id, name):
    path = landing + "/" + batch_id + "/" + name + ".csv"
    # Read every column as text. Bronze never fixes or changes data.
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    # Load metadata: when, from which file, which batch (this is our lineage back to the source)
    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn("source_file", F.lit(path))
    df = df.withColumn("batch_id", F.lit(batch_id))
    table_name = schema + ".bronze_" + name
    overwrite_batch(spark, df, table_name, batch_id)
    rows = spark.table(table_name).filter(F.col("batch_id") == batch_id).count()
    return rows


def load_batch(spark, landing, schema, batch_id):
    """Load all four files of one batch. Returns the number of rows per file."""
    counts = {}
    for name in FILES:
        counts[name] = load_file(spark, landing, schema, batch_id, name)
    return counts
