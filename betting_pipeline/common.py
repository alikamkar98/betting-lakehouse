# Small helpers used by several layers


def overwrite_batch(spark, df, table_name, batch_id):
    """Write one batch into a Delta table. Loading the same batch again replaces it (no duplicates)."""
    if not spark.catalog.tableExists(table_name):
        # First load: create the table
        df.write.format("delta").saveAsTable(table_name)
        return
    # Later loads: replace only the rows of this batch, keep all other batches
    condition = "batch_id = '" + batch_id + "'"
    df.write.format("delta").mode("overwrite").option("replaceWhere", condition).saveAsTable(table_name)


def merge_upsert(spark, df, table_name, key):
    """Simple upsert (SCD Type 1): update the row if the key exists, insert it if not."""
    if not spark.catalog.tableExists(table_name):
        # Create an empty table with the right columns, then MERGE fills it
        df.limit(0).write.format("delta").saveAsTable(table_name)
    df.createOrReplaceTempView("incoming_rows")
    result = spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING incoming_rows AS source
        ON target.{key} = source.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return result.first().asDict()
