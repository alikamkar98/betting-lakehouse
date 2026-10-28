# Databricks notebook source
# Block 0 smoke test: is Spark + Delta working in my workspace?

# COMMAND ----------
# Create the schema (a folder for our tables) and the volume (a folder for incoming files)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.betting")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.betting.landing")

# COMMAND ----------
# Make 3 tiny rows in memory
rows = [
    (1, "Alice", 10.50),
    (2, "Bob", 25.00),
    (3, "Carla", 5.00),
]
df = spark.createDataFrame(rows, ["bet_id", "customer", "stake"])

# COMMAND ----------
# Write them as a Delta table
df.write.format("delta").mode("overwrite").saveAsTable("workspace.betting.smoke_test")

# COMMAND ----------
# Read the table back
result = spark.table("workspace.betting.smoke_test")
display(result)

# COMMAND ----------
# Every change to a Delta table is saved as a version. Here we see version 0
display(spark.sql("DESCRIBE HISTORY workspace.betting.smoke_test"))

# COMMAND ----------
# Write one small text file into the landing volume (like dropping a file into ADLS Gen2)
dbutils.fs.put("/Volumes/workspace/betting/landing/hello.txt", "hello lakehouse", True)
display(dbutils.fs.ls("/Volumes/workspace/betting/landing/"))
