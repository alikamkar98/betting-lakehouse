# Databricks notebook source
# Runs every query in sql/analysis.sql and shows the result

# COMMAND ----------
with open("sql/analysis.sql") as file:
    text = file.read()

# Split the file into single statements at each ";"
statements = text.split(";")

for statement in statements:
    if statement.strip() == "":
        continue
    first_line = statement.strip().splitlines()[0]
    print(first_line)
    display(spark.sql(statement))
