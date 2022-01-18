# Databricks notebook source
dbutils.widgets.text("DATABASE_NAME", "dlt_pii")
dbutils.widgets.text("TABLE_NAME", "clean_processed")
dbutils.widgets.text("TABLE_PATH", "dbfs:/dlt_pii/customer_delta")

DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
TABLE_NAME = dbutils.widgets.get("TABLE_NAME")
TABLE_PATH = dbutils.widgets.get("TABLE_PATH")

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract, col

# Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy
pdf = spark.read.format("delta").load(f"{TABLE_PATH}/quarantine/").select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"])
  
failed_columns = pdf["failed_column"].tolist()
failed_expectations = pdf["expectation"].tolist()

print(f"Failed Columns: {failed_columns}")
print(f"Failed Expectations: {failed_expectations}")
