# Databricks notebook source
dbutils.widgets.text("DATABASE_NAME", "dlt_pii")
dbutils.widgets.multiselect("TABLE_NAMES", defaultValue="quarantine", choices=["quarantine", "redacted", "clean_processed"])

DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
TABLE_NAMES = dbutils.widgets.get("TABLE_NAMES").split(",")

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract, col

# Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy
pdf = spark.table(f"{DATABASE_NAME}.quarantine").select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"])["expectation"].tolist()

print(f"Failed Expectations: {failed_expectations}")

# COMMAND ----------

import re

if len(failed_expectations) > 0:
  spark.sql(f"COMMENT ON DATABASE {DATABASE_NAME} IS 'Warning! Tables in this database are suspected to contain PII'")
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('may_contain_pii' = True)")
  for table in TABLE_NAMES:
    for expectation in failed_expectations:
      col = re.search(r"`(.*?)`", expectation).group()
      comment = re.search(r"` (.*)", expectation).group(1)
      print(f"Adding comment '{comment}' to column {col} in table {DATABASE_NAME}.{table}")
      spark.sql(f"ALTER TABLE {DATABASE_NAME}.{table} CHANGE {col} COMMENT '{comment}'")
else: 
  spark.sql(f"COMMENT ON DATABASE {DATABASE_NAME} IS 'Tables in this database are not suspected to contain PII'")
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('may_contain_pii' = False)")
