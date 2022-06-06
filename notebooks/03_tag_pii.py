# Databricks notebook source
dbutils.widgets.text("DATABASE_NAME", "dlt_pii")
dbutils.widgets.multiselect("TABLE_NAMES", defaultValue="redacted", choices=["clean", "output", "redacted"])
dbutils.widgets.text("EXPECTATIONS_PATH", "/Workspace/Repos/andrew.weaver@databricks.com/dlt-pii-firewall/expectations/dynamic_firewall_rules.json")

DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
TABLE_NAMES = dbutils.widgets.get("TABLE_NAMES").split(",")
EXPECTATIONS_PATH = dbutils.widgets.get("EXPECTATIONS_PATH")

# COMMAND ----------

import pandas as pd
import json

def get_expectations_and_actions(columns, expectations_path):

  expectations_and_actions = pd.DataFrame(columns=["expectation", "constraint", "mode", "action", "tag"])

  with open(expectations_path, 'r') as f:
    raw_rules = json.load(f)["expectations"]

  for column in columns:
    for rule in raw_rules:
      expectations_and_actions = expectations_and_actions.append({"expectation": str(rule.get("name")).replace("{}", f"`{column}`"), "constraint": rule["constraint"].replace("{}", f"`{column}`"), "mode": rule["mode"], "action": str(rule.get("action")).replace("{}", f"`{column}`"), "tag": str(rule.get("tag")).replace("{}", f"`{column}`")}, ignore_index=True)
      
  return expectations_and_actions

# COMMAND ----------

columns = spark.table(f"{DATABASE_NAME}.redacted").columns
expectations_and_actions = get_expectations_and_actions(columns, EXPECTATIONS_PATH)

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract, col

# Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy
failed_expectations = spark.table(f"{DATABASE_NAME}.redacted").select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"]).merge(expectations_and_actions, on="expectation")

# COMMAND ----------

import re

if len(failed_expectations) > 0:
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_scanned' = 'True')")
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_found' = 'True')")
  for table in TABLE_NAMES:
    for index, failed_expectation in failed_expectations.iterrows():
      if failed_expectation["mode"] in ["TAG", "REDACT_AND_TAG"]:
        print(f"Adding comment '{failed_expectation['tag']}' to column {failed_expectation['failed_column']} in table {DATABASE_NAME}.{table} because mode is {failed_expectation['mode']}")
        spark.sql(f"ALTER TABLE {DATABASE_NAME}.{table} CHANGE {failed_expectation['failed_column']} COMMENT '{failed_expectation['tag']}'")
else: 
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_scanned' = 'True')")
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_found' = 'False')")
