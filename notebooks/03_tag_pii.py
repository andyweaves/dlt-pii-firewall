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

def new_row(rule, column_name): 
  
  return {"expectation": str(rule.get("name")).replace("{}", f"`{column_name}`"), "constraint": rule["constraint"].replace("{}",  f"`{column_name}`"), "mode": rule["mode"], "action": str(rule.get("action")).replace("{}", f"`{column_name}`"), "tag": str(rule.get("tag")).replace("{}", f"`{column_name}`"), "redact_threshold": rule.get("redact_threshold"), "tag_threshold": rule.get("tag_threshold")}

def get_expectations_and_actions(schema, expectations_path):
  
  expectations_and_actions = [] 

  with open(expectations_path, 'r') as f:
    raw_rules = json.load(f)["expectations"]
    
  for rule in raw_rules:
    for col in schema:
      row = new_row(rule, f"{col.name}")
      expectations_and_actions.append(row)
  
  return pd.DataFrame(expectations_and_actions, columns=["expectation", "constraint", "mode", "action", "tag", "redact_threshold", "tag_threshold"])

# COMMAND ----------

schema = spark.table(f"{DATABASE_NAME}.redacted").schema
expectations_and_actions = get_expectations_and_actions(schema, EXPECTATIONS_PATH)

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract, col

failed_expectations = (spark.table(f"{DATABASE_NAME}.redacted")
                       .select(explode("failed_expectations").alias("expectation")).distinct()
                       .withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1))
                       .toPandas().drop_duplicates(subset = ["failed_column"]).merge(expectations_and_actions, on="expectation"))

# COMMAND ----------

import re

if len(failed_expectations) > 0:
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_scanned' = 'True')")
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_found' = 'True')")
  for table in TABLE_NAMES:
    for index, failed_expectation in failed_expectations.iterrows():
      if failed_expectation["mode"] in ["TAG", "REDACT_AND_TAG"]:
        print(f"Adding comment '{failed_expectation['tag']}' to column {failed_expectation['failed_column']} in table {DATABASE_NAME}.{table} because mode is {failed_expectation['mode']}")
        spark.sql(f"ALTER TABLE {DATABASE_NAME}.{table} CHANGE `{failed_expectation['failed_column']}` COMMENT '{failed_expectation['tag']}'")
else: 
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_scanned' = 'True')")
  spark.sql(f"ALTER DATABASE {DATABASE_NAME} SET DBPROPERTIES ('pii_found' = 'False')")
