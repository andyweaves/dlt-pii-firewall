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
from pyspark.sql.types import StructType

def new_row(rule, column_name): 
  
  return {"expectation": str(rule.get("name")).replace("{}", f"`{column_name}`"), "constraint": rule["constraint"].replace("{}",  f"`{column_name}`"), "mode": rule["mode"], "action": str(rule.get("action")).replace("{}", f"`{column_name}`"), "tag": str(rule.get("tag")).replace("{}", f"`{column_name}`")}

def get_expectations_and_actions(schema, expectations_path):

  expectations_and_actions = [] 

  with open(expectations_path, 'r') as f:
    raw_rules = json.load(f)["expectations"]
    
  nested_columns = set(())
    
  for rule in raw_rules:
    for col in schema:
      if isinstance(col.dataType, StructType):
        for nested in col.dataType:
          row = new_row(rule, f"{nested.name}")
          nested_columns.add(col.name)
          expectations_and_actions.append(row)
      else:
        row = new_row(rule, f"{col.name}")
        expectations_and_actions.append(row)
  
  return pd.DataFrame(expectations_and_actions, columns=["expectation", "constraint", "mode", "action", "tag"]), nested_columns

# COMMAND ----------

schema = spark.table(f"{DATABASE_NAME}.redacted").schema
expectations_and_actions, nested_columns = get_expectations_and_actions(schema, EXPECTATIONS_PATH)

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
