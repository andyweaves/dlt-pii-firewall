# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")
expectations_path = spark.conf.get("expectations_path")

# COMMAND ----------

import json
from pyspark.sql.functions import col

def get_expectations(columns, expectations_file, key):
  
  results = {}
  
  with open(expectations_file, 'r') as f:
    raw_rules = json.load(f)["expectations"]
  for col in columns:
    for rule in raw_rules:
      results[rule["name"].replace("{}", f"`{col}`")] = rule[key].replace("{}", f"`{col}`")
  return results

# COMMAND ----------

columns = spark.read.parquet(input_path).columns
schema = spark.read.parquet(input_path).schema
constraints = get_expectations(columns, expectations_path, 'constraint')
actions = get_expectations(columns, expectations_path, 'action')

# When DLT fully supports Repos we'll be able to use this and it'll be much easier... 
#f"file:{os.path.dirname(os.getcwd())}/expectations/pii_detection.csv"

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def get_failed_expectations(expectations):

  return [name for name, success in zip(constraints, expectations) if not success]

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract
import pyspark.sql.functions as F

def get_select_expr(actions, columns):

  # Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy...
  pdf = spark.read.parquet(input_path).withColumn("failed_expectations", F.array([F.expr(value) for key, value in constraints.items()])).withColumn("failed_expectations", get_failed_expectations("failed_expectations")).filter(F.size("failed_expectations") > 0).select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"])
  
  failed_columns = pdf["failed_column"].tolist()
  failed_expectations = pdf["expectation"].tolist()
  
  pii_detected = False
  
  if len(failed_expectations) > 0:
    pii_detected = True
    
  return [x for x in columns if x not in failed_columns] + list({k: actions[k] for k in failed_expectations}.values()), pii_detected

# COMMAND ----------

select_expr, pii_detected = get_select_expr(actions, columns)

# COMMAND ----------

import dlt

@dlt.view(
  name="staging",
  comment="Raw data that has not been scanned for PII"
)
def staging():
  return (
    spark.read.parquet(input_path)
  )

# COMMAND ----------

@dlt.table(
  name="clean",
  comment="Clean data that has been scanned without finding any PII",
  path=f"{table_path}/clean/",
  table_properties={"pii_scanned" : "True", "pii_found": "False"}
)
@dlt.expect_all_or_drop(constraints) 
def clean():
  return dlt.read("staging")

# COMMAND ----------

@dlt.view(
 name="quarantine",
 comment="Data that has been scanned and quarantined for potentially containing PII"
)
def quarantine():
  
  return (
      dlt
        .read("staging")
        .withColumn("failed_expectations", F.array([F.expr(value) for key, value in constraints.items()]))
        .withColumn("failed_expectations", get_failed_expectations("failed_expectations"))
        .filter(F.size("failed_expectations") > 0)
  )

# COMMAND ----------

@dlt.table(
  name="redacted",
  comment="Data in which PII has been found and redacted based on a set of predefined rules",
  path=f"{table_path}/redacted/",
  table_properties={"pii_scanned" : "True", "pii_found": str(pii_detected), "pii_action": "REDACTED"}
)
def redacted(select_expr = select_expr):
  
  return dlt.read("quarantine").selectExpr(select_expr + ["failed_expectations"])

# COMMAND ----------

@dlt.table(
  name="clean_processed",
  comment="Data that has been scanned without any PII being found or where PII has been found and redacted based on a set of predefined rules",
  path=f"{table_path}/clean_processed/",
  table_properties={"pii_scanned" : "True", "pii_found": str(pii_detected), "pii_action": "REDACTED"}
)
def clean_processed():
  
  return dlt.read("redacted").drop("failed_expectations").unionByName(spark.table("LIVE.clean"))
