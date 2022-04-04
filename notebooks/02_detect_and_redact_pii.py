# Databricks notebook source
INPUT_PATH = spark.conf.get("INPUT_PATH")
INPUT_FORMAT = spark.conf.get("INPUT_FORMAT")
TABLE_PATH = spark.conf.get("TABLE_PATH")
EXPECTATIONS_PATH = spark.conf.get("EXPECTATIONS_PATH")
NUM_SAMPLE_ROWS = int(spark.conf.get("NUM_SAMPLE_ROWS"))

# COMMAND ----------

def get_spark_read(input_format, input_path):
  
  if input_format == "delta":
    return spark.read(INPUT_PATH)
  elif input_format == "parquet":
    return spark.read.parquet(INPUT_PATH)
  elif input_format == "json":
    return spark.read.json(INPUT_PATH)
  elif input_format == "csv":
    return spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

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

columns = get_spark_read(INPUT_FORMAT, INPUT_PATH).columns
expectations_and_actions = get_expectations_and_actions(columns, EXPECTATIONS_PATH)

# When DLT fully supports Repos we'll be able to use this... 
#f"file:{os.path.dirname(os.getcwd())}/expectations/pii_detection.csv"

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def get_failed_expectations(expectations):

  return [name for name, success in zip(constraints, expectations) if not success]

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract, col
import pyspark.sql.functions as F

constraints = dict(zip(expectations_and_actions.expectation, expectations_and_actions.constraint))

def get_select_expr(columns):

  # Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy...
  pdf = get_spark_read(INPUT_FORMAT, INPUT_PATH).limit(NUM_SAMPLE_ROWS).withColumn("failed_expectations", F.array([F.expr(value) for key, value in constraints.items()])).withColumn("failed_expectations", get_failed_expectations("failed_expectations")).filter(F.size("failed_expectations") > 0).select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"]).merge(expectations_and_actions, on="expectation")
  
  pii_detected = False
  
  if len(pdf) > 0:
    pii_detected = True
    
  # Todo - can this all be done with list comprehension? That would be more performant...  
  sql = [x for x in columns if x not in pdf["failed_column"].tolist()]
  
  def generate_sql(row):
    if row["mode"] in ["REDACT", "REDACT_AND_TAG"]:
      sql.append(row["action"])
    elif row["mode"] == "TAG":
      sql.append(row["failed_column"])  

  pdf.apply(generate_sql, axis=1)
    
  return sql, pii_detected

# COMMAND ----------

select_expr, pii_detected = get_select_expr(columns)

# COMMAND ----------

import dlt

@dlt.view(
  name="staging",
  comment="Raw data that has not been scanned for PII"
)
def staging():
  return (
    get_spark_read(INPUT_FORMAT, INPUT_PATH)
  )

# COMMAND ----------

@dlt.table(
  name="clean",
  comment="Clean data that has been scanned without finding any PII",
  path=f"{TABLE_PATH}/clean/",
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
  path=f"{TABLE_PATH}/redacted/",
  table_properties={"pii_scanned" : "True", "pii_found": str(pii_detected)}
)
def redacted(select_expr = select_expr):
  
  return dlt.read("quarantine").selectExpr(select_expr + ["failed_expectations"])

# COMMAND ----------

@dlt.table(
  name="clean_processed",
  comment="Data that has been scanned without any PII being found or where PII has been found and redacted based on a set of predefined rules",
  path=f"{TABLE_PATH}/clean_processed/",
  table_properties={"pii_scanned" : "True", "pii_found": str(pii_detected)}
)
def clean_processed():
  
  return dlt.read("redacted").drop("failed_expectations").unionByName(spark.table("LIVE.clean"))
