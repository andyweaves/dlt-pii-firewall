# Databricks notebook source
INPUT_PATH = spark.conf.get("INPUT_PATH")
INPUT_FORMAT = spark.conf.get("INPUT_FORMAT")
TABLE_PATH = spark.conf.get("TABLE_PATH")
EXPECTATIONS_PATH = spark.conf.get("EXPECTATIONS_PATH")
NUM_SAMPLE_ROWS = int(spark.conf.get("NUM_SAMPLE_ROWS"))

# COMMAND ----------

def get_spark_read(input_format, input_path):
  
  if input_format == "csv":
    return spark.read.format(input_format).load(input_path, header=True, inferSchema=True)
  else: 
    return spark.read.format(input_format).load(input_path)

# COMMAND ----------

import pandas as pd
import json
from pyspark.sql.types import StructType, MapType, ArrayType
from pyspark.sql.functions import explode

def new_row(rule, column_name): 
  
  return {"expectation": str(rule.get("name")).replace("{}", f"`{column_name}`"), "constraint": rule["constraint"].replace("{}",  f"`{column_name}`"), "mode": rule["mode"], "action": str(rule.get("action")).replace("{}", f"`{column_name}`"), "tag": str(rule.get("tag")).replace("{}", f"`{column_name}`")}

def get_expectations_and_actions(df, columns, expectations_path):
  
  schema = df.schema
  expectations_and_actions = [] 

  with open(expectations_path, 'r') as f:
    raw_rules = json.load(f)["expectations"]
    
  nested_columns = set(())
  columns = set(columns)
    
  for rule in raw_rules:
    for col in schema:
      if isinstance(col.dataType, StructType):
        for nested in col.dataType:
          row = new_row(rule, f"{nested.name}")
          nested_columns.add(col.name)
          expectations_and_actions.append(row)
      elif isinstance(col.dataType, MapType):
        keys = [row[0] for row in df.select(explode(col.name)).select("key").distinct().collect()]
        columns.discard(col.name)
        for key in keys:
          columns.add(f"{col.name}.{key}")
          row = new_row(rule, key)
          expectations_and_actions.append(row)    
      else:
        row = new_row(rule, f"{col.name}")
        expectations_and_actions.append(row)
  
  return pd.DataFrame(expectations_and_actions, columns=["expectation", "constraint", "mode", "action", "tag"]), list(columns), nested_columns

# COMMAND ----------

df = get_spark_read(INPUT_FORMAT, INPUT_PATH)
columns = df.schema.fieldNames()
expectations_and_actions, columns, nested_columns = get_expectations_and_actions(df, columns, EXPECTATIONS_PATH)

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def get_failed_expectations(expectations):

  return [name for name, success in zip(constraints, expectations) if not success]

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col
import pyspark.sql.functions as F

constraints = dict(zip(expectations_and_actions.expectation, expectations_and_actions.constraint))

def get_sql_expressions(columns, nested_columns):
    
    not_nested = [col for col in columns if col not in nested_columns]
    select_sql = not_nested + list({f"{col}.*" for col in nested_columns})

    df = get_spark_read(INPUT_FORMAT, INPUT_PATH).limit(NUM_SAMPLE_ROWS).selectExpr(select_sql).na.fill("")

    new_columns = df.columns

    # Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy...
    pdf = df.withColumn("failed_expectations", F.array([F.expr(value) for key, value in constraints.items()])).withColumn("failed_expectations", get_failed_expectations("failed_expectations")).filter(F.size("failed_expectations") > 0).select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"]).merge(expectations_and_actions, on="expectation")
    
    pii_detected = False

    if len(pdf) > 0:
        pii_detected = True

    # Todo - change to list comprehension. It's more performant...
    redact_sql = [col for col in new_columns if col not in pdf["failed_column"].tolist()]

    def generate_sql(row):
        if row["mode"] in ["REDACT", "REDACT_AND_TAG"]:
            redact_sql.append(row["action"])
        elif row["mode"] == "TAG":
            redact_sql.append(row["failed_column"])  

    pdf.apply(generate_sql, axis=1)

    return select_sql, redact_sql, pii_detected

# COMMAND ----------

select_sql, redact_sql, pii_detected = get_sql_expressions(columns, nested_columns)

# COMMAND ----------

import dlt

@dlt.view(
  name="staging",
  comment="Raw data that has not been scanned for PII"
)
def staging():
  
    return (
      get_spark_read(INPUT_FORMAT, INPUT_PATH).selectExpr(select_sql).na.fill("")
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
def redacted(redact_sql = redact_sql):
  
  return dlt.read("quarantine").selectExpr(redact_sql + ["failed_expectations"])

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

@dlt.table(
  name="output",
  comment="Data that has been scanned without any PII being found or where PII has been found and redacted based on a set of predefined rules",
  path=f"{TABLE_PATH}/output/",
  table_properties={"pii_scanned" : "True", "pii_found": str(pii_detected)}
)
def output():
  
  try:
    return dlt.read("redacted").drop("failed_expectations").unionByName(spark.table("LIVE.clean"))
  except AnalysisException as e:
    print(f"Caught exception due to mismatching schemas. Exception:\n{e}")
    return dlt.read("redacted").drop("failed_expectations")
