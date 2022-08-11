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

from pyspark.sql.types import StructType, MapType, ArrayType
from pyspark.sql.functions import col, explode, map_keys, size

def flatten_dataframe(df):
  
  for field in df.schema.fields:
  
    if isinstance(field.dataType, StructType):
      for nested_field in field.dataType:
        df = df.withColumn(f"{field.name}->{nested_field.name}", col(f"{field.name}.{nested_field.name}"))
      df = df.drop(col(field.name))
    
    elif isinstance(field.dataType, MapType):
      keys = list(map(lambda row: row[0], df.select(explode(map_keys(col("pii_map")))).distinct().collect()))
      for k in keys:
        df = df.withColumn(f"{field.name}->{k}", col(f"{field.name}.{k}"))
      df = df.drop(col(field.name))
      
    elif isinstance(field.dataType, ArrayType):
      i = 0
      while i <= df.select(size(col(field.name))).head()[0] - 1:
        df = df.withColumn(f"{field.name}->{i}", col(f"{field.name}")[i])
        i += 1
      df = df.drop(col(field.name))
  
  return df

# COMMAND ----------

import pandas as pd
import json

def new_row(rule, column_name): 
  
  return {"expectation": str(rule.get("name")).replace("{}", f"`{column_name}`"), "constraint": rule["constraint"].replace("{}",  f"`{column_name}`"), "mode": rule["mode"], "action": str(rule.get("action")).replace("{}", f"`{column_name}`"), "tag": str(rule.get("tag")).replace("{}", f"`{column_name}`")}

def get_expectations_and_actions(schema, expectations_path):
  
  expectations_and_actions = [] 

  with open(expectations_path, 'r') as f:
    raw_rules = json.load(f)["expectations"]
    
  for rule in raw_rules:
    for col in schema:
      row = new_row(rule, f"{col.name}")
      expectations_and_actions.append(row)
  
  return pd.DataFrame(expectations_and_actions, columns=["expectation", "constraint", "mode", "action", "tag"])

# COMMAND ----------

schema = flatten_dataframe(get_spark_read(INPUT_FORMAT, INPUT_PATH)).schema
expectations_and_actions = get_expectations_and_actions(schema, EXPECTATIONS_PATH)

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def get_failed_expectations(expectations):

  return [name for name, success in zip(constraints, expectations) if not success]

# COMMAND ----------

from pyspark.sql.functions import array, expr, regexp_extract

constraints = dict(zip(expectations_and_actions.expectation, expectations_and_actions.constraint))

def get_sql_expressions(columns):
    
    df = flatten_dataframe(get_spark_read(INPUT_FORMAT, INPUT_PATH).limit(NUM_SAMPLE_ROWS))

    # Drop duplicates because otherwise we'll need to handle duplicate columns in the downstream tables, which will get messy...
    pdf = df.withColumn("failed_expectations", array([expr(value) for key, value in constraints.items()])).withColumn("failed_expectations", get_failed_expectations("failed_expectations")).filter(size("failed_expectations") > 0).select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).toPandas().drop_duplicates(subset = ["failed_column"]).merge(expectations_and_actions, on="expectation")
    
    pii_detected = False

    if len(pdf) > 0:
        pii_detected = True

    # Todo - change to list comprehension. It's more performant...
    redact_sql = [col for col in columns if col not in pdf["failed_column"].tolist()]

    def generate_sql(row):
      if row["mode"] in ["REDACT", "REDACT_AND_TAG"]:
          redact_sql.append(row["action"])
      elif row["mode"] == "TAG":
          redact_sql.append(row["failed_column"])  

    pdf.apply(generate_sql, axis=1)

    return redact_sql, pii_detected

# COMMAND ----------

redact_sql, pii_detected = get_sql_expressions(schema.fieldNames())

# COMMAND ----------

import dlt

@dlt.view(
  name="staging",
  comment="Raw data that has not been scanned for PII"
)
def staging():
  
    return (
      flatten_dataframe(get_spark_read(INPUT_FORMAT, INPUT_PATH))
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
        .withColumn("failed_expectations", array([expr(value) for key, value in constraints.items()]))
        .withColumn("failed_expectations", get_failed_expectations("failed_expectations"))
        .filter(size("failed_expectations") > 0)
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
