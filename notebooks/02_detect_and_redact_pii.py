# Databricks notebook source
INPUT_PATH = spark.conf.get("INPUT_PATH")
INPUT_FORMAT = spark.conf.get("INPUT_FORMAT")
TABLE_PATH = spark.conf.get("TABLE_PATH")
EXPECTATIONS_PATH = spark.conf.get("EXPECTATIONS_PATH")
NUM_SAMPLE_ROWS = int(spark.conf.get("NUM_SAMPLE_ROWS"))
NESTED_DEPTH = int(spark.conf.get("NESTED_DEPTH"))

# COMMAND ----------

def get_spark_read(input_format, input_path):
  
  if input_format == "csv":
    return spark.read.format(input_format).load(input_path, header=True, inferSchema=True)
  else: 
    return spark.read.format(input_format).load(input_path)

# COMMAND ----------

from pyspark.sql.types import StructType, MapType, ArrayType
from pyspark.sql.functions import col, explode, map_keys, size

def flatten_dataframe(df, num_iterations):
  
  for n in range(1, num_iterations + 1, 1):
    
    nested = False
    
    for field in df.schema.fields:

      if isinstance(field.dataType, StructType):
        nested = True
        for nested_field in field.dataType:
          df = df.withColumn(f"L{n}->{field.name}->{nested_field.name}", col(f"{field.name}.{nested_field.name}"))
        df = df.drop(col(field.name))

      elif isinstance(field.dataType, MapType):
        nested = True
        keys = list(map(lambda row: row[0], df.select(explode(map_keys(col("pii_map")))).distinct().collect()))
        for k in keys:
          df = df.withColumn(f"L{n}->{field.name}->{k}", col(f"{field.name}.{k}"))
        df = df.drop(col(field.name))

      elif isinstance(field.dataType, ArrayType):
        nested = True
        i = 0
        while i <= df.select(size(col(field.name))).head()[0] - 1:
          df = df.withColumn(f"L{n}->{field.name}->{i}", col(f"{field.name}")[i])
          i += 1
        df = df.drop(col(field.name))
        
    if nested == False:
      break
  
  return df

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

schema = flatten_dataframe(get_spark_read(INPUT_FORMAT, INPUT_PATH), NESTED_DEPTH).schema
expectations_and_actions = get_expectations_and_actions(schema, EXPECTATIONS_PATH)

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def get_failed_expectations(expectations):

  return [name for name, success in zip(constraints, expectations) if not success]

# COMMAND ----------

import pyspark.pandas as ps
from pyspark.sql.functions import array, expr, regexp_extract, lit, desc

constraints = dict(zip(expectations_and_actions.expectation, expectations_and_actions.constraint))

def get_sql_expressions(columns):
    
    df = flatten_dataframe(get_spark_read(INPUT_FORMAT, INPUT_PATH).limit(NUM_SAMPLE_ROWS), NESTED_DEPTH)
   
    # Do the heavy lifting in pyspark and then convert to pandas once we've dropped duplicates to make the following steps run faster
    pdf = (df.withColumn("failed_expectations", array([expr(value) for key, value in constraints.items()]))
       .withColumn("failed_expectations", get_failed_expectations("failed_expectations"))
       .filter(size("failed_expectations") > 0)
       .select(explode("failed_expectations").alias("expectation"))
       .withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1))
       .groupBy("expectation", "failed_column").count()
       .orderBy(desc("count"))
       .withColumn("sample_rows", lit(NUM_SAMPLE_ROWS))
       .withColumn("percent_failed", col("count") / col("sample_rows") * 100)
       .toPandas()
       .merge(expectations_and_actions, on="expectation")
       .query('percent_failed >= redact_threshold')
       .drop_duplicates(subset = ["failed_column"], keep="first"))

    pii_detected = False

    if len(pdf) > 0:
      pii_detected = True

    redact_sql = [f"`{col}`" for col in columns if col not in pdf["failed_column"].tolist()]

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
      flatten_dataframe(get_spark_read(INPUT_FORMAT, INPUT_PATH), NESTED_DEPTH)
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
