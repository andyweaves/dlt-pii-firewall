# Databricks notebook source
dbutils.widgets.text("STORAGE_DIR", "dbfs:/dlt_pii/customer_pipeline")

STORAGE_DIR = dbutils.widgets.get("STORAGE_DIR")

# COMMAND ----------

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
rules = get_expectations(columns, expectations_path, 'constraint')

# When DLT supports repos we'll be able to use this and it'll be much easier... For now the expectations are in https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#files/2035247281457633
#f"file:{os.path.dirname(os.getcwd())}/expectations/pii_detection.csv"

# COMMAND ----------

import dlt

@dlt.view(
  name="staging",
  comment="Raw data that may contain PII"
)
def staging():
  return (
    spark.readStream.format("cloudFiles") 
    .option("cloudFiles.format", "parquet") 
    .schema(schema) 
    .load(input_path)
  )

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(rules, expectations) if not success]

# COMMAND ----------

@dlt.table(
  comment="Clean data that has been scanned and determined not to contain PII",
  path=f"{table_path}/clean/",
  table_properties={"may_contain_pii" : "False"}
)
@dlt.expect_all_or_drop(rules) 
def clean():
  return dlt.read_stream("staging")

# COMMAND ----------

import pyspark.sql.functions as F

@dlt.table(
 comment="Data that has been scanned and quarantined for potentially containing PII",
 path=f"{table_path}/quarantine/",
 table_properties={"may_contain_pii" : "True"}
)
def quarantine():
  return (
      dlt
        .read_stream("staging")
        .withColumn("failed_expectations", F.array([F.expr(value) for key, value in rules.items()]))
        .withColumn("failed_expectations", failed_expectations("failed_expectations"))
        .filter(F.size("failed_expectations") > 0)
  )

# COMMAND ----------

import pandas as pd
import json
from pyspark.sql.types import *

event_schema = StructType([
  StructField('timestamp', TimestampType(), True),
  StructField('step', StringType(), True),
  StructField('expectation', StringType(), True),
  StructField('passed', IntegerType(), True),
  StructField('failed', IntegerType(), True),
])
 
def events_to_dataframe(df):
  d = []
  group_key = df['timestamp'].iloc[0]
  for i, r in df.iterrows():
    json_obj = json.loads(r['details'])
    try:
      expectations = json_obj['flow_progress']['data_quality']['expectations']
      for expectation in expectations:
        d.append([group_key, expectation['dataset'], expectation['name'], expectation['passed_records'], expectation['failed_records']])
    except:
      pass
  return pd.DataFrame(d, columns=['timestamp', 'step', 'expectation', 'passed', 'failed'])

# COMMAND ----------

@dlt.table(
 path=f"{table_path}/metrics/",
 table_properties={"may_contain_pii" : "False"}
)
def metrics():
    return (
    spark
      .readStream
      .format("delta")
      .load(f"{STORAGE_DIR}/system/events")
      .filter(F.col("event_type") == "flow_progress")
      .groupBy("timestamp").applyInPandas(events_to_dataframe, schema=event_schema)
      .select("timestamp", "step", "expectation", "passed", "failed"))
