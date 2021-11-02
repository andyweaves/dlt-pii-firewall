# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")

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

import dlt
import pyspark.sql.functions as F

@dlt.table(
 path=f"{table_path}/metrics/"
)
def metrics():
    return (
    spark
      .readStream
      .format("delta")
      .load("/FileStore/andrew.weaver@databricks.com/dlt/customers/system/events")
      .filter(F.col("event_type") == "flow_progress")
      .groupBy("timestamp").applyInPandas(events_to_dataframe, schema=event_schema)
      .select("timestamp", "step", "expectation", "passed", "failed"))

# COMMAND ----------

import os
from pyspark.sql.functions import col

def get_rules_and_actions(columns, expectations_file):
  """
    loads data quality rules from csv file
    :param columns: 
    :param expectations_file: 
    :return: dictionary of rules 
  """
  rules, actions = {}, {}
  raw_rules = spark.read.csv(expectations_file, header=True, inferSchema=True).collect()
  for col in columns:
    for row in raw_rules:
      rules[row["name"].replace("{}", f"`{col}`")] = row["constraint"].replace("{}", f"`{col}`")
      actions[row["name"].replace("{}", f"`{col}`")] = row["action"].replace("{}", f"`{col}`")
  return rules, actions

# COMMAND ----------

columns = spark.read.parquet(input_path).columns
schema = spark.read.parquet(input_path).schema
rules, actions = get_rules_and_actions(columns, "file:/dbfs/FileStore/andrew.weaver@databricks.com/dlt/customers/expectations/pii_identification.csv") #f"file:{os.path.dirname(os.getcwd())}/expectations/pii_identification.csv"

# COMMAND ----------

def get_dlt_sql(actions, columns):
  
  failed_expectations = [row['expectation'] for row in spark.read.format("delta").load(f"{table_path}/metrics/").select("expectation").distinct().where("failed >= 1").collect()]
  return list({k: actions[k] for k in failed_expectations}.values()) + columns

# COMMAND ----------

#dlt_sql = get_dlt_sql(actions, columns) 
#dlt_sql

# COMMAND ----------

#import pickle

#file = open("/dbfs/FileStore/andrew.weaver@databricks.com/dlt/customers/dlt_sql", "wb")
#pickle.dump(dlt_sql, file)
#file.close()
