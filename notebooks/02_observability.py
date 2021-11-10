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
