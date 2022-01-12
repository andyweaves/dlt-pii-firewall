# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")

# COMMAND ----------

#input_path = "dbfs:/aweaver/customer_raw"
#table_path = "dbfs:/aweaver/delta/customers"

# COMMAND ----------

#from pyspark.sql.functions import array, lit

#df = spark.read.parquet(input_path)
#empty = df.withColumn("failed_expectations", array(lit("failed"), lit("expectations"))).where("customer_id IS NULL")
#empty.createTable(f"{target_db}.quarantine")

# COMMAND ----------

# Write a CSV file of expectation name, constraint and action to take if the expectation fails. When DLT fully supports repos we'll load this from the file that's checked in to source control...

dbutils.fs.put("/FileStore/andrew.weaver@databricks.com/dlt/customers/expectations/pii_identification.csv", """
name,constraint,action
{} may contain creditcard,CAST({} AS STRING) NOT REGEXP("^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})$") AS result,"concat('XXXXXXXXXXXXXXXX', substr({}, -3, 3)) AS {}"
{} may contain ssn,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}-\\\\d{2}-\\\\d{4}$") AS result,"sha2({}, 512) AS {}"
{} may contain expiry date,CAST({} AS STRING) NOT REGEXP("^\\\\d{2}/\\\\d{2}$") AS result,"regexp_replace({}, '^(0[1-9]|1[0-2])', 'XX') AS {}"
{} may contain security code,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}$") AS result,"'XXX' AS {}"
{} may contain email address,CAST({} AS STRING) NOT REGEXP("\\\\w@\\\\w.\\\\w") AS result,"regexp_extract({}, '^.*@(.*)$', 1) AS {}"
{} may contain ipv4,CAST({} AS STRING) NOT REGEXP("^((?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])[.]){3}(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$") AS result,"'[REDACTED]' AS {}"
""", overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Todo... get these to work too...
# MAGIC --SELECT CAST(address AS STRING) NOT REGEXP('^(.+)[,\\\\s]+(.+?)\\\s*(\\\d{5})?$') AS result
# MAGIC --SELECT CAST(phone_number AS STRING) NOT REGEXP("^(\\(?\\d\\d\\d\\)?)(|-|\\.)?\\d\\d\\d( |-|\\.)?\\d{4,4}(( |-|\\.)?[ext\\.]+ ?\\d+)?$") AS result 
# MAGIC --SELECT CAST(iban AS STRING) NOT REGEXP("[a-zA-Z]{2}[0-9]{2}[a-zA-Z0-9]{4}[0-9]{7}([a-zA-Z0-9]?){0,16}") AS result 

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
      .read
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
      #actions.append((row["name"].replace("{}", f"`{col}`"), row["action"].replace("{}", f"`{col}`")))
      actions[row["name"].replace("{}", f"`{col}`")] = row["action"].replace("{}", f"`{col}`")
  return rules, actions

# COMMAND ----------

columns = spark.read.parquet(input_path).columns
schema = spark.read.parquet(input_path).schema
rules, actions = get_rules_and_actions(columns, "file:/dbfs/FileStore/andrew.weaver@databricks.com/dlt/customers/expectations/pii_identification.csv") 
#f"file:{os.path.dirname(os.getcwd())}/expectations/pii_identification.csv"

# COMMAND ----------

#schema_with_failed = schema.add(StructField('failed_expectations', ArrayType(StringType(), True)))
#spark.catalog.createTable(f"{target_db}.quarantine", schema=schema_with_failed)

# COMMAND ----------

@dlt.view(
comment="Raw data that may potentially contain PII"
)
def staging():
  return (
    spark
      .read
      .format("parquet")
      .schema(schema)
      .load(input_path)
  )

# COMMAND ----------

@dlt.table(
  comment="Data that has been processed and successfully evaluated against our Expectations",
  path=f"{table_path}/clean/"
)
@dlt.expect_all_or_drop(rules) 
def clean():
  return dlt.read("staging")

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(rules, expectations) if not success]

# COMMAND ----------

import pyspark.sql.functions as F

@dlt.table(
 comment="Data that has been quarantined for potentially containing PII"
)
def quarantine():
  return (
      dlt
        .read("staging")
        .withColumn("failed_expectations", F.array([F.expr(value) for key, value in rules.items()]))
        .withColumn("failed_expectations", failed_expectations("failed_expectations"))
        .filter(F.size("failed_expectations") > 0)
  )

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

def get_dlt_sql(actions, columns):
  
  expectation_results = spark.read.format("delta").load(f"{table_path}/metrics/").select("expectation").distinct().where("failed >= 1").withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).collect()
  
  failed_expectations = [row['expectation'] for row in expectation_results]
  failed_columns = [row['failed_column'] for row in expectation_results]
  return [x for x in columns if x not in failed_columns] + list({k: actions[k] for k in failed_expectations}.values()) 

# COMMAND ----------

from pyspark.sql.functions import explode

def get_dlt_sql_2(actions, columns):

  expectation_results = spark.table("aweaver_dlt.quarantine").select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).collect()

  failed_expectations = [row['expectation'] for row in expectation_results]
  failed_columns = [row['failed_column'] for row in expectation_results]
  
  return [x for x in columns if x not in failed_columns] + list({k: actions[k] for k in failed_expectations}.values()) 

# COMMAND ----------

# Commenting this last step out for now to try the multi-task jobs approach!

#@dlt.table(
#  path=f"{table_path}/clean_processed/"
#)
#def clean_processed():
#  
#  sql = get_dlt_sql_2(actions, columns)
#  
#  print(f"Dynamic SQL: {dlt_sql}")
#  
#  return dlt.read("quarantine").selectExpr(sql)
