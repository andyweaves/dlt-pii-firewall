# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")

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

def get_sql(actions, columns):
  
  failed_expectations = [row['expectation'] for row in spark.read.format("delta").load(f"{table_path}/metrics/").select("expectation").distinct().where("failed >= 1").collect()]
  return list({k: actions[k] for k in failed_expectations}.values()) #+ columns

# COMMAND ----------

#def get_sql(actions, columns):
#  
#  failed_expectations = [row['expectation'] for row in spark.read.format("delta").load(f"{table_path}/failed_expectations/").select("expectation").collect()]
#  return list({k: actions[k] for k in failed_expectations}.values()) 

# COMMAND ----------

redact_sql = get_sql(actions, columns)

# COMMAND ----------

import dlt

@dlt.table(
  #path=f"{table_path}/clean_processed/"
)
def clean_processed():
  return spark.table("aweaver_dlt.quarantine").selectExpr(redact_sql)#read_stream("quarantine").selectExpr("*")
