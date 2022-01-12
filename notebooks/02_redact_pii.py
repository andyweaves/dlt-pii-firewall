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
      #actions.append((row["name"].replace("{}", f"`{col}`"), row["action"].replace("{}", f"`{col}`")))
      actions[row["name"].replace("{}", f"`{col}`")] = row["action"].replace("{}", f"`{col}`")
  return rules, actions

# COMMAND ----------

columns = spark.read.parquet(input_path).columns
schema = spark.read.parquet(input_path).schema
rules, actions = get_rules_and_actions(columns, "file:/dbfs/FileStore/andrew.weaver@databricks.com/dlt/customers/expectations/pii_identification.csv") 

# COMMAND ----------

from pyspark.sql.functions import explode, regexp_extract

def get_dlt_sql_2(actions, columns):

  expectation_results = spark.table("aweaver_dlt_2.quarantine").select(explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", regexp_extract(col("expectation"), "\`(.*?)\`", 1)).collect()

  failed_expectations = [row['expectation'] for row in expectation_results]
  failed_columns = [row['failed_column'] for row in expectation_results]
  
  return [x for x in columns if x not in failed_columns] + list({k: actions[k] for k in failed_expectations}.values()) 

# COMMAND ----------

import dlt

@dlt.table(
  path=f"{table_path}/clean_processed/"
)
def clean_processed():
  
  sql = get_dlt_sql_2(actions, columns)
  
  print(f"Dynamic SQL: {sql}")
  
  return spark.table("aweaver_dlt_2.quarantine").selectExpr(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE aweaver_dlt_2.quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM aweaver_dlt_2.clean_processed 
