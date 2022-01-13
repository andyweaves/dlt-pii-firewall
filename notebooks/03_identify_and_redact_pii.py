# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")

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
rules, actions = get_rules_and_actions(columns, "file:/dbfs/FileStore/andrew.weaver@databricks.com/dlt/customers/expectations/pii_identification.csv") 

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(rules, expectations) if not success]

# COMMAND ----------

import pyspark.sql.functions as F

def get_dlt_sql(actions, columns):

  expectation_results = spark.table("aweaver_dlt_3.quarantine").select(F.explode("failed_expectations").alias("expectation")).distinct().withColumn("failed_column", F.regexp_extract(col("expectation"), "\`(.*?)\`", 1)).collect()

  failed_expectations = [row['expectation'] for row in expectation_results]
  failed_columns = [row['failed_column'] for row in expectation_results]
  
  return [x for x in columns if x not in failed_columns] + list({k: actions[k] for k in failed_expectations}.values()) 

# COMMAND ----------

#@dlt.view(
#name=raw,
#comment="Raw data that may potentially contain PII"
#)
#def raw():
#  return (
#    spark
#      .read
#      .format("parquet")
#      .schema(schema)
#      .load(input_path)
#  )

# COMMAND ----------

#@dlt.table(
#  comment="Data that has been processed and successfully evaluated against our Expectations",
#  path=f"{table_path}/clean/"
#)
#@dlt.expect_all_or_drop(rules) 
#def clean():
#  return dlt.read("raw")

# COMMAND ----------

#import pyspark.sql.functions as F
#
#@dlt.view(
# comment="Data that has been quarantined for potentially containing PII"
#)
#def quarantine():
#  return (
#      dlt
#        .read("raw")
#        .withColumn("failed_expectations", F.array([F.expr(value) for key, value in rules.items()]))
#        .withColumn("failed_expectations", failed_expectations("failed_expectations"))
#        .filter(F.size("failed_expectations") > 0)
#  )

# COMMAND ----------

#@dlt.table(
# path=f"{table_path}/clean_processed/"
#)
#def clean_processed():
# 
# sql = get_dlt_sql(actions, columns)
# 
# print(f"Dynamic SQL: {dlt_sql}")
# 
# return dlt.read("quarantine").selectExpr(sql)

# COMMAND ----------

import dlt

def generate_table_or_view(name, comment, path=None):
  
  if name == "raw":
    @dlt.view(name=name, comment=comment)
    def raw():
      return (spark.read
      .format("parquet")
      .schema(schema)
      .load(input_path)
    )
    
  elif name == "clean":
    @dlt.table(name=name, comment=comment, path=path)
    @dlt.expect_all_or_drop(rules) 
    def clean():
      return dlt.read("raw")
    
  elif name == "quarantine":
    @dlt.table(name=name, comment=comment)
    def quarantine():
      return (dlt.read("raw")
        .withColumn("failed_expectations", F.array([F.expr(value) for key, value in rules.items()]))
        .withColumn("failed_expectations", failed_expectations("failed_expectations"))
        .filter(F.size("failed_expectations") > 0)
    )
    
  elif name == "clean_processed":
    @dlt.table(name=name, comment=comment, path=path)
    def clean_processed():
      sql = get_dlt_sql(actions, columns)
      print(f"Dynamic SQL: {dlt_sql}")
      return dlt.read("quarantine").selectExpr(sql)
    
  else:
    raise Exception(f"{name} is an invalid table name!") 

# COMMAND ----------

generate_table_or_view(name="raw", comment="Raw data that may potentially contain PII")
generate_table_or_view(name="clean", comment="Data that has been processed and successfully evaluated against our Expectations", path=f"{table_path}/clean/")
generate_table_or_view(name="quarantine", comment="Data that has been quarantined for potentially containing PII")
generate_table_or_view(name="clean_processed", comment="Data that has been redacted", path=f"{table_path}/clean_processed/")
