# Databricks notebook source
dbutils.widgets.text("input_path", defaultValue="dbfs:/aweaver/customer_raw")
input_path = dbutils.widgets.get("input_path")

# COMMAND ----------

dbutils.fs.put("/aweaver/expectations/pii_identification.csv", """
name,constraint
{} is not creditcard,CAST({} AS STRING) NOT REGEXP("^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})$") AS result
{} is not ssn,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}-\\\\d{2}-\\\\d{4}$") AS result
{} is not expiry date,CAST({} AS STRING) NOT REGEXP("^\\\\d{2}/\\\\d{2}$") AS result
{} is not security code,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}$") AS result
{} is not email address,CAST({} AS STRING) NOT REGEXP("\\\\w@\\\\w.\\\\w") AS result
{} is not ipv4,CAST({} AS STRING) NOT REGEXP("^((?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])[.]){3}(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$") AS result
""", overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT CAST(address AS STRING) NOT REGEXP("^(.+)[,\\s]+(.+?)\s*(\d{5})?$") AS result FROM aw
# MAGIC --SELECT CAST(phone_number AS STRING) NOT REGEXP("^(\\(?\\d\\d\\d\\)?)(|-|\\.)?\\d\\d\\d( |-|\\.)?\\d{4,4}(( |-|\\.)?[ext\\.]+ ?\\d+)?$") AS result FROM aw 
# MAGIC --SELECT CAST(iban AS STRING) NOT REGEXP("[a-zA-Z]{2}[0-9]{2}[a-zA-Z0-9]{4}[0-9]{7}([a-zA-Z0-9]?){0,16}") AS result FROM aw

# COMMAND ----------

import os
from pyspark.sql.functions import col

def get_rules_for_dataset(columns, expectations):
  """
    loads data quality rules from csv file
    :param tag: group to match
    :return: dictionary of rules that matched the group
  """
  rules = {}
  raw_rules = spark.read.csv(expectations, header=True, inferSchema=True).collect()
  for col in columns:
    for row in raw_rules:
      rules[row["name"].replace("{}", col)] = row["constraint"].replace("{}", col)
  return rules

# COMMAND ----------

columns = spark.read.parquet(input_path).columns
schema = spark.read.parquet(input_path).schema
rules = get_rules_for_dataset(columns, "file:/dbfs/aweaver/expectations/pii_identification.csv") #f"file:{os.path.dirname(os.getcwd())}/expectations/pii_identification.csv"
rules

# COMMAND ----------

import dlt

@dlt.create_table()
def bronze():
  return (
    spark
      .readStream
      .format("parquet")
      .schema(schema)
      .load(input_path)
  )

# COMMAND ----------

@dlt.create_table()
@dlt.expect_all_or_drop(rules) 
def silver():
  return dlt.read_stream("bronze")

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(rules, expectations) if not success]

# COMMAND ----------

import pyspark.sql.functions as F

@dlt.create_table()
def pii_quarantine():
  return (
      dlt
        .read_stream("bronze")
        .withColumn("_expectations", F.array([F.expr(value) for key, value in rules.items()]))
        .withColumn("failed_expectations", failed_expectations("failed_expectations"))
        .filter(F.size("_expectations") > 0)
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aweaver.quarantine
