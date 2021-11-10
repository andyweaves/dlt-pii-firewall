# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")

# COMMAND ----------

dbutils.fs.put("/FileStore/andrew.weaver@databricks.com/dlt/customers/expectations/pii_identification.csv", """
name,constraint,action
{} may contain creditcard,CAST({} AS STRING) NOT REGEXP("^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})$") AS result,"concat('XXXXXXXXXXXXXXXX', substr({}, -3, 3)) AS {}"
{} may contain ssn,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}-\\\\d{2}-\\\\d{4}$") AS result,"'[REDACTED]' AS {}"
{} may contain expiry date,CAST({} AS STRING) NOT REGEXP("^\\\\d{2}/\\\\d{2}$") AS result,"regexp_replace({}, '^(0[1-9]|1[0-2])', 'XX') AS {}"
{} may contain security code,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}$") AS result,"'[REDACTED]' AS {}"
{} may contain email address,CAST({} AS STRING) NOT REGEXP("\\\\w@\\\\w.\\\\w") AS result,"regexp_extract({}, '^.*@(.*)$', 1) AS {}"
{} may contain ipv4,CAST({} AS STRING) NOT REGEXP("^((?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])[.]){3}(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$") AS result,"'[REDACTED]' AS {}"
""", overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT CAST(address AS STRING) NOT REGEXP("^(.+)[,\\s]+(.+?)\s*(\d{5})?$") AS result FROM aw
# MAGIC --SELECT CAST(phone_number AS STRING) NOT REGEXP("^(\\(?\\d\\d\\d\\)?)(|-|\\.)?\\d\\d\\d( |-|\\.)?\\d{4,4}(( |-|\\.)?[ext\\.]+ ?\\d+)?$") AS result FROM aw 
# MAGIC --SELECT CAST(iban AS STRING) NOT REGEXP("[a-zA-Z]{2}[0-9]{2}[a-zA-Z0-9]{4}[0-9]{7}([a-zA-Z0-9]?){0,16}") AS result FROM aw

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

import dlt

@dlt.view(
comment="Raw data that may potentially contain PII"
)
def staging():
  return (
    spark
      .read
      #.readStream
      .format("parquet")
      .schema(schema)
      .load(input_path)
  )

# COMMAND ----------

@dlt.table(
  comment="Data that has been processed and successfully evaluated against our PII detection expectations",
  path=f"{table_path}/clean/"
)
@dlt.expect_all_or_drop(rules) 
def clean():
  return dlt.read("staging")#.read_stream("staging")

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(rules, expectations) if not success]

# COMMAND ----------

import pyspark.sql.functions as F

@dlt.table(
 comment="Data that has been quarantined for potentially containing PII",
 path=f"{table_path}/quarantine/"
)
def quarantine():
  return (
      dlt
        .read("staging")
        #.read_stream("staging")
        .withColumn("failed_expectations", F.array([F.expr(value) for key, value in rules.items()]))
        .withColumn("failed_expectations", failed_expectations("failed_expectations"))
        .filter(F.size("failed_expectations") > 0)
  )

# COMMAND ----------

#@dlt.table(
#  path=f"{table_path}/failed_expectations/"
#)
#def failed_expectations():
#  return (dlt
#          .read("quarantine")
#          .select(current_timestamp().alias("timestamp"), explode(col("failed_expectations")).alias("expectation")).distinct()
#         )
