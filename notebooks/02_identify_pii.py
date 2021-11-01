# Databricks notebook source
dbutils.widgets.text("input_path", defaultValue="dbfs:/aweaver/customer_raw")
input_path = dbutils.widgets.get("input_path")

# COMMAND ----------

dbutils.fs.put("/aweaver/expectations/pii_identification.csv", """
name,constraint,action
{} may contain creditcard,CAST({} AS STRING) NOT REGEXP("^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})$") AS result,"concat('XXXXXXXXXXXXXXXX', substr({}, -3, 3))"
{} may contain ssn,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}-\\\\d{2}-\\\\d{4}$") AS result,"XXX"
{} may contain expiry date,CAST({} AS STRING) NOT REGEXP("^\\\\d{2}/\\\\d{2}$") AS result,"regexp_replace({}, '^(0[1-9]|1[0-2])', 'XX')"
{} may contain security code,CAST({} AS STRING) NOT REGEXP("^\\\\d{3}$") AS result,"XXX"
{} may contain email address,CAST({} AS STRING) NOT REGEXP("\\\\w@\\\\w.\\\\w") AS result,"regexp_extract({}, '^.*@(.*)$', 1)"
{} may contain ipv4,CAST({} AS STRING) NOT REGEXP("^((?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])[.]){3}(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$") AS result,"XXX"
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
rules, actions = get_rules_and_actions(columns, "file:/dbfs/aweaver/expectations/pii_identification.csv") #f"file:{os.path.dirname(os.getcwd())}/expectations/pii_identification.csv"
rules

# COMMAND ----------

actions

# COMMAND ----------

import dlt

@dlt.view(
comment="Raw data that may potentially contain PII"
)
def staging():
  return (
    spark
      .readStream
      .format("parquet")
      .schema(schema)
      .load(input_path)
  )

# COMMAND ----------

@dlt.table(
  comment="Data that has been processed and successfully evaluated against our Expectations"
)
@dlt.expect_all_or_drop(rules) 
def clean():
  return dlt.read_stream("staging")

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(rules, expectations) if not success]

# COMMAND ----------

import pyspark.sql.functions as F

@dlt.view(
 comment="Data that has been quarantined for potentially containing PII"
)
def quarantine():
  return (
      dlt
        .read_stream("staging")
        .withColumn("_failed_expectations", F.array([F.expr(value) for key, value in rules.items()]))
        .withColumn("failed_expectations", failed_expectations("_failed_expectations"))
        .filter(F.size("failed_expectations") > 0)
  )

# COMMAND ----------

#failed_expectations = [row['failed_expectations'] for row in spark.sql("SELECT explode(failed_expectations) AS failed_expectations from aweaver.quarantine").distinct().collect()]
#failed_expectations

# COMMAND ----------

@dlt.table
def clean_processed():
  return spark.sql("SELECT * FROM aweaver.quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/release-notes/runtime/9.0.html#exclude-columns-in-select--public-preview

# COMMAND ----------

#df = spark.sql(f"""
#SELECT {', '.join(columns)} FROM aweaver.quarantine
#""")
#display(df)