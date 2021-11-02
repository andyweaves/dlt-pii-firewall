# Databricks notebook source
input_path = spark.conf.get("input_path")
table_path = spark.conf.get("table_path")

# COMMAND ----------

import pickle

#file = open("/dbfs/FileStore/andrew.weaver@databricks.com/dlt/customers/dlt_sql", "rb")
#sql = pickle.load(file)
#file.close()

# COMMAND ----------

import dlt

@dlt.table(
  path=f"{table_path}/clean_processed/"
)
def clean_processed():
  return dlt.read_stream("quarantine").selectExpr("*")

#ToDo...

# 1. In the near future .selectExpr("* EXCEPT (failed_expectations)") + other columns should work, right now we're pinned to DBR 8.4... 
# 2. In future it'd be nice to join with the clean table, otherwise we'll have to do that as an additional workflow... I.e .join(read("LIVE.clean"), ["customer_id"], "left") ... 
