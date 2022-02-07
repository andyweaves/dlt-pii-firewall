# Databricks notebook source
dbutils.widgets.text("INPUT_DIR", "dbfs:/dlt_pii/customer_raw")
dbutils.widgets.text("DELTA_DIR", "dbfs:/dlt_pii/customer_delta")
dbutils.widgets.text("STORAGE_DIR", "dbfs:/dlt_pii/customer_pipeline")
dbutils.widgets.text("DATABASE_NAME", "dlt_pii")
dbutils.widgets.dropdown("CREATE_NEW_DATABASE", defaultValue="True", choices=["True", "False"])
dbutils.widgets.dropdown("CLEANUP_INPUT", defaultValue="False", choices=["True", "False"])

INPUT_DIR = dbutils.widgets.get("INPUT_DIR")
DELTA_DIR = dbutils.widgets.get("DELTA_DIR")
STORAGE_DIR = dbutils.widgets.get("STORAGE_DIR")
DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
CREATE_NEW_DATABASE = dbutils.widgets.get("CREATE_NEW_DATABASE") == "True"
CLEANUP_INPUT = dbutils.widgets.get("CLEANUP_INPUT") == "True"

DIRS = [DELTA_DIR, STORAGE_DIR]

if CLEANUP_INPUT:
  DIRS.append(INPUT_DIR)

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME} CASCADE")

# COMMAND ----------

if CREATE_NEW_DATABASE:
  print(f"Creating database {DATABASE_NAME}...")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

# COMMAND ----------

for DIR in DIRS:
  print(f"Deleting {DIR}...")
  dbutils.fs.rm(DIR, recurse=True)

# COMMAND ----------

#current_user = sql("SELECT current_user() AS current_user").head()[0]
#current_user
