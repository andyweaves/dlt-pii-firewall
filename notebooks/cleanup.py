# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS aweaver_dlt CASCADE;
# MAGIC --CREATE DATABASE IF NOT EXISTS aweaver_dlt
# MAGIC --LOCATION "dbfs:/aweaver/delta/customers";

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE TABLE IF NOT EXISTS aweaver_dlt.metrics
# MAGIC --USING DELTA
# MAGIC --(timestamp TIMESTAMP, step STRING, expectation STRING, passed INT, failed INT)

# COMMAND ----------

dbutils.fs.rm("/FileStore/andrew.weaver@databricks.com/dlt/customers/", recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/aweaver/delta/customers", recurse=True)
