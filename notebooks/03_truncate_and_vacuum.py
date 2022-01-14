# Databricks notebook source
dbutils.widgets.text("DATABASE_NAME", "dlt_pii")
dbutils.widgets.text("TABLE_NAME", "quarantine")
dbutils.widgets.dropdown("VACUUM_RETENTION", defaultValue="0", choices=["0", "1", "8", "24", "168"])

DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
TABLE_NAME = dbutils.widgets.get("TABLE_NAME")
VACUUM_RETENTION = dbutils.widgets.get("VACUUM_RETENTION")

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {DATABASE_NAME}.{TABLE_NAME}")

# COMMAND ----------

if int(VACUUM_RETENTION) != 168:
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

spark.sql(f"VACUUM {DATABASE_NAME}.{TABLE_NAME} RETAIN {VACUUM_RETENTION} HOURS")
