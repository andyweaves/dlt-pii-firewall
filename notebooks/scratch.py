# Databricks notebook source
STORAGE_DIR = "dbfs:/pipelines/pii_wip"

# COMMAND ----------

from pyspark.sql.functions import *

events = spark.read.format("delta").load(f"{STORAGE_DIR}/system/events/").orderBy(desc("timestamp"))
#.where("event_type = 'flow_progress'")
display(events)

# COMMAND ----------

display(events.where(col("event_type").isin(["flow_progress", "update_progress", "maintenance_progress"])).selectExpr("id", "timestamp", "event_type", "message", "details").orderBy(desc("timestamp")))

# COMMAND ----------

display(events.select("event_type").distinct().orderBy(asc("event_type")))

# COMMAND ----------

audit = events.where("event_type = 'user_action'").selectExpr("id", "timestamp", "details:user_action:user_name", "details:user_action:action", "details")
display(audit)

# COMMAND ----------

expectations = events.where("details:flow_progress.status='COMPLETED'").selectExpr("id", "timestamp", "explode(from_json(details:flow_progress.data_quality.expectations, 'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) as expectations").selectExpr("date(timestamp) as date", "expectations.dataset as dataset", "expectations.name as expectation", "expectations.passed_records as passed_records", "expectations.failed_records as failed_records").groupBy("date", "dataset", "expectation").sum().withColumnRenamed("sum(passed_records)", "passed_records").withColumnRenamed("sum(failed_records)", "failed_records")
display(expectations)

# COMMAND ----------

dq = expectations.selectExpr("date(timestamp) as date", "expectations.dataset as dataset", "expectations.name as expectation", "expectations.passed_records as passed_records", "expectations.failed_records as failed_records").groupBy("date", "dataset", "expectation").sum().withColumnRenamed("sum(passed_records)", "passed_records").withColumnRenamed("sum(failed_records)", "failed_records")
display(dq)

# COMMAND ----------

dq = events.where("details:flow_progress.status='COMPLETED'").selectExpr("id", "timestamp", "details:flow_progress.metrics.num_output_rows", "details:flow_progress.data_quality.dropped_records", "details:flow_progress.status", "*")
display(dq)

# COMMAND ----------

events.createOrReplaceTempView("events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT event_type FROM events
