# Databricks notebook source
table_path = spark.conf.get("table_path")
storage_path = spark.conf.get("storage_path")

# COMMAND ----------

import dlt
from pyspark.sql.functions import desc, col

@dlt.table(
 name="event_logs",
 comment="The raw event logs relating to our DLT pipeline",
 path=f"{table_path}/event_logs/"
)
def event_logs():
    return (
    spark.read
      .format("delta")
      .load(f"{storage_path}/system/events")
      .orderBy(desc("timestamp"))
    )

# COMMAND ----------

@dlt.table(
 name="audit_logs",
 comment="Audit logs relating to our DLT pipeline",
 path=f"{table_path}/audit_logs/"
)
def audit_logs():
    return (
    dlt.read("event_logs").where("event_type = 'user_action'")
      .selectExpr("id", "timestamp", "details:user_action:user_name", "details:user_action:action", "details")
      .orderBy(desc("timestamp"))
    )

# COMMAND ----------

@dlt.table(
 name="data_quality_logs",
 comment="Data Quality logs relating to our DLT pipeline",
 path=f"{table_path}/data_quality_logs/"
)
def data_quality_logs():
    return (
    dlt.read("event_logs").where("details:flow_progress.status='COMPLETED'")
      .selectExpr("id", "timestamp", "explode(from_json(details:flow_progress.data_quality.expectations, 'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) as expectations")
      .selectExpr("id", "date(timestamp) as date", "expectations.dataset as dataset", "expectations.name as expectation", "expectations.passed_records as passed_records", "expectations.failed_records as failed_records")
      .groupBy("id", "date", "dataset", "expectation").sum().withColumnRenamed("sum(passed_records)", "passed_records").withColumnRenamed("sum(failed_records)", "failed_records").orderBy(desc("date"))
 )

# COMMAND ----------

@dlt.table(
 name="flow_logs",
 comment="Flow logs relating to our DLT pipeline",
 path=f"{table_path}/flow_logs/"
)
def flow_logs():
    return (
    dlt.read("event_logs").where(col("event_type").isin(["flow_progress", "update_progress", "maintenance_progress"]))
      .selectExpr("id", "timestamp", "event_type", "message", "details")
      .orderBy(desc("timestamp"))
    )
