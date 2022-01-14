-- Databricks notebook source
SHOW TABLES IN dlt_pii

-- COMMAND ----------

SELECT * FROM dlt_pii.quarantine

-- COMMAND ----------

DESCRIBE HISTORY dlt_pii.quarantine

-- COMMAND ----------

SELECT * FROM dlt_pii.clean

-- COMMAND ----------

DESCRIBE TABLE dlt_pii.clean

-- COMMAND ----------

DESCRIBE TABLE dlt_pii.clean_processed

-- COMMAND ----------

SELECT * FROM dlt_pii.clean_processed
