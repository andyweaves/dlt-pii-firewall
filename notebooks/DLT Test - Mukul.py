# Databricks notebook source
ssns = [(0, "766-90-6881"),(1, "540-94-4867"),(2, "167-84-0473"),(3, "223-12-9690"),(4, "088-49-0114")]
rdd = spark.sparkContext.parallelize(ssns)

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([StructField("id", LongType(), True),
  StructField("ssn", StringType(), True)])

df = spark.createDataFrame(rdd, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/mukul/dlt_test

# COMMAND ----------

df.createOrReplaceTempView("mukul_dlt")
df.write.mode("overwrite").format("delta").save("dbfs:/mukul/dlt_test/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL regex query on view
# MAGIC -- don't touch this, this has the initial repro
# MAGIC SELECT ssn REGEXP('\\d{3}-\\d{2}-\\d{4}') FROM mukul_dlt

# COMMAND ----------

# Python with SQL regex query on view
# don't touch this, this has the initial repro
display(spark.sql("SELECT ssn REGEXP('\\d{3}-\\d{2}-\\d{4}') FROM mukul_dlt"))

# COMMAND ----------

# Play around with the stuff below

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL query string. Produces the correct pattern.
# MAGIC SELECT ssn REGEXP('\\d{3}-\\d{2}-\\d{4}') FROM mukul_dlt

# COMMAND ----------

# Python - Incorrect - the '\' gets dropped.
display(spark.sql("SELECT ssn REGEXP('\\d{3}-\\d{2}-\\d{4}') FROM mukul_dlt"))

# COMMAND ----------

# Python - Correct - the '\' is included.
display(spark.sql("SELECT ssn REGEXP('\\\d{3}-\\\d{2}-\\\d{4}') FROM mukul_dlt"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# spark.read.table("mukul_dlt").select("NOT CAST(ssn AS STRING) REGEXP('\\d{3}-\\d{2}-\\d{4}')")

# COMMAND ----------

spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format", "parquet").load("dbfs:/mukul/dlt_test/")

# COMMAND ----------

import dlt

@dlt.table(
  name="mukul_dlt_test"
)
@dlt.expect_all({"is not ssn": "NOT CAST(ssn AS STRING) REGEXP('\\d{3}-\\d{2}-\\d{4}')"}) 
def get_input_data():
  return (
    spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format", "parquet").load("dbfs:/mukul/dlt_test/")
  )
