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

df.createOrReplaceTempView("aweaver_dlt")
df.write.mode("overwrite").format("parquet").save("dbfs:/aweaver/dlt_test/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NOT CAST(ssn AS STRING) REGEXP('\\d{3}-\\d{2}-\\d{4}') FROM aweaver_dlt

# COMMAND ----------

import dlt

@dlt.table(
  name="aweaver_dlt_test"
)
@dlt.expect_all({"is not ssn": "NOT CAST(ssn AS STRING) REGEXP('\\d{3}-\\d{2}-\\d{4}')"}) 
def get_input_data():
  return (
    spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format", "parquet").load("dbfs:/aweaver/dlt_test/")
  )
