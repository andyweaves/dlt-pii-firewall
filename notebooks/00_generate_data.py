# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.widgets.dropdown("num_records", defaultValue="1000", choices=["50", "100", "1000", "10000", "250000"])
dbutils.widgets.text("output_path", defaultValue="dbfs:/aweaver/customer_raw")

num_records = int(dbutils.widgets.get("num_records"))
output_path = dbutils.widgets.get("output_path")

# COMMAND ----------

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

schema = StructType([
  StructField("customer_id", LongType(), False),
  StructField("name", StringType(), False),
  StructField("email", StringType(), False),
  StructField("address", StringType(), False),
  StructField("ip_address", StringType(), False),
  StructField("phone_number", StringType(), False),
  StructField("ssn", StringType(), False),
  StructField("iban", StringType(), False),
  StructField("credit_card", LongType(), False),
  StructField("expiry_date", StringType(), False),
  StructField("security_code", StringType(), False)
  ])

@pandas_udf("long")
def get_customer_id(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for id in batch_iter:
        yield int(time.time()) + id

def generate_fake_data(pdf: pd.DataFrame) -> pd.DataFrame:
  
  def generate_data(y):
      
    from faker import Faker
    fake = Faker('en_US')

    y["name"] = fake.name()
    y["email"] = fake.email()
    y["address"] = fake.address()
    y["ip_address"] = fake.ipv4()
    y["phone_number"] = fake.phone_number()
    y["ssn"] = fake.ssn()
    y["iban"] = fake.iban()
    y["credit_card"] = int(fake.credit_card_number())
    y["expiry_date"] = fake.credit_card_expire()
    y["security_code"] = fake.credit_card_security_code()

    return y
    
  return pdf.apply(generate_data, axis=1).drop(["partition_id", "id"], axis=1)

df = spark.range(1, num_records + 1).withColumn("customer_id", get_customer_id(col("id"))).withColumn("partition_id", spark_partition_id()).groupBy("partition_id").applyInPandas(generate_fake_data, schema).orderBy(asc("customer_id"))

display(df)

# COMMAND ----------

df.write.format("parquet").mode("append").save(output_path) 
