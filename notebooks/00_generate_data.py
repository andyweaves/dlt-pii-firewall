# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.widgets.dropdown("NUM_ROWS", defaultValue="1000", choices=["50", "100", "1000", "10000", "250000"])
dbutils.widgets.text("OUTPUT_DIR", defaultValue="dbfs:/dlt_pii/customer_raw")

NUM_ROWS = int(dbutils.widgets.get("NUM_ROWS"))
OUTPUT_DIR = dbutils.widgets.get("OUTPUT_DIR")

# COMMAND ----------

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import date
import random

schema = StructType([
  StructField("customer_id", LongType(), False),
  StructField("name", StringType(), False),
  StructField("email", StringType(), False),
  StructField("date_of_birth", DateType(), False),
  StructField("age", IntegerType(), False),
  StructField("address", StringType(), False),
  StructField("ip_address", StringType(), False),
  StructField("phone_number", StringType(), False),
  StructField("ssn", StringType(), False),
  StructField("iban", StringType(), False),
  StructField("credit_card", LongType(), False),
  StructField("expiry_date", StringType(), False),
  StructField("security_code", StringType(), False),
  StructField("freetext", StringType(), False)
  ])

@pandas_udf("long")
def get_customer_id(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for id in batch_iter:
        yield int(time.time()) + id

def generate_fake_data(pdf: pd.DataFrame) -> pd.DataFrame:
  
  def generate_data(y):
      
    from faker import Faker
    fake = Faker('en_US')
    
    def get_random_pii():
      return random.choice([fake.email(), fake.credit_card_number(), fake.ipv4(), fake.ssn(), fake.iban(), fake.date_between(start_date='-90y', end_date='-18y')])
    
    dob = fake.date_between(start_date='-90y', end_date='-18y')

    y["name"] = fake.name()
    y["email"] = fake.email()
    y["date_of_birth"] = dob
    y["age"] = date.today().year - dob.year
    y["address"] = fake.address()
    y["ip_address"] = fake.ipv4()
    y["phone_number"] = fake.phone_number()
    y["ssn"] = fake.ssn()
    y["iban"] = fake.iban()
    y["credit_card"] = int(fake.credit_card_number())
    y["expiry_date"] = fake.credit_card_expire()
    y["security_code"] = fake.credit_card_security_code()
    y["freetext"] = f"{fake.sentence()} {get_random_pii()} {fake.sentence()} {get_random_pii()} {fake.sentence()}"

    return y
    
  return pdf.apply(generate_data, axis=1).drop(["partition_id", "id"], axis=1)

df = spark.range(1, NUM_ROWS + 1).withColumn("customer_id", get_customer_id(col("id"))).withColumn("partition_id", spark_partition_id()).groupBy("partition_id").applyInPandas(generate_fake_data, schema).orderBy(asc("customer_id"))

display(df)

# COMMAND ----------

df.write.format("parquet").mode("append").save(OUTPUT_DIR) 
