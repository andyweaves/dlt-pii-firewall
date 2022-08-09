# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.widgets.dropdown("NUM_ROWS", defaultValue="1000", choices=["50", "100", "1000", "3000", "5000", "10000", "250000", "500000", "1000000", "5000000", "10000000", "20000000"])
dbutils.widgets.text("OUTPUT_DIR", defaultValue="dbfs:/dlt_pii/customer_raw")
dbutils.widgets.dropdown("GENERATE_CLEAN_DATA", defaultValue="False", choices=["True", "False"])
dbutils.widgets.dropdown("GENERATE_PII_DATA", defaultValue="True", choices=["True", "False"])

GENERATE_PII_DATA = dbutils.widgets.get("GENERATE_PII_DATA") == "True"
GENERATE_CLEAN_DATA = dbutils.widgets.get("GENERATE_CLEAN_DATA") == "True"
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
from faker import Faker
from mimesis import Generic
from mimesis.locales import Locale

schema = StructType([
  StructField("customer_id", LongType(), False),
  StructField("name", StringType(), False),
  StructField("email", StringType(), False),
  StructField("date_of_birth", DateType(), False),
  StructField("age", LongType(), False),
  StructField("address", StringType(), False),
  StructField("ipv4", StringType(), False),
  StructField("ipv6", StringType(), False),
  StructField("mac_address", StringType(), False),
  StructField("phone_number", StringType(), False),
  StructField("ssn", StringType(), False),
  StructField("iban", StringType(), False),
  StructField("credit_card", LongType(), False),
  StructField("expiry_date", StringType(), False),
  StructField("security_code", StringType(), False),
  StructField("freetext", StringType(), False)
  ])

fake = Faker('en_US')
generic = Generic(locale=Locale.EN)

def get_random_pii():
  return random.choice([fake.ascii_free_email(), fake.ipv4(), fake.ipv6()])

@pandas_udf("long")
def get_customer_id(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  for id in batch_iter:
      yield int(time.time()) + id
      
pii_struct_schema = StructType([
    StructField("email_address", StringType(), False),
    StructField("ipv4_private", StringType(), False),
    StructField("ip_address_v6", StringType(), False),
    StructField("ipv4_with_port", StringType(), False),
    StructField("mac", StringType(), False),
    StructField("imei", StringType(), False),
    StructField("credit_card_number", StringType(), False), 
    StructField("credit_card_expiration_date", StringType(), False), 
    StructField("cvv", StringType(), False), 
    StructField("paypal", StringType(), False), 
    StructField("random_text_with_email", StringType(), False),
    StructField("random_text_with_ipv4", StringType(), False)
])

def pii_struct():
  return (generic.person.email(), fake.ipv4_private(), fake.ipv6(), generic.internet.ip_v4_with_port(), generic.internet.mac_address(), generic.code.imei(), generic.payment.credit_card_number(), generic.payment.credit_card_expiration_date(), generic.payment.cvv(), generic.payment.paypal(), f"{fake.catch_phrase()} {generic.person.email()}", f"{fake.catch_phrase()} {fake.ipv4_public()}")

pii_struct_udf = udf(pii_struct, pii_struct_schema)

def generate_fake_data(pdf: pd.DataFrame) -> pd.DataFrame:
    
  def generate_data(y):
    
    dob = fake.date_between(start_date='-99y', end_date='-18y')

    y["name"] = fake.name()
    y["email"] = fake.ascii_free_email()
    y["date_of_birth"] = dob #.strftime("%Y-%m-%d")
    y["age"] = date.today().year - dob.year
    y["address"] = fake.address()
    y["ipv4"] = fake.ipv4()
    y["ipv6"] = fake.ipv6()
    y["mac_address"] = fake.mac_address()
    y["phone_number"] = fake.phone_number()
    y["ssn"] = fake.ssn()
    y["iban"] = fake.iban()
    y["credit_card"] = int(fake.credit_card_number())
    y["expiry_date"] = fake.credit_card_expire()
    y["security_code"] = fake.credit_card_security_code()
    y["freetext"] = f"{fake.sentence()} {get_random_pii()} {fake.sentence()} {get_random_pii()} {fake.sentence()}"

    return y
    
  return pdf.apply(generate_data, axis=1).drop(["partition_id", "id"], axis=1)

if GENERATE_PII_DATA:

  initial_data = spark.range(1, NUM_ROWS + 1).withColumn("customer_id", get_customer_id(col("id"))).repartition(100)
  
  initial_data.write.format("parquet").mode("overwrite").save(OUTPUT_DIR)
  
  pii_data = (spark.read.parquet(OUTPUT_DIR)
              .withColumn("partition_id", spark_partition_id())
              .groupBy("partition_id")
              .applyInPandas(generate_fake_data, schema)
              .withColumn("pii_struct", pii_struct_udf())
              .withColumn("pii_map", create_map(lit("email_addr"), col("email"), lit("ip_address"), col("ipv4"), lit("home_address"), col("address")))
              .withColumn("pii_array", array("email", "ipv4", "ipv6"))
              .orderBy(asc("customer_id")))

  data = pii_data

# COMMAND ----------

from pyspark.sql.functions import struct, lit

import pandas as pd

if GENERATE_CLEAN_DATA:
 
  # Generate some "clean" data which doesn't contain PII and union them to our data which contains generated PII...
  raw_data = [
    [10001, "John L3nn0n", "", date.today(), 1, "1 Strawberry Fields", "999.999.999.999", "w:@:l:r:u:5", "z:0:0:0:0:0", "1234", "Taxman (1966)", "", 1234, "Y3st3rday", "1", "There are places I'll remember all my life though some have changed"], 
    [20001, "Paul McCartn3y", "", date.today(), 9, "2 Penny Lane", "1.1.1", "y:e:l:l:0:w", "x:0:0:0:0:0", "5678", "Money (That's What I Want)", "", 5678, "E1ght Days a W33k", "2", "And in the end the love you take is equal to the love you make"], 
    [30001, "R1ngo Starr", "", date.today(), 6, "3 Abbey Road", "255.255", "x:y:z:1", "0:0:0:0:0:0", "Parlophone 4", "You Never Give Me Your M0ney", "", 1111, "T0m0rr0w N3v3r Kn0ws", "3", "How does it feel to be one of the beautiful people?"], 
    [40001, "George Harr1s0n", "", date.today(), 8, "4 Octopus's Garden", "0.1", "a:b:c:d:e:f:g:h", "a:b:c:d:e:f", "No Reply 4", "Back in the U.S.S.R. b0y", "", 2222, "Tim3l3ss", "4", "Something in the way she moves..."]]

  pdf = pd.DataFrame(raw_data, columns = ["customer_id", "name", "email", "date_of_birth", "age", "address", "ipv4", "ipv6", "mac_address", "phone_number", "ssn", "iban", "credit_card", "expiry_date", "security_code", "freetext"])

  clean_data = (spark.createDataFrame(pdf)
                .withColumn("nested_pii", 
                  struct([
                  lit("There are places I'll remember, all my life...").alias("email_address"), 
                  lit("999.999.999.999").alias("ipv4_private"),
                  lit("w:@:l:r:u:5").alias("ip_address_v6"),
                  lit("1.1:22").alias("ipv4_with_port"),
                  lit("a:b:c:d:e:f").alias("mac"),
                  lit("1966").alias("imei"),
                  lit("1234").alias("credit_card_number"),
                  lit("1970").alias("credit_card_expiration_date"),
                  lit("1s").alias("cvv"),
                  lit("There are places I'll remember all my life though some have changed. Some forever, not for better... some have gone and some remain. All these places have their moments, with lovers and friends I still can recall. Some are dead and some are living... In my life I've loved them all").alias("paypal"),
                  lit("I saw a film today, oh boy... about a lucky man that made the grade. A crowd of people stood and stared... they'd seen his face before, nobody-was-really-sure-if-he-was-from-the-house-of-lords....").alias("random_text_with_email"),
                  lit("All you need is love 1.1.1").alias("random_text_with_ipv4")
                  ]))
               .withColumn("pii_map", create_map(lit("email_addr"), col("email"), lit("ip_address"), col("ipv4"), lit("home_address"), col("address")))
               .withColumn("pii_array", array("email", "ipv4", "ipv6"))
               )
  
  if GENERATE_PII_DATA:
    data = pii_data.union(clean_data)    
  else:
    data = clean_data

# COMMAND ----------

data.write.format("parquet").mode("overwrite").save(OUTPUT_DIR) 

# COMMAND ----------

df = spark.read.parquet(OUTPUT_DIR)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)
