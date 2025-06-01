# Databricks notebook source
# MAGIC %run "./utils"

# COMMAND ----------

import os
if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
    from .utils import *

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import pandas as pd
from pyspark.sql.window import Window


# COMMAND ----------

schema = StructType([
    StructField("customer_id",StringType(),False),
    StructField("customer_name",StringType(),True),
    StructField("email",StringType(),True),
    StructField("phone",StringType(),True),
    StructField("address",StringType(),True),
    StructField("segment",StringType(),True),
    StructField("country",StringType(),True),
    StructField("city",StringType(),True),
    StructField("state",StringType(),True),
    StructField("postal",StringType(),True),
    StructField("region", StringType(),True)
])
def load_dataset(spark,files):
    df = spark.read\
    .format("com.crealytics.spark.excel")\
    .option("header", "true")\
    .schema(schema)\
    .load("dbfs:/FileStore/customers/Customer.xlsx")
    print("reading from source")
    return df

# COMMAND ----------

def load_enrich_data(spark,src):
    print("loading data to silver layer")
    df = spark.sql(f"select * from {src}")
    #clean the data
    df = df.withColumn('customer_name', trim(regexp_replace(regexp_replace('customer_name',r"[^a-zA-Z\s]", ""),r"\s+", " ")))\
    .withColumn('email', regexp_replace('email',r"[^\w\.\@\+\-]", ""))\
    .withColumn('phone',when(length(regexp_replace(col("phone"), r"\D", "")) < 10,None)\
        .otherwise(regexp_replace(col("phone"), r"\D", "")\
        ))
    win_spec=Window.partitionBy('customer_id').orderBy(col('load_ts').desc())
    df = df.withColumn("newest", row_number().over(win_spec)).filter(col("newest") == 1).drop("newest")
    return df


# COMMAND ----------

def main():
    raw_table = "bronze.customers"
    df = load_dataset(spark,"dbfs:/FileStore/customers/Customer.xlsx")
    df = df.withColumn("load_ts", current_timestamp())
    df.write.mode("append").saveAsTable(raw_table)
    print("done loading to raw layer")
    
    df1 = load_enrich_data(spark,raw_table)
    df1.write.mode('overwrite').saveAsTable("silver.customers")
    print("done loading to silver layer")
    

# COMMAND ----------

main()