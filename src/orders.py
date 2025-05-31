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

schema = StructType([
    StructField("Row ID",LongType(),False),
    StructField("Order ID",StringType(),False),
    StructField("Order Date",StringType(),False),
    StructField("Ship Date",StringType(),False),
    StructField("Ship Mode",StringType(),False),
    StructField("Customer ID",StringType(),False),
    StructField("Product ID",StringType(),False),
    StructField("Quantity",LongType(),False),
    StructField("Price",DoubleType(),False),
    StructField("Discount",DoubleType(),False),
    StructField("Profit",DoubleType(),False),
    StructField("_corrupt_record",StringType(),True),
])

def load_dataset(spark,files):
    df = spark.read.option("header", True).option("mode", "PERMISSIVE")\
        .option("multiLine",True).schema(schema)\
        .option("columnNameOfCorruptRecord", "_corrupt_record").json(files)
    df = df.withColumn("load_ts", current_timestamp())
    return df

def cleanSchema(df):
    ori = df.columns
    new = [i.lower().replace(' ','_') for i in ori]
    for o_col,n_col in zip(ori,new):
        df = df.withColumnRenamed(o_col,n_col)
    return df

def load_silver_table(spark,bronze_table,silver_table,customer_table,product_table):
    max_d = spark.sql(f"select max(load_ts) from {silver_table}").collect()[0][0]
    max_d = '1900-01-01' if max_d==None else max_d
    df = spark.sql(f"select row_id, order_id, order_date,customer_id,product_id,profit,load_ts,_corrupt_record from {bronze_table}")
    df.withColumn("order_date", to_date("order_date","d/M/yyyy")).createOrReplaceTempView('orders')
    query=f"select * from orders where load_ts >= '{max_d}' and _corrupt_record is null"
    df1 = spark.sql(query).drop('_corrupt_record')
    df_product = spark.sql(f"select * from {product_table}")
    df_customer = spark.sql(f"select * from {customer_table}")
    df2 = df1.join(broadcast(df_product),on=df.product_id==df_product.product_id,how="left")\
        .join(broadcast(df_customer),on=df.customer_id==df_customer.customer_id,how="left")
    cols = [df1[c] for c in df1.columns]+[col("category"),col("sub_category"),col("customer_name"),df_customer.country]
    df2 = df2.fillna("unknown",subset=["category","sub_category"])\
        .select(*cols)    
    return df2
    

def main():
    bronze_table='bronze.orders'
    silver_table='silver.order_enrich'
    product_table='silver.products_1'
    customer_table='silver.customers'
    try:
        spark = SparkSession.builder.appName("PEI")\
            .getOrCreate()
        directory = "/FileStore/orders"
        files = getUnprocessedFiles(directory=directory,obj="order",spark=spark)
        filepath_to_process = [f['file_path'] for f in files]
        df = load_dataset(spark,filepath_to_process)
        if df.count()==0:
            print("nothing to load")
            return
        df1 = cleanSchema(df)
        df1.write.mode('append').saveAsTable(bronze_table)
        mark_file_process(spark,files)
        df_enrich = load_silver_table(spark,bronze_table,silver_table,customer_table,product_table)
        df_enrich.write.mode('append').saveAsTable(silver_table)
        print("done")
    except Exception as e:
        print(e)

# COMMAND ----------

main()