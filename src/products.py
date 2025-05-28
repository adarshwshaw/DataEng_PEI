# Databricks notebook source
# MAGIC %run "./utils"

# COMMAND ----------

import os
if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
    from .utils import *

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,current_timestamp
from delta.tables import DeltaTable

schema = StructType([
    StructField("product_id",StringType(),False),
    StructField("category",StringType(),False),
    StructField("sub_category",StringType(),False),
    StructField("product_name",StringType(),False),
    StructField("state",StringType(),False),
    StructField("price_per_product",DoubleType(),False),
    StructField("_corrupt_record", StringType(),True)
])

def load_dataset(spark,files,corrupt_path):
    df = spark.read.option("header", True).option("mode", "PERMISSIVE")\
        .option("columnNameOfCorruptRecord", "_corrupt_record").csv(files, schema=schema)
    df = df.withColumn("load_ts", current_timestamp())
    badRecordsDF = df.filter(col("_corrupt_record").isNotNull())

    # Save bad records to a CSV file
    badRecordsDF.write.option("header", "true").mode("append")\
        .parquet(corrupt_path)
    
    goodrecdf = df.filter(col("_corrupt_record").isNull()).drop(col('_corrupt_record'))
    return goodrecdf

def load_enriched_data(spark,bronze_df, silver_table, on, update_condition):
    target_table = DeltaTable.forName(spark, silver_table)
    target_table.alias("target").merge(
        bronze_df.alias("source"),on
    ).whenMatchedUpdateAll(update_condition)\
        .whenNotMatchedInsertAll().execute()


def main():
    try:
        spark = SparkSession.builder.appName("PEI")\
            .getOrCreate()

        directory = "/FileStore/products"
        corrupt_path="/FileStore/badrecords/products"

        files = getUnprocessedFiles(directory=directory, obj='product',  spark=spark)
        
        filepath_to_process = [f['file_path'] for f in files]
        df = load_dataset(spark,filepath_to_process,corrupt_path)
        if (df.isEmpty()):
            print("nothing to load")
            return 
        df.write.mode('append').saveAsTable('bronze.products')
        mark_file_process(spark,files)
        df = df.select("product_id","category","sub_category","load_ts").dropDuplicates()
        load_enriched_data(spark,df,"silver.products","target.product_id=source.product_id","source.load_ts > target.load_ts")
    except Exception as e:
        print(e)
    

# COMMAND ----------

main()