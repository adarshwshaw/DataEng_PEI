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

def load_dataset(spark,files):
    df = spark.read.option("header", True).option("mode", "PERMISSIVE")\
        .option("columnNameOfCorruptRecord", "_corrupt_record").csv(files, schema=schema)
    df = df.withColumn("load_ts", current_timestamp())
    return df

def load_enriched_data(spark,bronze_df, silver_table):
    # target_table = DeltaTable.forName(spark, silver_table)
    # target_table.alias("target").merge(
    #     bronze_df.alias("source"),on
    # ).whenMatchedUpdateAll(update_condition)\
    #     .whenNotMatchedInsertAll().execute()
    bronze_df.createOrReplaceTempView("bronze_t")
    query = f"""
        Merge into {silver_table} tar
        using bronze_t src 
        on tar.product_id=src.product_id
        when matched AND src.load_ts >= tar.load_ts THEN UPDATE SET 
        tar.category = src.category,
        tar.sub_category = src.sub_category,
        tar.load_ts = src.load_ts
        when not matched then insert (
        product_id, category, sub_category, load_ts
        ) VALUES (
        src.product_id, src.category, src.sub_category, src.load_ts
        )
        """
    print(query)
    spark.sql(
        query
    )



def main():
    try:
        spark = SparkSession.builder.appName("PEI")\
            .getOrCreate()

        directory = "/FileStore/products"
        # corrupt_path="/FileStore/badrecords/products"

        files = getUnprocessedFiles(directory=directory, obj='product',  spark=spark)
        
        filepath_to_process = [f['file_path'] for f in files]
        df = load_dataset(spark,filepath_to_process)
        if (df.isEmpty()):
            print("nothing to load")
            return 
        df.write.mode('append').saveAsTable('bronze.products')
        df1 = spark.sql("select product_id,category,sub_category,_corrupt_record,load_ts from bronze.products where _corrupt_record is null and load_ts = (select max(load_ts) from bronze.products)").dropDuplicates()
        mark_file_process(spark,files)
        load_enriched_data(spark,df1,"silver.products_1")
    except Exception as e:
        print(e)
    

# COMMAND ----------

main()