# Databricks notebook source
'''
This file is to set up infrastucture and create tables in databricks community edition
'''

# COMMAND ----------

# MAGIC %pip install openpyxl pandas

# COMMAND ----------

paths=[
    "/FileStore/badrecords/products",
    "/user/hive/warehouse/bronze.db/processed_files",
    "/user/hive/warehouse/bronze.db/products",
    "/user/hive/warehouse/silver.db/products_1",
    "/user/hive/warehouse/bronze.db/customers",
    "/user/hive/warehouse/silver.db/customers",
    "/user/hive/warehouse/bronze.db/orders",
    "/user/hive/warehouse/silver.db/order_enrich"
]
res=[]
for f in paths:
    res.append(dbutils.fs.rm(f, recurse=True))
res


# COMMAND ----------

# dbutils.fs.mkdirs("/FileStore/products")
# dbutils.fs.mkdirs("/FileStore/customers")
# dbutils.fs.mkdirs("/FileStore/orders")
# dbutils.fs.mkdirs("/FileStore/badrecords/products")
# dbutils.fs.mkdirs("/FileStore/badrecords/customers")
# dbutils.fs.mkdirs("/FileStore/badrecords/orders")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- schema for raw layer
# MAGIC create schema if not exists bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- audit table to track files already processed
# MAGIC drop table if exists bronze.processed_files;
# MAGIC CREATE TABLE IF NOT EXISTS bronze.processed_files (
# MAGIC     file_path STRING,
# MAGIC     object STRING,
# MAGIC     modification_time LONG
# MAGIC ) USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bronze.orders; 
# MAGIC CREATE Table IF NOT EXISTS bronze.orders(
# MAGIC   row_id long,
# MAGIC   order_id String,
# MAGIC   order_date String,
# MAGIC   ship_date String,
# MAGIC   ship_mode String,
# MAGIC   customer_id String,
# MAGIC   product_id String,
# MAGIC   quantity Long,
# MAGIC   price Double,
# MAGIC   discount Double,
# MAGIC   profit Double,
# MAGIC   _corrupt_record String,
# MAGIC   load_ts timestamp
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bronze.customers;
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers (
# MAGIC     customer_id String,
# MAGIC     customer_name String,
# MAGIC     email String,
# MAGIC     phone String,
# MAGIC     address String,
# MAGIC     segment String,
# MAGIC     country String,
# MAGIC     city String,
# MAGIC     state String,
# MAGIC     postal String,
# MAGIC     region String,
# MAGIC     load_ts TIMESTAMP
# MAGIC ) USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bronze.products;
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products (
# MAGIC     product_id STRING,
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     product_name STRING,
# MAGIC     state STRING,
# MAGIC     price_per_product DOUBLE,
# MAGIC     load_ts TIMESTAMP,
# MAGIC     _corrupt_record STRING
# MAGIC ) USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- schema for enriched layer
# MAGIC create schema if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver.products_1;
# MAGIC CREATE TABLE IF NOT EXISTS silver.products_1 (
# MAGIC     product_id STRING,
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     load_ts TIMESTAMP
# MAGIC ) USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver.customers;
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers (
# MAGIC     customer_id String,
# MAGIC     customer_name String,
# MAGIC     email String,
# MAGIC     phone String,
# MAGIC     address String,
# MAGIC     segment String,
# MAGIC     country String,
# MAGIC     city String,
# MAGIC     state String,
# MAGIC     postal String,
# MAGIC     region String,
# MAGIC     load_ts TIMESTAMP
# MAGIC ) USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver.order_enrich;
# MAGIC create table if not exists silver.order_enrich(
# MAGIC   row_id long,
# MAGIC   order_id string,
# MAGIC   order_date date,
# MAGIC   customer_id string,
# MAGIC   product_id string,
# MAGIC   profit decimal(38,2),
# MAGIC   category string,
# MAGIC   sub_category string,
# MAGIC   customer_name string,
# MAGIC   country string,
# MAGIC   load_ts timestamp
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists gold.kpi_agg;
# MAGIC create view if not exists gold.kpi_agg as
# MAGIC select
# MAGIC   year(order_date) as order_year,
# MAGIC   category,
# MAGIC   sub_category,
# MAGIC   customer_id,
# MAGIC   sum(profit) as total_profit
# MAGIC from
# MAGIC   silver.order_enrich
# MAGIC group by
# MAGIC   order_year,
# MAGIC   category,
# MAGIC   sub_category,
# MAGIC   customer_id
# MAGIC order by
# MAGIC   order_year,
# MAGIC   category,
# MAGIC   sub_category,
# MAGIC   customer_id