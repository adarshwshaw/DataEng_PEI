# Databricks notebook source
'''
This file is to set up infrastucture and create tables in databricks community edition
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC -- schema for raw layer
# MAGIC create schema if not exists bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- audit table to track files already processed
# MAGIC CREATE TABLE IF NOT EXISTS bronze.processed_files (
# MAGIC     file_path STRING,
# MAGIC     object STRING,
# MAGIC     modification_time LONG
# MAGIC ) USING delta

# COMMAND ----------

