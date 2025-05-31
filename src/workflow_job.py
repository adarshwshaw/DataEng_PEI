# Databricks notebook source
# MAGIC %run "./customer"

# COMMAND ----------

# MAGIC %run "./products"

# COMMAND ----------

# MAGIC %run "./orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.order_enrich

# COMMAND ----------

# MAGIC %sql
# MAGIC select year(order_date) as order_year,category,sub_category,customer_id,sum(profit) as total_profit from silver.order_enrich group by order_year,category,sub_category,customer_id order by order_year,category,sub_category,customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from silver.products_1