# Databricks notebook source
# MAGIC %run "./customer"

# COMMAND ----------

# MAGIC %run "./products"

# COMMAND ----------

# MAGIC %run "./orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_year, sum(total_profit) as total_profit from gold.kpi_agg group by order_year order by order_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_year,category, sum(total_profit) as total_profit from gold.kpi_agg group by order_year,category order by order_year, category

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_year,customer_id, sum(total_profit) as total_profit from gold.kpi_agg group by order_year,customer_id order by order_year, customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, sum(total_profit) as total_profit from gold.kpi_agg group by customer_id order by  customer_id