# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create Databases and Delta Tables
# MAGIC > *DB and Delta Tables*
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Databases
# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_bronze LOCATION "/mnt/layer-bronze/databricks";
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_silver LOCATION "/mnt/layer-silver/databricks";
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_gold LOCATION "/mnt/layer-gold/databricks";

# COMMAND ----------

# MAGIC %sql
# MAGIC create table analytics_bronze.teste ( id int) location "/mnt/layer-bronze/databricks/teste";

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into analytics_bronze.teste values(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases
