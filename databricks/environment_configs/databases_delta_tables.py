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
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_bronze LOCATION "/mnt/layer-bronze/databricks/analytics_bronze";
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_silver LOCATION "/mnt/layer-silver/databricks/analytics_silver";
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_gold   LOCATION "/mnt/layer-gold/databricks/analytics_gold";

# COMMAND ----------

# DBTITLE 1,Create Delta Table Raw
# MAGIC %sql
# MAGIC create table analytics_bronze.cc_transactions(
# MAGIC   username STRING,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   credit_card_number STRING,
# MAGIC   credit_card_provider STRING,
# MAGIC   credit_card_expiration STRING,
# MAGIC   purchase_amount STRING,
# MAGIC   event_timestamp STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/layer-bronze/databricks/analytics_bronze/cc_transactions";

# COMMAND ----------

# DBTITLE 1,Create Delta Table Silver
# MAGIC %sql
# MAGIC create table analytics_silver.cc_transactions(
# MAGIC   id_transaction bigint generated always as identity(start with 1 increment by 1),
# MAGIC   transaction_hash string,
# MAGIC   username STRING,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   credit_card_number STRING,
# MAGIC   credit_card_provider STRING,
# MAGIC   credit_card_expiration STRING,
# MAGIC   purchase_amount FLOAT,
# MAGIC   event_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/layer-silver/databricks/analytics_silver/cc_transactions";
