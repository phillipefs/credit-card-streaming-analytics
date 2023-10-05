# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Process Events CreditCard Gold
# MAGIC > *Process events cc gold* <br>
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

df_cc = spark.readStream.format("delta").table("analytics_silver.cc_transactions")
                

# COMMAND ----------

df_cc.groupBy("country").count()\
    .writeStream\
    .option("checkpointLocation", "/mnt/layer-gold/databricks/analytics_gold/checkpoint_events/country_orders_quantity")\
    .outputMode("complete")\
    .toTable("analytics_gold.country_orders_quantity")



# COMMAND ----------

df_cc.groupBy("credit_card_provider").count()\
    .writeStream\
    .option("checkpointLocation", "/mnt/layer-gold/databricks/analytics_gold/checkpoint_events/provider_orders_quantity")\
    .outputMode("complete")\
    .toTable("analytics_gold.provider_orders_quantity")
