# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Process Events CreditCard Silver
# MAGIC > *Process events cc silver* <br>
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

df_credit_card_raw = spark.readStream.format("delta").table("analytics_bronze.cc_transactions")

# COMMAND ----------

cols = df_credit_card_raw.columns
df_credit_card_raw = df_credit_card_raw.withColumn("transaction_hash", md5(concat(*cols)))\
    .select(
        col("transaction_hash"),
        col("username"),
        col("name"),
        col("email"),
        col("city"),
        col("country"),
        col("credit_card_number"),
        col("credit_card_provider"),
        col("credit_card_expiration"),
        col("purchase_amount").cast("float").alias("purchase_amount"), 
        current_timestamp().alias("event_timestamp")
    )

# COMMAND ----------

# DBTITLE 1,Upsert Silver
def upsert_delta(df_micro_batch, batch_id):
    df_micro_batch.createOrReplaceTempView("source")
    df_micro_batch.sparkSession.sql("""
        MERGE INTO analytics_silver.cc_transactions target
        USING source
        ON source.transaction_hash = target.transaction_hash
        WHEN NOT MATCHED THEN INSERT (transaction_hash, username, name, email, city, country, credit_card_number, credit_card_provider, credit_card_expiration, purchase_amount, event_timestamp) 
        VALUES (
          source.transaction_hash, source.username, source.name, source.email,
          source.city, source.country, source.credit_card_number,
          source.credit_card_provider, source.credit_card_expiration,
          source.purchase_amount, source.event_timestamp
        )
""")

(df_credit_card_raw.writeStream
  .format("delta")
  .foreachBatch(upsert_delta)
  .outputMode("append")
  .start()
)
