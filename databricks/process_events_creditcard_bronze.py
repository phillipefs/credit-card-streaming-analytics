# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Process Events CreditCard Bronze
# MAGIC > *Process events cc bronze* <br>
# MAGIC > *Documentation and References: https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md*
# MAGIC <br>

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
import json 

connectionString = dbutils.secrets.get(scope="kv-databricks", key="endpoint-eventhub")

# Start from beginning of stream
startOffset = "-1"

# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

event_hub_configs = {
    "eventhubs.connectionString":sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
    "eventhubs.consumerGroup":"$Default"
}

event_hub_configs["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

df_events_cc = spark.readStream.format("eventhubs").options(**event_hub_configs).load()

json_schema = StructType(
    [
        StructField("username", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("credit_card_number", StringType(), True),
        StructField("credit_card_provider", StringType(), True),
        StructField("credit_card_expiration", StringType(), True),
        StructField("purchase_amount", StringType(), True)
    ]
)

df_events_cc = (
    df_events_cc
    .withColumn("body", F.from_json(df_events_cc.body.cast("string"), json_schema))
    .select(
        F.col("body.username").alias("username"),
        F.col("body.name").alias("name"),
        F.col("body.email").alias("email"),
        F.col("body.city").alias("city"),
        F.col("body.country").alias("country"),
        F.col("body.credit_card_number").alias("credit_card_number"),
        F.col("body.credit_card_provider").alias("credit_card_provider"),
        F.col("body.credit_card_expiration").alias("credit_card_expiration"),
        F.col("body.purchase_amount").alias("purchase_amount"), 
        (F.col("enqueuedTime") - F.expr("INTERVAL 3 HOURS")).alias("event_timestamp")

    )
)
display(df_events_cc)



# df_events_cc.writeStream.format("delta")\
#   .option("checkpointLocation", "/mnt/landing/checkpoint_events_cc")\
#   .option("outputMode", "append")\
#   .toTable("analytics_bronze.transactions_credit_card")
