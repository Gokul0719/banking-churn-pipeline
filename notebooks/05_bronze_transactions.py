# Databricks notebook source
transaction_schema = """
transaction_id STRING,
customer_id LONG,
transaction_amount DOUBLE,
transaction_type STRING,
channel STRING,
transaction_timestamp TIMESTAMP,
ingestion_timestamp TIMESTAMP
"""

# COMMAND ----------

stream_df = (
    spark.readStream
         .schema(transaction_schema)
         .option("header", True)
         .csv("/Volumes/workspace/default/data/raw/transactions/")
)

# COMMAND ----------

import time

while True:

    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            "/Volumes/workspace/default/data/bronze/checkpoints/transaction_stream"
        )
        .start("/Volumes/workspace/default/data/bronze/transactions")
    )

    query.awaitTermination()

    print("Consumer sleeping...")
    time.sleep(7)

# COMMAND ----------

