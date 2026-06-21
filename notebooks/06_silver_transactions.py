# Databricks notebook source
bronze_stream_df = (
    spark.readStream
    .format("delta")
    .load("/Volumes/workspace/default/data/bronze/transactions")
)

# COMMAND ----------

from pyspark.sql.functions import *

silver_stream_df = (
    bronze_stream_df
    .groupBy("customer_id")
    .agg(
        count("*").alias("transaction_count"),

        round(
            sum("transaction_amount"),2
        ).alias("total_transaction_amount"),

        round(
            avg("transaction_amount"),2
        ).alias("avg_transaction_amount"),

        max("transaction_timestamp")
        .alias("last_transaction_time")
    )
)

# COMMAND ----------

dbutils.fs.mkdirs(
    "/Volumes/workspace/default/data/silver/checkpoints/transaction_metrics"
)

# COMMAND ----------

import time

while True:

    query = (
        silver_stream_df.writeStream
        .format("delta")
        .outputMode("complete")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            "/Volumes/workspace/default/data/silver/checkpoints/transaction_metrics"
        )
        .start("/Volumes/workspace/default/data/silver/transactions")
    )

    query.awaitTermination()

    time.sleep(5)

# COMMAND ----------

