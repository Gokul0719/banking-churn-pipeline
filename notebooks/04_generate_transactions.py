# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import random

silver_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/silver/customers"
)

customer_ids = [
    row.customer_id
    for row in silver_df.select("customer_id").collect()
]

customer_id = random.choice(customer_ids)

# COMMAND ----------

from faker import Faker
from datetime import datetime
import pandas as pd
import random
import time
import uuid

fake = Faker()

while True:

    transactions = []

    for i in range(50):

        transaction_type = random.choice(
            ["Deposit", "Withdrawal", "Transfer", "Payment"]
        )

        # Realistic amount ranges
        if transaction_type == "Deposit":
            amount = round(random.uniform(100, 50000), 2)

        elif transaction_type == "Withdrawal":
            amount = round(random.uniform(50, 20000), 2)

        elif transaction_type == "Transfer":
            amount = round(random.uniform(100, 100000), 2)

        else:
            amount = round(random.uniform(50, 5000), 2)

        transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": random.choice(customer_ids),
            "transaction_amount": amount,
            "transaction_type": transaction_type,
            "channel": random.choice(
                [
                    "Mobile",
                    "ATM",
                    "Branch",
                    "Internet Banking"
                ]
            ),

            # Actual transaction time
            "transaction_timestamp": datetime.now(),

            # When we ingested the event
            "ingestion_timestamp": datetime.now()
        })

    pdf = pd.DataFrame(transactions)

    spark_df = spark.createDataFrame(pdf)

    spark_df.coalesce(1).write \
        .mode("append") \
        .option("header", True) \
        .csv("/Volumes/workspace/default/data/raw/transactions/")

    print(f"Generated 50 transactions at {datetime.now()}")

    time.sleep(10)

# COMMAND ----------

