# Databricks notebook source
customer_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/silver/customers"
)

txn_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/silver/transactions"
)

gold_df = customer_df.join(
    txn_df,
    on="customer_id",
    how="left"
)

# COMMAND ----------

gold_df = gold_df.fillna({
    "transaction_count": 0,
    "total_transaction_amount": 0,
    "avg_transaction_amount": 0
})

# COMMAND ----------

from pyspark.sql.functions import *

gold_df = gold_df.withColumn(
    "high_value_customer",
    when(
        (col("balance") > 100000) &
        (col("transaction_count") >= 5),
        "Yes"
    ).otherwise("No")
)

# COMMAND ----------

gold_df = gold_df.withColumn(
    "low_activity_customer",
    when(
        col("transaction_count") < 2,
        "Yes"
    ).otherwise("No")
)

# COMMAND ----------

gold_df = gold_df.withColumn(
    "premium_customer",
    when(
        (col("balance") > 150000) &
        (col("transaction_count") >= 10),
        "Yes"
    ).otherwise("No")
)

# COMMAND ----------

gold_df = gold_df.withColumn(
    "high_risk_customer",
    when(
        (col("is_active_member") == 0) &
        (col("balance") < 50000) &
        (col("transaction_count") < 2),
        "Yes"
    ).otherwise("No")
)

# COMMAND ----------

gold_df = gold_df.withColumn(
    "risk_score",
    (
        when(col("balance") < 50000, 30).otherwise(0)
        +
        when(col("is_active_member") == 0, 25).otherwise(0)
        +
        when(col("credit_score") < 600, 20).otherwise(0)
        +
        when(col("transaction_count") < 2, 25).otherwise(0)
    )
)

# COMMAND ----------

gold_df = gold_df.withColumn(
    "risk_category",
    when(col("risk_score") >= 70, "High")
    .when(col("risk_score") >= 40, "Medium")
    .otherwise("Low")
)

# COMMAND ----------

gold_df = gold_df.withColumn(
    "customer_segment",
    when(
        (col("balance") > 150000) &
        (col("transaction_count") >= 10),
        "Premium"
    )
    .when(
        (col("balance") > 100000) &
        (col("transaction_count") >= 5),
        "High Value"
    )
    .when(
        col("risk_category") == "High",
        "High Risk"
    )
    .when(
        col("transaction_count") < 2,
        "Low Activity"
    )
    .otherwise("Standard")
)

# COMMAND ----------

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/data/gold/customer_risk")

# COMMAND ----------

