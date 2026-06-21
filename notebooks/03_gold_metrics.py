# Databricks notebook source
silver_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/silver/customers"
)

# COMMAND ----------

from pyspark.sql.functions import *

gold_df = silver_df.agg(
    count("*").alias("total_customers"),
    sum("churned").alias("churned_customers"),
    sum("is_active_member").alias("active_customers"),
    avg("balance").alias("avg_balance"),
    avg("credit_score").alias("avg_credit_score"),
    avg("estimated_salary").alias("avg_salary")
)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/data/gold/customer_kpis")

# COMMAND ----------

country_df = (
    silver_df
    .groupBy("country")
    .agg(
        count("*").alias("total_customers"),
        sum("churned").alias("churned_customers")
    )
)

# COMMAND ----------

country_df = country_df.withColumn(
    "churn_rate",
    round(
        col("churned_customers") * 100 / col("total_customers"),
        2
    )
)

display(country_df)

# COMMAND ----------

country_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/data/gold/churn_by_country")

# COMMAND ----------

age_df = (
    silver_df
    .groupBy("age_group")
    .agg(
        count("*").alias("total_customers"),
        sum("churned").alias("churned_customers")
    )
)

age_df = age_df.withColumn(
    "churn_rate",
    round(
        col("churned_customers") * 100 / col("total_customers"),
        2
    )
)

display(age_df)

# COMMAND ----------

age_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/workspace/default/data/gold/churn_by_age_group")

# COMMAND ----------

