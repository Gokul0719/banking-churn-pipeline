# Databricks notebook source
bronze_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/bronze/customers"
)
display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

silver_df = bronze_df.drop("RowNumber", "Surname")
display(silver_df)


# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df = (
    silver_df
    .withColumnRenamed("CustomerId", "customer_id")
    .withColumnRenamed("CreditScore", "credit_score")
    .withColumnRenamed("Geography", "country")
    .withColumnRenamed("Gender", "gender")
    .withColumnRenamed("Age", "age")
    .withColumnRenamed("Tenure", "tenure")
    .withColumnRenamed("Balance", "balance")
    .withColumnRenamed("NumOfProducts", "num_products")
    .withColumnRenamed("HasCrCard", "has_credit_card")
    .withColumnRenamed("IsActiveMember", "is_active_member")
    .withColumnRenamed("EstimatedSalary", "estimated_salary")
    .withColumnRenamed("Exited", "churned")
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df = silver_df.withColumn("age_group",
                                 when(col("age")<30, "Young")
                                 .when(col("age")<=50, "Middle Age")
                                 .otherwise("Senior")
)

display(silver_df.limit(5))

# COMMAND ----------

silver_df = silver_df.withColumn(
    "balance_category",
    when(col("balance") == 0, "Zero")
    .when(col("balance") < 50000, "Low")
    .when(col("balance") < 100000, "Medium")
    .otherwise("High")
)

display(silver_df.limit(5))

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/data/silver/customers")

# COMMAND ----------

display(
    spark.read.format("delta").load(
        "/Volumes/workspace/default/data/silver/customers"
    )
)

# COMMAND ----------

silver_df.printSchema()


# COMMAND ----------

