# Databricks notebook source
# Unity Catalog Volume paths (replace 'main' and 'default' with your catalog and schema)
catalog = "workspace"
schema = "default"
volume = "data"

# Create volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# Create folder structure in the volume
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/raw/customers")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/raw/transactions")

dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/bronze")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/silver")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/{volume}/gold")

# COMMAND ----------

dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/{volume}/raw/customers")

# COMMAND ----------

customer_df = spark.read.csv(f"/Volumes/{catalog}/{schema}/{volume}/raw/customers", header=True, inferSchema=True)
customer_df.display()

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

customer_df.count()

# COMMAND ----------

customer_df.write.format("delta").save(f"/Volumes/{catalog}/{schema}/{volume}/bronze/customers")

# COMMAND ----------

bronze_df = spark.read.format("delta").load(f"/Volumes/{catalog}/{schema}/{volume}/bronze/customers")
bronze_df.display()

# COMMAND ----------

