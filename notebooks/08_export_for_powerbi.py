# Databricks notebook source
# ==========================================
# Helper Function
# ==========================================

def rename_part_file(folder_path, output_filename):

    files = dbutils.fs.ls(folder_path)

    # Find generated CSV
    csv_file = [f.path for f in files if f.name.startswith("part-")][0]

    # Rename to desired filename
    dbutils.fs.mv(
        csv_file,
        f"{folder_path}/{output_filename}"
    )

    # Remove _SUCCESS file if present
    for f in dbutils.fs.ls(folder_path):
        if "_SUCCESS" in f.name:
            dbutils.fs.rm(f.path)


# ==========================================
# Cleanup old exports
# ==========================================

dbutils.fs.rm(
    "/Volumes/workspace/default/data/export",
    recurse=True
)

base_path = "/Volumes/workspace/default/data/export/powerbi"


# ==========================================
# Read Tables
# ==========================================

customer_risk_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/gold/customer_risk"
)

country_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/gold/churn_by_country"
)

age_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/gold/churn_by_age_group"
)

kpi_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/gold/customer_kpis"
)

transaction_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/silver/transactions"
)

customer_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/silver/customers"
)


# ==========================================
# Customer Risk
# ==========================================

(customer_risk_df.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(f"{base_path}/customer_risk"))

rename_part_file(
    f"{base_path}/customer_risk",
    "customer_risk.csv"
)

print("✅ customer_risk.csv exported")


# ==========================================
# Customer KPIs
# ==========================================

(kpi_df.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(f"{base_path}/customer_kpis"))

rename_part_file(
    f"{base_path}/customer_kpis",
    "customer_kpis.csv"
)

print("✅ customer_kpis.csv exported")


# ==========================================
# Customer Master
# ==========================================

(customer_df.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(f"{base_path}/customers"))

rename_part_file(
    f"{base_path}/customers",
    "customers.csv"
)

print("✅ customers.csv exported")


# ==========================================
# Transactions
# ==========================================

(transaction_df.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(f"{base_path}/transactions"))

rename_part_file(
    f"{base_path}/transactions",
    "transactions.csv"
)

print("✅ transactions.csv exported")


# ==========================================
# Churn By Country
# ==========================================

(country_df.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(f"{base_path}/churn_by_country"))

rename_part_file(
    f"{base_path}/churn_by_country",
    "churn_by_country.csv"
)

print("✅ churn_by_country.csv exported")


# ==========================================
# Churn By Age Group
# ==========================================

(age_df.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(f"{base_path}/churn_by_age_group"))

rename_part_file(
    f"{base_path}/churn_by_age_group",
    "churn_by_age_group.csv"
)

print("✅ churn_by_age_group.csv exported")


print("\n🎉 Power BI export completed successfully!")