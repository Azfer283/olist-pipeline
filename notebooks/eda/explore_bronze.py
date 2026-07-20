# Databricks notebook source
"""
Exploratory Data Analysis — Olist E-Commerce Dataset.
Run after Bronze ingestion to understand the data before building Silver/Gold.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Olist EDA
# MAGIC Quick exploration of the Bronze layer to understand:
# MAGIC - Row counts per table
# MAGIC - Key distributions (order status, payment types, states)
# MAGIC - Date ranges
# MAGIC - Null rates on critical columns

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(globals().get("__file__", "")), "..", "..", "src"))

from pyspark.sql import SparkSession
from utils.schema_definitions import BRONZE_TABLES

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table Row Counts

# COMMAND ----------

for name, table in BRONZE_TABLES.items():
    try:
        count = spark.table(table).count()
        print(f"  {name:25s} | {count:>10,} rows")
    except Exception:
        print(f"  {name:25s} | NOT FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Orders — Status Distribution & Date Range

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(BRONZE_TABLES["orders"])

# Order status distribution
orders.groupBy("order_status").count().orderBy(F.desc("count")).show()

# Date range
orders.select(
    F.min("order_purchase_timestamp").alias("earliest_order"),
    F.max("order_purchase_timestamp").alias("latest_order"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Null Rate Analysis

# COMMAND ----------

# Check null rates on key columns across all tables
for name, table in BRONZE_TABLES.items():
    try:
        df = spark.table(table)
        total = df.count()
        if total == 0:
            continue
        print(f"\n--- {name} ({total:,} rows) ---")
        for col_name in df.columns:
            if col_name.startswith("_"):
                continue
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                pct = (null_count / total) * 100
                print(f"  {col_name:40s} | {null_count:>10,} nulls ({pct:.1f}%)")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Payment Types & Top States

# COMMAND ----------

# Payment types
payments = spark.table(BRONZE_TABLES["order_payments"])
payments.groupBy("payment_type").count().orderBy(F.desc("count")).show()

# Top 10 customer states
customers = spark.table(BRONZE_TABLES["customers"])
customers.groupBy("customer_state").count().orderBy(F.desc("count")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Duplicate Analysis (Pre-Silver)

# COMMAND ----------

# Check for duplicates on expected primary keys
dup_checks = {
    "orders": (BRONZE_TABLES["orders"], ["order_id"]),
    "customers": (BRONZE_TABLES["customers"], ["customer_id"]),
    "products": (BRONZE_TABLES["products"], ["product_id"]),
    "sellers": (BRONZE_TABLES["sellers"], ["seller_id"]),
    "order_items": (BRONZE_TABLES["order_items"], ["order_id", "order_item_id"]),
    "order_reviews": (BRONZE_TABLES["order_reviews"], ["review_id"]),
}

for name, (table, keys) in dup_checks.items():
    try:
        df = spark.table(table)
        total = df.count()
        distinct = df.dropDuplicates(keys).count()
        dupes = total - distinct
        print(f"  {name:20s} | total: {total:>10,} | distinct: {distinct:>10,} | dupes: {dupes:>10,}")
    except Exception:
        print(f"  {name:20s} | NOT FOUND")
