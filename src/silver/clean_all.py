# Databricks notebook source
"""
Silver Orchestrator — Runs all Silver cleaning jobs in order.
"""

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(globals().get("__file__", "")), ".."))

from pyspark.sql import SparkSession
from utils.spark_utils import setup_databases
from utils.schema_definitions import BRONZE_TABLES, SILVER_TABLES
from silver.clean_orders import clean_orders
from silver.clean_geo_entities import clean_customers, clean_sellers
from silver.clean_products import clean_products
from silver.clean_order_tables import (
    clean_order_items, clean_order_payments, clean_order_reviews
)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
setup_databases(spark)

# Create quarantine and DQ log tables if not exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS olist_silver.quarantine (
        source_table STRING,
        rejection_reason STRING,
        quarantine_timestamp TIMESTAMP,
        record_json STRING
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS olist_silver.dq_log (
        table_name STRING,
        layer STRING,
        input_count LONG,
        output_count LONG,
        quarantined_count LONG,
        null_counts STRING,
        run_timestamp STRING
    ) USING DELTA
""")

# COMMAND ----------

results = {}

# Order matters — geolocation is used by customers and sellers
cleaning_jobs = [
    ("orders", clean_orders),
    ("customers", clean_customers),
    ("products", clean_products),
    ("sellers", clean_sellers),
    ("order_items", clean_order_items),
    ("order_payments", clean_order_payments),
    ("order_reviews", clean_order_reviews),
]

for table_key, clean_fn in cleaning_jobs:
    bronze = BRONZE_TABLES[table_key]
    silver = SILVER_TABLES[table_key]

    print(f"\n{'='*60}")
    print(f"Cleaning: {bronze} → {silver}")
    print(f"{'='*60}")

    try:
        count = clean_fn(spark, bronze, silver)
        results[table_key] = {"status": "SUCCESS", "count": count}
    except Exception as e:
        results[table_key] = {"status": "FAILED", "error": str(e)}
        print(f"[ERROR] {silver}: {e}")

# COMMAND ----------

# Summary
print("\n" + "="*60)
print("SILVER CLEANING SUMMARY")
print("="*60)
for table_key, result in results.items():
    status = result["status"]
    if status == "SUCCESS":
        print(f"  {table_key:30s} | {status} | {result['count']:,} rows")
    else:
        print(f"  {table_key:30s} | {status} | {result['error'][:50]}")

# Quarantine summary
quarantine_count = spark.table("olist_silver.quarantine").count()
print(f"\n  Quarantined records total: {quarantine_count}")

# DQ log summary
dq_log_count = spark.table("olist_silver.dq_log").count()
print(f"  DQ log entries: {dq_log_count}")
