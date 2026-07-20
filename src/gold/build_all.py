# Databricks notebook source
"""
Gold Orchestrator — Builds all Gold aggregation tables.
"""

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(globals().get("__file__", "")), ".."))

from pyspark.sql import SparkSession
from utils.spark_utils import setup_databases
from utils.schema_definitions import GOLD_TABLES
from gold.builders import (
    build_daily_sales_summary,
    build_customer_ltv,
    build_seller_performance,
    build_order_fulfillment,
    build_product_category_performance,
)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
setup_databases(spark)

# COMMAND ----------

results = {}

gold_jobs = [
    ("daily_sales_summary", build_daily_sales_summary),
    ("customer_ltv", build_customer_ltv),
    ("seller_performance", build_seller_performance),
    ("order_fulfillment_metrics", build_order_fulfillment),
    ("product_category_performance", build_product_category_performance),
]

for table_key, build_fn in gold_jobs:
    gold_table = GOLD_TABLES[table_key]

    print(f"\n{'='*60}")
    print(f"Building: {gold_table}")
    print(f"{'='*60}")

    try:
        count = build_fn(spark, gold_table)
        results[table_key] = {"status": "SUCCESS", "count": count}
    except Exception as e:
        results[table_key] = {"status": "FAILED", "error": str(e)}
        print(f"[ERROR] {gold_table}: {e}")

# COMMAND ----------

# Summary
print("\n" + "="*60)
print("GOLD BUILD SUMMARY")
print("="*60)
for table_key, result in results.items():
    status = result["status"]
    if status == "SUCCESS":
        print(f"  {table_key:35s} | {status} | {result['count']:,} rows")
    else:
        print(f"  {table_key:35s} | {status} | {result['error'][:50]}")
