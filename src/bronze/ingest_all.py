# Databricks notebook source
"""
Bronze Orchestrator — Ingests all Olist source CSVs into Bronze Delta tables.
Run this notebook to load all tables in one go.
"""

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(globals().get("__file__", "")), ".."))

from pyspark.sql import SparkSession
from utils.spark_utils import setup_databases
from utils.schema_definitions import (
    RAW_SCHEMA_REGISTRY, BRONZE_TABLES, SOURCE_FILES
)
from bronze.ingest_table import ingest_table

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
setup_databases(spark)

# COMMAND ----------

# Source data path — configurable via Databricks widget or env var
try:
    dbutils.widgets.text("source_data_path", "/FileStore/olist/raw")
    SOURCE_DATA_PATH = dbutils.widgets.get("source_data_path")
except NameError:
    import os
    SOURCE_DATA_PATH = os.environ.get("OLIST_SOURCE_DATA_PATH", "/FileStore/olist/raw")

# Validate SOURCE_DATA_PATH against allowed prefixes (same as ingest_table)
import posixpath
_ALLOWED_PREFIXES = ("/FileStore/olist/", "dbfs:/FileStore/olist/", "/tmp/")
_normalised_sdp = posixpath.normpath(SOURCE_DATA_PATH)
if not any(_normalised_sdp.startswith(posixpath.normpath(p)) for p in _ALLOWED_PREFIXES):
    raise ValueError(f"SOURCE_DATA_PATH '{SOURCE_DATA_PATH}' is outside allowed prefixes")

# COMMAND ----------

results = {}

for table_key in RAW_SCHEMA_REGISTRY:
    source_file = SOURCE_FILES[table_key]
    source_path = f"{SOURCE_DATA_PATH}/{source_file}.csv"
    table_name = BRONZE_TABLES[table_key]
    schema = RAW_SCHEMA_REGISTRY[table_key]

    print(f"\n{'='*60}")
    print(f"Ingesting: {source_file} → {table_name}")
    print(f"{'='*60}")

    try:
        count = ingest_table(spark, source_path, table_name, schema)
        results[table_key] = {"status": "SUCCESS", "count": count}
    except Exception as e:
        results[table_key] = {"status": "FAILED", "error": str(e)}
        raise RuntimeError(f"Bronze ingestion failed for {table_name}: {e}") from e

# COMMAND ----------

# Summary
print("\n" + "="*60)
print("BRONZE INGESTION SUMMARY")
print("="*60)
for table_key, result in results.items():
    status = result["status"]
    if status == "SUCCESS":
        print(f"  {table_key:30s} | {status} | {result['count']:,} rows")
    else:
        print(f"  {table_key:30s} | {status} | {result['error'][:50]}")
