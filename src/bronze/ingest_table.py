# Databricks notebook source
"""
Bronze Ingestion — Generic loader for all Olist source CSVs.
Loads CSV with explicit schema, adds metadata columns, writes as Delta.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import posixpath


def ingest_table(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    schema,
):
    """
    Load a CSV file into a Bronze Delta table.

    Args:
        spark: SparkSession
        source_path: Path to the CSV file
        table_name: Full table name (e.g., olist_bronze.orders)
        schema: StructType schema for the CSV
    """
    allowed_prefixes = ("/FileStore/olist/", "dbfs:/FileStore/olist/", "/tmp/")
    normalised = posixpath.normpath(source_path)
    if not any(normalised.startswith(posixpath.normpath(p)) for p in allowed_prefixes):
        raise ValueError(
            f"source_path '{source_path}' is outside allowed prefixes: {allowed_prefixes}"
        )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .csv(source_path)
    )

    if df.head(1) == []:
        raise ValueError(f"Source CSV is empty or missing: {source_path}")

    # Add metadata columns
    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"))
        .withColumn("_ingestion_date", F.current_date())
    )

    # Write as Delta — full overwrite on first load
    (
        df.write
        .mode("overwrite")
        .format("delta")
        .partitionBy("_ingestion_date")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    # Bronze DQ: verify row count survived write (catch silent truncation)
    written_count = spark.table(table_name).count()
    if written_count == 0:
        raise RuntimeError(f"Bronze DQ failed: {table_name} has 0 rows after write")

    print(f"[Bronze] {table_name}: {written_count} rows ingested")
    return written_count
