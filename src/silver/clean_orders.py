# Databricks notebook source
"""
Silver — Clean and transform Orders.
Casts types, deduplicates, quarantines nulls, adds year_month partition column.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.data_quality import run_dq_checks, enforce_schema
from utils.schema_definitions import BRONZE_META_COLS, ORDERS_SILVER_SCHEMA


def clean_orders(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean orders from Bronze → Silver."""
    df = spark.table(bronze_table)

    # Drop metadata columns from bronze
    df = df.drop(*BRONZE_META_COLS)

    # Cast timestamps
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in timestamp_cols:
        df = df.withColumn(col, F.to_timestamp(col))

    # Add partition column
    df = df.withColumn(
        "order_purchase_year_month",
        F.date_format("order_purchase_timestamp", "yyyy-MM"),
    )

    # DQ: dedup on order_id, quarantine nulls, log metrics
    df = run_dq_checks(
        spark, df,
        table_name="orders",
        layer="silver",
        primary_keys=["order_id"],
        not_null_columns=["order_id", "customer_id", "order_purchase_timestamp"],
    )

    # Enforce Silver schema contract
    df = enforce_schema(df, ORDERS_SILVER_SCHEMA)

    # Write Silver
    (
        df.write
        .mode("overwrite")
        .format("delta")
        .partitionBy("order_purchase_year_month")
        .saveAsTable(silver_table)
    )

    count = spark.table(silver_table).count()
    print(f"[Silver] {silver_table}: {count} rows written")
    return count
