# Databricks notebook source
"""
Silver — Clean Order Items, Payments, and Reviews.
Simpler tables — cast types, dedup, quarantine via run_dq_checks.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.data_quality import run_dq_checks, enforce_schema
from utils.schema_definitions import (
    BRONZE_META_COLS,
    ORDER_ITEMS_SILVER_SCHEMA, ORDER_PAYMENTS_SILVER_SCHEMA, ORDER_REVIEWS_SILVER_SCHEMA,
)


def clean_order_items(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean order items."""
    df = spark.table(bronze_table).drop(*BRONZE_META_COLS)

    df = (
        df
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
        .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
    )

    # DQ: dedup on composite key, quarantine nulls, log metrics
    df = run_dq_checks(
        spark, df,
        table_name="order_items",
        layer="silver",
        primary_keys=["order_id", "order_item_id"],
        not_null_columns=["order_id", "product_id", "seller_id"],
    )

    # Enforce Silver schema contract
    df = enforce_schema(df, ORDER_ITEMS_SILVER_SCHEMA)

    (
        df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
    )

    count = spark.table(silver_table).count()
    print(f"[Silver] {silver_table}: {count} rows written")
    return count


def clean_order_payments(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean order payments."""
    df = spark.table(bronze_table).drop(*BRONZE_META_COLS)

    df = (
        df
        .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.col("payment_value").cast("decimal(10,2)"))
    )

    # DQ: dedup on composite key, quarantine nulls, log metrics
    df = run_dq_checks(
        spark, df,
        table_name="order_payments",
        layer="silver",
        primary_keys=["order_id", "payment_sequential"],
        not_null_columns=["order_id"],
    )

    # Enforce Silver schema contract
    df = enforce_schema(df, ORDER_PAYMENTS_SILVER_SCHEMA)

    (
        df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
    )

    count = spark.table(silver_table).count()
    print(f"[Silver] {silver_table}: {count} rows written")
    return count


def clean_order_reviews(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean order reviews."""
    df = spark.table(bronze_table).drop(*BRONZE_META_COLS)

    df = (
        df
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
    )

    # DQ: dedup on review_id, quarantine nulls, log metrics
    df = run_dq_checks(
        spark, df,
        table_name="order_reviews",
        layer="silver",
        primary_keys=["review_id"],
        not_null_columns=["review_id", "order_id"],
    )

    # Enforce Silver schema contract
    df = enforce_schema(df, ORDER_REVIEWS_SILVER_SCHEMA)

    (
        df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
    )

    count = spark.table(silver_table).count()
    print(f"[Silver] {silver_table}: {count} rows written")
    return count
