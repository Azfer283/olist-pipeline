# Databricks notebook source
"""
Silver — Clean Products.
Casts numeric columns, translates category names Portuguese → English.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.data_quality import run_dq_checks, enforce_schema
from utils.schema_definitions import BRONZE_TABLES, BRONZE_META_COLS, PRODUCTS_SILVER_SCHEMA


def clean_products(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean products and translate category names."""
    df = spark.table(bronze_table).drop(*BRONZE_META_COLS)

    # Cast numeric columns
    int_cols = [
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ]
    for col in int_cols:
        df = df.withColumn(col, F.col(col).cast("int"))

    # Fix typos in column names (lenght → length)
    df = (
        df
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
    )

    # Join with category translation
    translation_df = spark.table(BRONZE_TABLES["category_translation"]).drop(*BRONZE_META_COLS)

    df = df.join(
        translation_df,
        on="product_category_name",
        how="left",
    )

    # DQ: dedup on product_id, quarantine nulls, log metrics
    df = run_dq_checks(
        spark, df,
        table_name="products",
        layer="silver",
        primary_keys=["product_id"],
        not_null_columns=["product_id"],
    )

    # Enforce Silver schema contract
    df = enforce_schema(df, PRODUCTS_SILVER_SCHEMA)

    # Write Silver
    (
        df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
    )

    count = spark.table(silver_table).count()
    print(f"[Silver] {silver_table}: {count} rows written")
    return count
