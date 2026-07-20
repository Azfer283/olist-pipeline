# Databricks notebook source
"""
Silver — Clean geo-enriched entities (customers, sellers).
Both follow the same pattern: drop meta → geo join → rename lat/lng → DQ → schema → write.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.data_quality import run_dq_checks, enforce_schema
from utils.schema_definitions import BRONZE_META_COLS
from utils.spark_utils import get_geo_lookup


def clean_geo_entity(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    schema,
    entity: str,
    primary_key: str,
):
    """Clean a geo-enriched entity (customers or sellers) from Bronze → Silver."""
    df = spark.table(bronze_table).drop(*BRONZE_META_COLS)

    # Geolocation enrichment
    geo_df = get_geo_lookup(spark)
    df = df.join(
        geo_df,
        df[f"{entity}_zip_code_prefix"] == geo_df.geolocation_zip_code_prefix,
        "left",
    )
    df = (
        df
        .withColumn(f"{entity}_lat", F.col("geo_lat"))
        .withColumn(f"{entity}_lng", F.col("geo_lng"))
        .drop("geolocation_zip_code_prefix", "geo_lat", "geo_lng")
    )

    # DQ checks
    df = run_dq_checks(
        spark, df,
        table_name=entity + "s",
        layer="silver",
        primary_keys=[primary_key],
        not_null_columns=[primary_key],
    )

    # Enforce Silver schema contract
    df = enforce_schema(df, schema)

    # Write Silver — CTAS avoids DataFrameWriter V2 OverwriteByExpression issue
    df.createOrReplaceTempView("_delta_write_tmp")
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    spark.sql(f"CREATE TABLE {silver_table} USING DELTA AS SELECT * FROM _delta_write_tmp")

    count = spark.table(silver_table).count()
    print(f"[Silver] {silver_table}: {count} rows written")
    return count


def clean_customers(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean customers and enrich with geolocation."""
    from utils.schema_definitions import CUSTOMERS_SILVER_SCHEMA
    return clean_geo_entity(spark, bronze_table, silver_table, CUSTOMERS_SILVER_SCHEMA, "customer", "customer_id")


def clean_sellers(spark: SparkSession, bronze_table: str, silver_table: str):
    """Clean sellers and enrich with geolocation."""
    from utils.schema_definitions import SELLERS_SILVER_SCHEMA
    return clean_geo_entity(spark, bronze_table, silver_table, SELLERS_SILVER_SCHEMA, "seller", "seller_id")
