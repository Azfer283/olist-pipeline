# Databricks notebook source
"""
Gold layer builders — all five aggregation functions in one module.
Each takes a SparkSession and a target table name, builds the aggregate, and writes it.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.schema_definitions import SILVER_TABLES


def _assert_gold_quality(spark: SparkSession, gold_table: str, min_rows: int = 1):
    """Post-write Gold DQ: verify table is non-empty and has no all-null rows."""
    count = spark.table(gold_table).count()
    if count < min_rows:
        raise RuntimeError(
            f"Gold DQ failed: {gold_table} has {count} rows (expected >= {min_rows})"
        )
    return count


def _write_delta(spark, df, table, partition_by=None):
    """CTAS write — avoids DataFrameWriter V2 OverwriteByExpression truncate issue."""
    df.createOrReplaceTempView("_delta_write_tmp")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    spark.sql(f"CREATE TABLE {table} USING DELTA {partition_clause} AS SELECT * FROM _delta_write_tmp")


def build_daily_sales_summary(spark: SparkSession, gold_table: str):
    """Build daily sales summary from Silver tables."""

    orders = spark.table(SILVER_TABLES["orders"])
    items = spark.table(SILVER_TABLES["order_items"])
    products = spark.table(SILVER_TABLES["products"])
    customers = spark.table(SILVER_TABLES["customers"])

    # Join orders → items → products + customers
    df = (
        orders
        .join(items, "order_id", "inner")
        .join(products, "product_id", "left")
        .join(customers, "customer_id", "left")
    )

    # Only delivered orders
    df = df.filter(F.col("order_status") == "delivered")

    # Step 1: Sum revenue per order within each group
    df = df.withColumn("order_date", F.to_date("order_purchase_timestamp"))

    order_totals = (
        df
        .groupBy(
            "order_date",
            "product_category_name_english",
            "customer_state",
            "order_id",
        )
        .agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("order_revenue"),
            F.sum("price").alias("order_product_revenue"),
            F.sum("freight_value").alias("order_freight_revenue"),
        )
    )

    # Step 2: Aggregate per-order totals into group-level metrics
    summary = (
        order_totals
        .groupBy(
            "order_date",
            "product_category_name_english",
            "customer_state",
        )
        .agg(
            F.sum("order_revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("order_revenue").alias("avg_order_value"),
            F.sum("order_product_revenue").alias("product_revenue"),
            F.sum("order_freight_revenue").alias("freight_revenue"),
        )
        .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
    )

    _write_delta(spark, summary, gold_table, partition_by="year_month")

    count = _assert_gold_quality(spark, gold_table)
    print(f"[Gold] {gold_table}: {count} rows")
    return count


def build_customer_ltv(spark: SparkSession, gold_table: str):
    """Build customer LTV table."""

    orders = spark.table(SILVER_TABLES["orders"])
    items = spark.table(SILVER_TABLES["order_items"])
    reviews = spark.table(SILVER_TABLES["order_reviews"])
    customers = spark.table(SILVER_TABLES["customers"])

    # Total spend per order
    order_totals = (
        items
        .groupBy("order_id")
        .agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("order_total"),
            F.count("order_item_id").alias("items_per_order"),
        )
    )

    # Deduplicate reviews to one score per order (multiple reviews per order
    # exist in Silver; joining without dedup fans out rows and inflates spend)
    order_reviews = (
        reviews
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )

    # Join everything
    df = (
        orders
        .join(order_totals, "order_id", "inner")
        .join(order_reviews, "order_id", "left")
        .join(customers, "customer_id", "inner")
    )

    # Only completed orders
    df = df.filter(F.col("order_status") == "delivered")

    # Aggregate per customer
    ltv = (
        df
        .groupBy(
            "customer_unique_id",
            "customer_state",
            "customer_city",
        )
        .agg(
            F.sum("order_total").alias("total_spend"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("review_score").alias("avg_review_score"),
            F.min("order_purchase_timestamp").alias("first_order_date"),
            F.max("order_purchase_timestamp").alias("last_order_date"),
            F.avg("order_total").alias("avg_order_value"),
            F.sum("items_per_order").alias("total_items_purchased"),
        )
    )

    _write_delta(spark, ltv, gold_table)

    count = _assert_gold_quality(spark, gold_table)
    print(f"[Gold] {gold_table}: {count} rows")
    return count


def build_seller_performance(spark: SparkSession, gold_table: str):
    """Build seller performance table."""

    orders = spark.table(SILVER_TABLES["orders"])
    items = spark.table(SILVER_TABLES["order_items"])
    reviews = spark.table(SILVER_TABLES["order_reviews"])
    sellers = spark.table(SILVER_TABLES["sellers"])

    # Join
    df = (
        items
        .join(orders, "order_id", "inner")
        .join(reviews.select("order_id", "review_score"), "order_id", "left")
        .join(sellers, "seller_id", "inner")
    )

    df = df.filter(F.col("order_status") == "delivered")

    # Calculate delivery metrics
    df = df.withColumn(
        "delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
    )
    df = df.withColumn(
        "estimated_delivery_days",
        F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"),
    )
    df = df.withColumn(
        "on_time",
        F.when(F.col("order_delivered_customer_date").isNull(), None)
        .when(
            F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), 1
        ).otherwise(0),
    )

    # Aggregate per seller
    performance = (
        df
        .groupBy(
            "seller_id",
            "seller_city",
            "seller_state",
        )
        .agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("total_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("on_time").alias("on_time_delivery_rate"),
            F.avg("review_score").alias("avg_review_score"),
            F.count("order_item_id").alias("total_items_sold"),
        )
    )

    _write_delta(spark, performance, gold_table)

    count = _assert_gold_quality(spark, gold_table)
    print(f"[Gold] {gold_table}: {count} rows")
    return count


def build_order_fulfillment(spark: SparkSession, gold_table: str):
    """Build order fulfillment metrics."""

    orders = spark.table(SILVER_TABLES["orders"])

    df = orders.filter(F.col("order_status") == "delivered")

    # Calculate time deltas (in hours)
    fulfillment = (
        df
        .withColumn(
            "approval_time_hours",
            (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")) / 3600,
        )
        .withColumn(
            "carrier_pickup_time_hours",
            (F.unix_timestamp("order_delivered_carrier_date") - F.unix_timestamp("order_approved_at")) / 3600,
        )
        .withColumn(
            "delivery_time_days",
            F.datediff("order_delivered_customer_date", "order_delivered_carrier_date"),
        )
        .withColumn(
            "total_time_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
        )
        .withColumn(
            "estimated_vs_actual_days",
            F.datediff("order_estimated_delivery_date", "order_delivered_customer_date"),
        )
        .withColumn(
            "delivered_early",
            F.when(F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), 1).otherwise(0),
        )
        .withColumn(
            "year_month",
            F.date_format("order_purchase_timestamp", "yyyy-MM"),
        )
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "approval_time_hours",
            "carrier_pickup_time_hours",
            "delivery_time_days",
            "total_time_days",
            "estimated_vs_actual_days",
            "delivered_early",
            "year_month",
        )
    )

    _write_delta(spark, fulfillment, gold_table, partition_by="year_month")

    count = _assert_gold_quality(spark, gold_table)
    print(f"[Gold] {gold_table}: {count} rows")
    return count


def build_product_category_performance(spark: SparkSession, gold_table: str):
    """Build product category performance table."""

    items = spark.table(SILVER_TABLES["order_items"])
    products = spark.table(SILVER_TABLES["products"])
    orders = spark.table(SILVER_TABLES["orders"])
    reviews = spark.table(SILVER_TABLES["order_reviews"])

    df = (
        items
        .join(products, "product_id", "inner")
        .join(orders, "order_id", "inner")
        .join(reviews.select("order_id", "review_score"), "order_id", "left")
    )

    df = df.filter(F.col("order_status") == "delivered")

    performance = (
        df
        .groupBy("product_category_name_english")
        .agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("total_revenue"),
            F.sum("freight_value").alias("total_freight"),
            F.countDistinct("order_id").alias("total_orders"),
            F.count("order_item_id").alias("total_items_sold"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("price").alias("avg_item_price"),
            F.countDistinct("seller_id").alias("unique_sellers"),
            F.countDistinct("product_id").alias("unique_products"),
        )
        .withColumn(
            "revenue_per_order",
            F.col("total_revenue") / F.col("total_orders"),
        )
    )

    _write_delta(spark, performance, gold_table)

    count = _assert_gold_quality(spark, gold_table)
    print(f"[Gold] {gold_table}: {count} rows")
    return count
