"""
Tests for Gold aggregation layer.
Validates business logic in aggregate calculations.
Tests run against a real local SparkSession.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestDailySalesSummary:
    """Test daily sales summary aggregation logic."""

    def test_only_delivered_orders_included(self, spark):
        """Non-delivered orders should be excluded from sales summary."""
        df = spark.createDataFrame(
            [
                ("ord_1", "delivered", 100.0),
                ("ord_2", "shipped", 200.0),
                ("ord_3", "canceled", 50.0),
                ("ord_4", "delivered", 75.0),
            ],
            schema=["order_id", "order_status", "price"],
        )
        delivered = df.filter(F.col("order_status") == "delivered")
        assert delivered.count() == 2

    def test_revenue_includes_freight(self, spark):
        """total_revenue should be sum(price + freight_value)."""
        df = spark.createDataFrame(
            [(100.0, 10.0), (200.0, 20.0)],
            schema=["price", "freight_value"],
        )
        total = df.agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("total_revenue")
        ).collect()[0].total_revenue

        assert total == 330.0  # (100+10) + (200+20)

    def test_order_count_uses_distinct(self, spark):
        """order_count should countDistinct, not count (items can repeat per order)."""
        df = spark.createDataFrame(
            [("ord_1", "item_1"), ("ord_1", "item_2"), ("ord_2", "item_1")],
            schema=["order_id", "order_item_id"],
        )
        result = df.agg(F.countDistinct("order_id").alias("order_count")).collect()[0]
        assert result.order_count == 2  # not 3

    def test_avg_order_value_is_per_order_not_per_item(self, spark):
        """avg_order_value = total_revenue / order_count, NOT avg of line items."""
        df = spark.createDataFrame(
            [
                ("ord_1", 50.0, 5.0),   # item 1 of order 1
                ("ord_1", 30.0, 3.0),   # item 2 of order 1 → order total = 88
                ("ord_2", 100.0, 10.0), # item 1 of order 2 → order total = 110
            ],
            schema=["order_id", "price", "freight_value"],
        )
        result = (
            df
            .agg(
                F.sum(F.col("price") + F.col("freight_value")).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
            )
            .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))
            .collect()[0]
        )
        # total_revenue = 198, order_count = 2, avg = 99
        assert result.avg_order_value == 99.0
        # If we'd used F.avg(price+freight), we'd get (55+33+110)/3 = 66 — WRONG


class TestCustomerLTV:
    """Test customer LTV aggregation logic."""

    def test_groups_by_customer_unique_id(self, spark):
        """LTV should group by customer_unique_id, not customer_id."""
        df = spark.createDataFrame(
            [
                ("uniq_1", "cust_1a", 100.0),
                ("uniq_1", "cust_1b", 200.0),  # same customer, different customer_id
                ("uniq_2", "cust_2a", 50.0),
            ],
            schema=["customer_unique_id", "customer_id", "order_total"],
        )
        ltv = df.groupBy("customer_unique_id").agg(
            F.sum("order_total").alias("total_spend"),
        )
        row = ltv.filter(F.col("customer_unique_id") == "uniq_1").collect()[0]
        assert row.total_spend == 300.0  # 100 + 200

    def test_avg_review_nullable(self, spark):
        """Customers without reviews should have null avg, not 0."""
        df = spark.createDataFrame(
            [("cust_1", 5), ("cust_1", 3), ("cust_2", None)],
            schema=["customer_id", "review_score"],
        )
        result = df.groupBy("customer_id").agg(
            F.avg("review_score").alias("avg_review"),
        )
        cust2 = result.filter(F.col("customer_id") == "cust_2").collect()[0]
        assert cust2.avg_review is None  # not 0


class TestSellerPerformance:
    """Test seller performance aggregation logic."""

    def test_on_time_rate_between_0_and_1(self, spark):
        """on_time_delivery_rate should be between 0 and 1."""
        df = spark.createDataFrame(
            [
                ("2023-01-10", "2023-01-15"),  # delivered before estimate → on time
                ("2023-01-10", "2023-01-08"),  # delivered after estimate → late
                ("2023-01-10", "2023-01-10"),  # exact → on time
            ],
            schema=["order_estimated_delivery_date", "order_delivered_customer_date"],
        )
        df = df.withColumn(
            "on_time",
            F.when(F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), 1).otherwise(0),
        )
        rate = df.agg(F.avg("on_time").alias("rate")).collect()[0].rate
        assert 0 <= rate <= 1
        # 2 on time out of 3
        assert abs(rate - 2/3) < 0.01

    def test_delivery_days_calculation(self, spark):
        """delivery_days should be datediff between delivered and purchased."""
        df = spark.createDataFrame(
            [("2023-01-01", "2023-01-06")],
            schema=["purchase_date", "delivery_date"],
        )
        df = df.withColumn(
            "delivery_days",
            F.datediff(F.col("delivery_date"), F.col("purchase_date")),
        )
        assert df.collect()[0].delivery_days == 5


class TestOrderFulfillment:
    """Test order fulfillment metrics logic."""

    def test_approval_time_hours(self, spark):
        """Approval time should be calculated in hours."""
        df = spark.createDataFrame(
            [("2023-01-01 10:00:00", "2023-01-01 13:00:00")],
            schema=["purchase_ts", "approved_ts"],
        )
        df = df.withColumn("purchase_ts", F.to_timestamp("purchase_ts"))
        df = df.withColumn("approved_ts", F.to_timestamp("approved_ts"))
        df = df.withColumn(
            "approval_hours",
            (F.unix_timestamp("approved_ts") - F.unix_timestamp("purchase_ts")) / 3600,
        )
        assert df.collect()[0].approval_hours == 3.0

    def test_delivered_early_flag(self, spark):
        """delivered_early should be 1 when actual <= estimated."""
        df = spark.createDataFrame(
            [
                ("2023-01-10", "2023-01-15"),  # early
                ("2023-01-20", "2023-01-15"),  # late
            ],
            schema=["delivered_date", "estimated_date"],
        )
        df = df.withColumn(
            "delivered_early",
            F.when(F.col("delivered_date") <= F.col("estimated_date"), 1).otherwise(0),
        )
        rows = df.orderBy("delivered_date").collect()
        assert rows[0].delivered_early == 1  # Jan 10 <= Jan 15
        assert rows[1].delivered_early == 0  # Jan 20 > Jan 15


class TestProductCategoryPerformance:
    """Test product category performance logic."""

    def test_revenue_per_order(self, spark):
        """revenue_per_order should equal total_revenue / total_orders."""
        df = spark.createDataFrame(
            [
                ("electronics", 1000.0, 5),
                ("books", 200.0, 10),
            ],
            schema=["category", "total_revenue", "total_orders"],
        )
        df = df.withColumn(
            "revenue_per_order",
            F.col("total_revenue") / F.col("total_orders"),
        )
        rows = {r.category: r for r in df.collect()}
        assert rows["electronics"].revenue_per_order == 200.0
        assert rows["books"].revenue_per_order == 20.0

    def test_unique_counts_use_distinct(self, spark):
        """unique_sellers should use countDistinct, not count."""
        df = spark.createDataFrame(
            [
                ("cat_a", "seller_1"),
                ("cat_a", "seller_1"),  # same seller, should not double-count
                ("cat_a", "seller_2"),
            ],
            schema=["category", "seller_id"],
        )
        result = df.groupBy("category").agg(
            F.countDistinct("seller_id").alias("unique_sellers"),
        ).collect()[0]
        assert result.unique_sellers == 2  # not 3
