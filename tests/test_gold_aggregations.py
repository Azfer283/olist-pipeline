"""
Tests for the Gold aggregation layer.

These call the REAL build_* functions in src/gold/builders.py against small
in-memory Silver fixtures, then assert on the builders' actual Gold output.

The builders read fixed Silver table names (utils.schema_definitions.SILVER_TABLES)
via spark.table and write Delta tables under olist_gold, so fixtures are written
as real Delta tables at those names and a throwaway olist_gold database holds the
outputs.

Includes a regression guard for the review-dedup fix (finding #1): an order with
TWO reviews must NOT fan out its order_items and inflate revenue/freight/items.
These guard tests FAIL against the old undeduped join.
"""

import sys, os
from datetime import datetime

import pytest
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from gold.builders import (
    build_daily_sales_summary,
    build_customer_ltv,
    build_seller_performance,
    build_order_fulfillment,
    build_product_category_performance,
)
from utils.schema_definitions import SILVER_TABLES, GOLD_TABLES


def _write_silver(spark, name, rows, cols):
    """Write an in-memory Silver fixture as a Delta table at the fixed name."""
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.createDataFrame(rows, cols).write.format("delta").saveAsTable(name)


@pytest.fixture
def gold(spark):
    """spark session with a clean olist_gold database for Gold output tables."""
    spark.sql("CREATE DATABASE IF NOT EXISTS olist_gold")
    yield spark
    spark.sql("DROP DATABASE IF EXISTS olist_gold CASCADE")


class TestDailySalesSummary:
    """build_daily_sales_summary against real Silver fixtures."""

    def test_excludes_non_delivered_and_aggregates_per_order(self, gold):
        spark = gold
        d = datetime(2023, 3, 15, 10, 0, 0)
        _write_silver(
            spark, SILVER_TABLES["orders"],
            [("O1", "C1", "delivered", d), ("O2", "C2", "shipped", d)],
            ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        )
        _write_silver(
            spark, SILVER_TABLES["order_items"],
            [("O1", "P1", 50.0, 5.0), ("O1", "P1", 30.0, 3.0), ("O2", "P1", 100.0, 10.0)],
            ["order_id", "product_id", "price", "freight_value"],
        )
        _write_silver(
            spark, SILVER_TABLES["products"],
            [("P1", "books")],
            ["product_id", "product_category_name_english"],
        )
        _write_silver(
            spark, SILVER_TABLES["customers"],
            [("C1", "SP"), ("C2", "RJ")],
            ["customer_id", "customer_state"],
        )

        build_daily_sales_summary(spark, GOLD_TABLES["daily_sales_summary"])
        rows = spark.table(GOLD_TABLES["daily_sales_summary"]).collect()

        assert len(rows) == 1  # only the delivered order forms a group
        r = rows[0]
        assert r.order_count == 1
        assert r.total_revenue == 88.0     # (50+5) + (30+3), summed within the order
        assert r.avg_order_value == 88.0   # per-order, not per-line-item
        assert r.year_month == "2023-03"


class TestCustomerLTV:
    """build_customer_ltv against real Silver fixtures."""

    def test_groups_by_customer_unique_id(self, gold):
        spark = gold
        d1 = datetime(2023, 1, 1, 10, 0, 0)
        d2 = datetime(2023, 2, 1, 10, 0, 0)
        _write_silver(
            spark, SILVER_TABLES["orders"],
            [("O1", "C1", "delivered", d1), ("O2", "C2", "delivered", d2)],
            ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        )
        _write_silver(
            spark, SILVER_TABLES["order_items"],
            [("O1", 1, 100.0, 10.0), ("O2", 1, 200.0, 20.0)],
            ["order_id", "order_item_id", "price", "freight_value"],
        )
        _write_silver(
            spark, SILVER_TABLES["order_reviews"],
            [("RV1", "O1", 5)],
            ["review_id", "order_id", "review_score"],
        )
        # C1 and C2 share one customer_unique_id -> collapse into a single LTV row
        _write_silver(
            spark, SILVER_TABLES["customers"],
            [("C1", "U1", "sp", "SP"), ("C2", "U1", "sp", "SP")],
            ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
        )

        build_customer_ltv(spark, GOLD_TABLES["customer_ltv"])
        rows = spark.table(GOLD_TABLES["customer_ltv"]).collect()

        assert len(rows) == 1
        r = rows[0]
        assert r.customer_unique_id == "U1"
        assert r.total_orders == 2
        assert r.total_spend == 330.0  # 110 + 220


class TestOrderFulfillment:
    """build_order_fulfillment against real Silver fixtures."""

    def test_metrics_and_delivered_only(self, gold):
        spark = gold
        purchase = datetime(2023, 1, 1, 10, 0, 0)
        approved = datetime(2023, 1, 1, 13, 0, 0)
        carrier = datetime(2023, 1, 2, 10, 0, 0)
        delivered = datetime(2023, 1, 5, 10, 0, 0)
        estimated = datetime(2023, 1, 10, 10, 0, 0)
        _write_silver(
            spark, SILVER_TABLES["orders"],
            [
                ("O1", "C1", "delivered", purchase, approved, carrier, delivered, estimated),
                ("O2", "C2", "canceled", purchase, approved, carrier, delivered, estimated),
            ],
            [
                "order_id", "customer_id", "order_status", "order_purchase_timestamp",
                "order_approved_at", "order_delivered_carrier_date",
                "order_delivered_customer_date", "order_estimated_delivery_date",
            ],
        )

        build_order_fulfillment(spark, GOLD_TABLES["order_fulfillment_metrics"])
        rows = spark.table(GOLD_TABLES["order_fulfillment_metrics"]).collect()

        assert len(rows) == 1  # only the delivered order
        r = rows[0]
        assert r.order_id == "O1"
        assert r.approval_time_hours == 3.0  # 10:00 -> 13:00
        assert r.delivered_early == 1        # Jan 5 <= Jan 10


class TestSellerPerformance:
    """build_seller_performance — including the review-dedup regression guard."""

    def test_two_reviews_do_not_inflate_revenue(self, gold):
        """REGRESSION GUARD (finding #1): one order + two reviews.

        Without per-order review dedup, the left join fans each order_item into
        two rows, doubling revenue and item counts. Deduped, each order_item is
        counted once and avg_review_score is the mean of the two scores.
        """
        spark = gold
        purchase = datetime(2023, 1, 1, 10, 0, 0)
        delivered = datetime(2023, 1, 5, 10, 0, 0)
        estimated = datetime(2023, 1, 10, 10, 0, 0)
        _write_silver(
            spark, SILVER_TABLES["orders"],
            [("O1", "C1", "delivered", purchase, delivered, estimated)],
            [
                "order_id", "customer_id", "order_status", "order_purchase_timestamp",
                "order_delivered_customer_date", "order_estimated_delivery_date",
            ],
        )
        _write_silver(
            spark, SILVER_TABLES["order_items"],
            [("O1", 1, "P1", "S1", 100.0, 10.0)],
            ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
        )
        # TWO reviews for the same order — the finding #1 fan-out trap
        _write_silver(
            spark, SILVER_TABLES["order_reviews"],
            [("RV1", "O1", 5), ("RV2", "O1", 3)],
            ["review_id", "order_id", "review_score"],
        )
        _write_silver(
            spark, SILVER_TABLES["sellers"],
            [("S1", "sao paulo", "SP")],
            ["seller_id", "seller_city", "seller_state"],
        )

        build_seller_performance(spark, GOLD_TABLES["seller_performance"])
        row = spark.table(GOLD_TABLES["seller_performance"]).collect()[0]

        # (a) one order_item counted once — NOT doubled by the 2-review fan-out
        assert row.total_revenue == 110.0   # undeduped code -> 220.0
        assert row.total_items_sold == 1    # undeduped code -> 2
        assert row.total_orders == 1
        # (b) review score is the mean of the two reviews
        assert row.avg_review_score == 4.0  # mean(5, 3)


class TestProductCategoryPerformance:
    """build_product_category_performance — including the dedup regression guard."""

    def test_two_reviews_do_not_inflate_revenue(self, gold):
        """REGRESSION GUARD (finding #1): one order + two reviews.

        seller_performance and product_category_performance dedup reviews on
        separate code paths, so guard both.
        """
        spark = gold
        purchase = datetime(2023, 1, 1, 10, 0, 0)
        _write_silver(
            spark, SILVER_TABLES["order_items"],
            [("O1", 1, "P1", "S1", 100.0, 10.0)],
            ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
        )
        _write_silver(
            spark, SILVER_TABLES["products"],
            [("P1", "electronics")],
            ["product_id", "product_category_name_english"],
        )
        _write_silver(
            spark, SILVER_TABLES["orders"],
            [("O1", "C1", "delivered", purchase)],
            ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        )
        # TWO reviews for the same order — the finding #1 fan-out trap
        _write_silver(
            spark, SILVER_TABLES["order_reviews"],
            [("RV1", "O1", 5), ("RV2", "O1", 3)],
            ["review_id", "order_id", "review_score"],
        )

        build_product_category_performance(spark, GOLD_TABLES["product_category_performance"])
        row = spark.table(GOLD_TABLES["product_category_performance"]).collect()[0]

        # (a) freight/revenue/items counted once — NOT doubled by the fan-out
        assert row.total_revenue == 110.0    # undeduped code -> 220.0
        assert row.total_freight == 10.0     # undeduped code -> 20.0
        assert row.total_items_sold == 1     # undeduped code -> 2
        assert row.total_orders == 1
        assert row.revenue_per_order == 110.0  # undeduped code -> 220.0
        # (b) review score is the mean of the two reviews
        assert row.avg_review_score == 4.0   # mean(5, 3)
