"""
Tests for the Silver cleaning layer.

The data-quality utility tests exercise data_quality.py directly. The clean-*
tests call the REAL clean_* functions against small in-memory Bronze fixtures
(registered as temp views for the source arg; clean_products also needs the
fixed olist_bronze.category_translation table) and assert on the actual Silver
output tables.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DecimalType, TimestampType,
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.data_quality import check_nulls, quarantine_records, enforce_schema
from utils.schema_definitions import (
    BRONZE_TABLES,
    ORDERS_RAW_SCHEMA, PRODUCTS_RAW_SCHEMA, ORDER_ITEMS_RAW_SCHEMA,
    ORDER_REVIEWS_RAW_SCHEMA, CATEGORY_TRANSLATION_RAW_SCHEMA,
    ORDERS_SILVER_SCHEMA, PRODUCTS_SILVER_SCHEMA, ORDER_ITEMS_SILVER_SCHEMA,
    ORDER_REVIEWS_SILVER_SCHEMA,
)
from silver.clean_orders import clean_orders
from silver.clean_products import clean_products
from silver.clean_order_tables import clean_order_items, clean_order_reviews


class TestDataQualityUtils:
    """Test the data_quality.py utility functions directly."""

    def test_check_nulls_counts_correctly(self, spark):
        """check_nulls should return accurate null counts per column."""
        df = spark.createDataFrame(
            [("a", "x"), (None, "y"), (None, None)],
            schema=["col_a", "col_b"],
        )
        result = check_nulls(df, ["col_a", "col_b"])
        assert result["col_a"] == 2
        assert result["col_b"] == 1

    def test_check_nulls_zero_when_no_nulls(self, spark):
        """check_nulls should return 0 for columns with no nulls."""
        df = spark.createDataFrame(
            [("a",), ("b",)],
            schema=["col_a"],
        )
        result = check_nulls(df, ["col_a"])
        assert result["col_a"] == 0


class TestQuarantineRecords:
    """Test the quarantine_records function."""

    def test_splits_good_and_bad(self, spark):
        """quarantine_records should split df into good/bad based on condition."""
        spark.sql("CREATE DATABASE IF NOT EXISTS olist_silver")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS olist_silver.quarantine (
                source_table STRING, rejection_reason STRING,
                quarantine_timestamp TIMESTAMP, record_json STRING
            ) USING DELTA
        """)

        df = spark.createDataFrame(
            [("ord_1", "cust_1"), (None, "cust_2"), ("ord_3", None)],
            schema=["order_id", "customer_id"],
        )
        good_df, q_count = quarantine_records(
            spark, df, "order_id IS NULL", "Null order_id", "test_table"
        )

        assert good_df.count() == 2  # ord_1 and ord_3
        assert q_count == 1  # the null order_id row

    def test_quarantine_preserves_all_good_records(self, spark):
        """Good records should pass through unmodified."""
        spark.sql("""
            CREATE TABLE IF NOT EXISTS olist_silver.quarantine (
                source_table STRING, rejection_reason STRING,
                quarantine_timestamp TIMESTAMP, record_json STRING
            ) USING DELTA
        """)

        df = spark.createDataFrame(
            [("a",), ("b",), ("c",)],
            schema=["id"],
        )
        good_df, q_count = quarantine_records(
            spark, df, "id IS NULL", "Null id", "test"
        )

        assert good_df.count() == 3
        assert q_count == 0


class TestCleanOrders:
    """Test the real clean_orders transformation Bronze -> Silver."""

    def test_casts_dedups_quarantines_and_partitions(self, spark):
        rows = [
            # order_id, customer_id, status, purchase, approved, carrier, delivered, estimated
            ("O1", "C1", "delivered", "2023-03-15 10:30:00", None, None, None, None),
            ("O1", "C1", "delivered", "2023-03-15 10:30:00", None, None, None, None),  # dup PK
            (None, "C2", "delivered", "2023-03-16 09:00:00", None, None, None, None),  # null PK -> quarantine
        ]
        spark.createDataFrame(rows, ORDERS_RAW_SCHEMA).createOrReplaceTempView("bronze_orders_fixture")

        clean_orders(spark, "bronze_orders_fixture", "olist_silver.orders_clean_test")
        out = spark.table("olist_silver.orders_clean_test")

        # dedup on order_id + quarantine of the null order_id -> one surviving row
        assert out.count() == 1
        assert out.schema["order_purchase_timestamp"].dataType == TimestampType()
        assert out.collect()[0].order_purchase_year_month == "2023-03"
        # enforce_schema yields exactly the Silver contract columns, in order
        assert out.columns == [f.name for f in ORDERS_SILVER_SCHEMA.fields]


class TestCleanProducts:
    """Test the real clean_products transformation (rename typos, cast, translate)."""

    def test_renames_typos_casts_and_translates(self, spark):
        spark.sql("CREATE DATABASE IF NOT EXISTS olist_bronze")
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLES['category_translation']}")
        spark.createDataFrame(
            [("beleza_saude", "health_beauty")],
            CATEGORY_TRANSLATION_RAW_SCHEMA,
        ).write.format("delta").saveAsTable(BRONZE_TABLES["category_translation"])

        rows = [
            # product_id, category, name_lenght, desc_lenght, photos, weight, length, height, width
            ("P1", "beleza_saude", "10", "20", "3", "500", "10", "5", "8"),
            ("P1", "beleza_saude", "10", "20", "3", "500", "10", "5", "8"),  # dup PK
        ]
        spark.createDataFrame(rows, PRODUCTS_RAW_SCHEMA).createOrReplaceTempView("bronze_products_fixture")

        clean_products(spark, "bronze_products_fixture", "olist_silver.products_clean_test")
        out = spark.table("olist_silver.products_clean_test")

        assert out.count() == 1  # dedup on product_id
        assert "product_name_length" in out.columns
        assert "product_name_lenght" not in out.columns
        assert out.schema["product_weight_g"].dataType == IntegerType()
        assert out.collect()[0].product_category_name_english == "health_beauty"
        assert out.columns == [f.name for f in PRODUCTS_SILVER_SCHEMA.fields]


class TestCleanOrderItems:
    """Test the real clean_order_items transformation."""

    def test_casts_and_dedups_composite_key(self, spark):
        rows = [
            # order_id, order_item_id, product_id, seller_id, shipping_limit, price, freight
            ("O1", "1", "P1", "S1", "2023-01-01 00:00:00", "29.99", "7.50"),
            ("O1", "1", "P1", "S1", "2023-01-01 00:00:00", "29.99", "7.50"),  # dup composite key
            ("O1", "2", "P1", "S1", "2023-01-01 00:00:00", "10.00", "2.00"),
            ("O2", "1", "P2", "S2", "2023-01-01 00:00:00", "5.00", "1.00"),
        ]
        spark.createDataFrame(rows, ORDER_ITEMS_RAW_SCHEMA).createOrReplaceTempView("bronze_items_fixture")

        clean_order_items(spark, "bronze_items_fixture", "olist_silver.items_clean_test")
        out = spark.table("olist_silver.items_clean_test")

        assert out.count() == 3  # dedup on (order_id, order_item_id)
        assert out.schema["order_item_id"].dataType == IntegerType()
        assert out.schema["price"].dataType == DecimalType(10, 2)
        assert out.columns == [f.name for f in ORDER_ITEMS_SILVER_SCHEMA.fields]


class TestCleanOrderReviews:
    """Test the real clean_order_reviews transformation."""

    def test_casts_score_and_dedups(self, spark):
        rows = [
            # review_id, order_id, score, title, message, creation, answer
            ("RV1", "O1", "5", None, None, None, None),
            ("RV1", "O1", "5", None, None, None, None),  # dup review_id
            ("RV2", "O1", "3", None, None, None, None),
        ]
        spark.createDataFrame(rows, ORDER_REVIEWS_RAW_SCHEMA).createOrReplaceTempView("bronze_reviews_fixture")

        clean_order_reviews(spark, "bronze_reviews_fixture", "olist_silver.reviews_clean_test")
        out = spark.table("olist_silver.reviews_clean_test")

        assert out.count() == 2  # dedup on review_id
        assert out.schema["review_score"].dataType == IntegerType()
        assert sorted(r.review_score for r in out.collect()) == [3, 5]
        assert out.columns == [f.name for f in ORDER_REVIEWS_SILVER_SCHEMA.fields]


class TestEnforceSchema:
    """Test the enforce_schema utility function."""

    def test_selects_correct_columns_in_order(self, spark):
        """enforce_schema should select only schema columns in schema order."""
        schema = StructType([
            StructField("b", StringType(), True),
            StructField("a", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [(1, "x", "extra")],
            schema=["a", "b", "c"],
        )
        result = enforce_schema(df, schema)

        assert result.columns == ["b", "a"]  # schema order, not original
        assert result.count() == 1

    def test_casts_types_to_match_schema(self, spark):
        """enforce_schema should cast columns to the schema's data types."""
        schema = StructType([
            StructField("price", DoubleType(), True),
            StructField("qty", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [("29.99", "3")],
            schema=["price", "qty"],
        )
        result = enforce_schema(df, schema)

        assert result.schema["price"].dataType == DoubleType()
        assert result.schema["qty"].dataType == IntegerType()
        row = result.collect()[0]
        assert abs(row.price - 29.99) < 0.01
        assert row.qty == 3
