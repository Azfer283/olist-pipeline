"""
Tests for Silver cleaning layer.
Validates type casting, dedup logic, and quarantine behavior.
Tests run against a real local SparkSession.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.data_quality import check_nulls, quarantine_records, enforce_schema


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
        # Quarantine table created by conftest.py session fixture
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


class TestCleanOrdersLogic:
    """Test orders cleaning transformations (without writing to tables)."""

    def test_timestamp_casting(self, spark):
        """String timestamps should cast to TimestampType."""
        df = spark.createDataFrame(
            [("2023-01-15 10:30:00",)],
            schema=["order_purchase_timestamp"],
        )
        df = df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))

        assert df.schema["order_purchase_timestamp"].dataType == TimestampType()
        row = df.collect()[0]
        assert row.order_purchase_timestamp.year == 2023
        assert row.order_purchase_timestamp.month == 1

    def test_year_month_partition_format(self, spark):
        """year_month partition column should be yyyy-MM format."""
        df = spark.createDataFrame(
            [("2023-03-15 10:30:00",)],
            schema=["order_purchase_timestamp"],
        )
        df = df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        df = df.withColumn(
            "order_purchase_year_month",
            F.date_format("order_purchase_timestamp", "yyyy-MM"),
        )

        result = df.collect()[0].order_purchase_year_month
        assert result == "2023-03"

    def test_dedup_keeps_one_per_key(self, spark):
        """dropDuplicates on order_id should keep exactly one row per key."""
        df = spark.createDataFrame(
            [("ord_1", "a"), ("ord_1", "b"), ("ord_2", "c")],
            schema=["order_id", "customer_id"],
        )
        df = df.dropDuplicates(["order_id"])
        assert df.count() == 2


class TestCleanProductsLogic:
    """Test products cleaning transformations."""

    def test_typo_column_rename(self, spark):
        """'lenght' columns should be renamed to 'length'."""
        df = spark.createDataFrame(
            [(10, 20)],
            schema=["product_name_lenght", "product_description_lenght"],
        )
        df = (
            df
            .withColumnRenamed("product_name_lenght", "product_name_length")
            .withColumnRenamed("product_description_lenght", "product_description_length")
        )

        col_names = df.columns
        assert "product_name_length" in col_names
        assert "product_description_length" in col_names
        assert "product_name_lenght" not in col_names

    def test_int_casting(self, spark):
        """String numeric columns should cast to IntegerType."""
        df = spark.createDataFrame(
            [("100", "50")],
            schema=["product_weight_g", "product_height_cm"],
        )
        df = df.withColumn("product_weight_g", F.col("product_weight_g").cast("int"))
        df = df.withColumn("product_height_cm", F.col("product_height_cm").cast("int"))

        assert df.schema["product_weight_g"].dataType == IntegerType()
        assert df.schema["product_height_cm"].dataType == IntegerType()
        assert df.collect()[0].product_weight_g == 100


class TestCleanOrderItemsLogic:
    """Test order items cleaning transformations."""

    def test_price_freight_cast_to_double(self, spark):
        """price and freight_value should become DoubleType."""
        df = spark.createDataFrame(
            [("29.99", "7.50")],
            schema=["price", "freight_value"],
        )
        df = df.withColumn("price", F.col("price").cast("double"))
        df = df.withColumn("freight_value", F.col("freight_value").cast("double"))

        assert df.schema["price"].dataType == DoubleType()
        assert df.schema["freight_value"].dataType == DoubleType()
        assert abs(df.collect()[0].price - 29.99) < 0.01

    def test_composite_key_dedup(self, spark):
        """Dedup on (order_id, order_item_id) should keep unique combos."""
        df = spark.createDataFrame(
            [("ord_1", 1), ("ord_1", 1), ("ord_1", 2), ("ord_2", 1)],
            schema=["order_id", "order_item_id"],
        )
        df = df.dropDuplicates(["order_id", "order_item_id"])
        assert df.count() == 3  # (ord_1,1), (ord_1,2), (ord_2,1)


class TestCleanOrderReviewsLogic:
    """Test order reviews cleaning transformations."""

    def test_review_score_cast_to_int(self, spark):
        """review_score string should cast to IntegerType."""
        df = spark.createDataFrame(
            [("5",), ("3",), ("1",)],
            schema=["review_score"],
        )
        df = df.withColumn("review_score", F.col("review_score").cast("int"))

        assert df.schema["review_score"].dataType == IntegerType()
        scores = [row.review_score for row in df.collect()]
        assert sorted(scores) == [1, 3, 5]


class TestEnforceSchema:
    """Test the enforce_schema utility function."""

    def test_selects_correct_columns_in_order(self, spark):
        """enforce_schema should select only schema columns in schema order."""
        from pyspark.sql.types import StructType, StructField

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
        from pyspark.sql.types import StructType, StructField

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
