"""
Tests for data quality checks — Bronze validation, Gold assertions, and edge cases.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.data_quality import check_nulls, run_dq_checks, enforce_schema


class TestEnforceSchema:
    """Test schema enforcement utility."""

    def test_reorders_and_casts_columns(self, spark):
        """enforce_schema should reorder columns to match target schema."""
        target_schema = StructType([
            StructField("col_b", StringType(), True),
            StructField("col_a", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [("1", "hello"), ("2", "world")],
            schema=["col_a", "col_b"],
        )
        result = enforce_schema(df, target_schema)
        assert result.columns == ["col_b", "col_a"]

    def test_casts_string_to_int(self, spark):
        """enforce_schema should cast string columns to target types."""
        target_schema = StructType([
            StructField("value", IntegerType(), True),
        ])
        df = spark.createDataFrame([("42",), ("7",)], schema=["value"])
        result = enforce_schema(df, target_schema)
        row = result.collect()[0]
        assert row.value == 42
        assert isinstance(row.value, int)


class TestRunDqChecks:
    """Test the full run_dq_checks pipeline."""

    @pytest.fixture(autouse=True)
    def _setup_dq_tables(self, spark):
        """Ensure DQ support tables exist."""
        spark.sql("CREATE DATABASE IF NOT EXISTS olist_silver")
        spark.sql("""CREATE TABLE IF NOT EXISTS olist_silver.dq_log (
            table_name STRING, layer STRING, input_count LONG,
            output_count LONG, quarantined_count LONG, null_counts STRING, run_timestamp STRING
        ) USING DELTA""")
        spark.sql("""CREATE TABLE IF NOT EXISTS olist_silver.quarantine (
            source_table STRING, rejection_reason STRING,
            quarantine_timestamp TIMESTAMP, record_json STRING
        ) USING DELTA""")

    def test_deduplicates_on_primary_keys(self, spark):
        """run_dq_checks should remove duplicates based on primary_keys."""
        df = spark.createDataFrame(
            [("a", "x"), ("a", "y"), ("b", "z")],
            schema=["id", "value"],
        )
        result = run_dq_checks(
            spark, df, table_name="test_dedup", layer="test",
            primary_keys=["id"], not_null_columns=["id"],
        )
        assert result.count() == 2  # "a" deduped

    def test_quarantines_null_records(self, spark):
        """Records with null in not_null_columns should be quarantined."""
        df = spark.createDataFrame(
            [("a", "x"), (None, "y"), ("b", None)],
            schema=["id", "value"],
        )
        result = run_dq_checks(
            spark, df, table_name="test_nulls", layer="test",
            primary_keys=["id"], not_null_columns=["id"],
        )
        # (None, "y") should be quarantined
        assert result.count() == 2  # "a" and "b" remain

    def test_logs_metrics_after_run(self, spark):
        """run_dq_checks should write a row to the DQ log table."""
        before_count = spark.table("olist_silver.dq_log").count()

        df = spark.createDataFrame([("a",), ("b",)], schema=["id"])
        run_dq_checks(
            spark, df, table_name="test_logging", layer="test",
            primary_keys=["id"], not_null_columns=["id"],
        )

        after_count = spark.table("olist_silver.dq_log").count()
        assert after_count == before_count + 1


class TestGoldDqAssertion:
    """Test the Gold-layer DQ assertion."""

    def test_raises_on_empty_table(self, spark):
        """_assert_gold_quality should raise RuntimeError on empty table."""
        spark.sql("CREATE DATABASE IF NOT EXISTS olist_gold")
        spark.createDataFrame([], schema="col STRING").write.mode("overwrite").saveAsTable("olist_gold.test_empty")

        from gold.builders import _assert_gold_quality
        with pytest.raises(RuntimeError, match="Gold DQ failed"):
            _assert_gold_quality(spark, "olist_gold.test_empty")

    def test_passes_on_populated_table(self, spark):
        """_assert_gold_quality should return count for non-empty table."""
        spark.sql("CREATE DATABASE IF NOT EXISTS olist_gold")
        spark.createDataFrame([("a",), ("b",)], schema=["col"]).write.mode("overwrite").saveAsTable("olist_gold.test_ok")

        from gold.builders import _assert_gold_quality
        count = _assert_gold_quality(spark, "olist_gold.test_ok")
        assert count == 2
