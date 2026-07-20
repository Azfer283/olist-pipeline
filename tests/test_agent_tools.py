"""
Tests for the agent's read-only tools.
These run against a local SparkSession — no Ollama / LLM required.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent.tools import (
    inspect_table,
    read_dq_logs,
    read_quarantine,
    compare_counts,
    trace_record,
)
from utils.data_quality import log_dq_metrics, quarantine_records


def _write_delta(spark, df, table):
    df.createOrReplaceTempView("_delta_write_tmp")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} USING DELTA AS SELECT * FROM _delta_write_tmp")


@pytest.fixture(scope="module")
def bronze_db(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS olist_bronze")
    yield
    spark.sql("DROP DATABASE IF EXISTS olist_bronze CASCADE")
    spark.sql("DROP TABLE IF EXISTS olist_silver.orders")


class TestInspectTable:
    def test_reports_counts_and_nulls(self, spark, bronze_db):
        df = spark.createDataFrame(
            [("o1", "c1"), ("o2", None)], ["order_id", "customer_id"]
        )
        _write_delta(spark, df, "olist_bronze.orders")

        out = inspect_table(spark, "olist_bronze.orders")

        assert "Row count: 2" in out
        assert "customer_id: 1" in out

    def test_missing_table(self, spark, bronze_db):
        out = inspect_table(spark, "olist_bronze.sellers")
        assert "does not exist" in out


class TestCompareCounts:
    def test_shows_data_loss(self, spark, bronze_db):
        bronze = spark.createDataFrame([("o1",), ("o2",), ("o3",)], ["order_id"])
        _write_delta(spark, bronze, "olist_bronze.orders")
        silver = spark.createDataFrame([("o1",), ("o2",)], ["order_id"])
        _write_delta(spark, silver, "olist_silver.orders")

        out = compare_counts(spark)

        assert "orders" in out
        assert "bronze=3" in out
        assert "silver=2" in out
        assert "lost=1" in out


class TestDqLogsAndQuarantine:
    def test_read_dq_logs(self, spark):
        log_dq_metrics(
            spark, "orders", "silver",
            input_count=100, output_count=98, quarantined_count=2,
            null_counts={"order_id": 2},
        )
        out = read_dq_logs(spark, "orders")
        assert "orders" in out
        assert "quarantined=2" in out

    def test_read_quarantine(self, spark):
        df = spark.createDataFrame(
            [("o1", "c1"), (None, "c2")], ["order_id", "customer_id"]
        )
        good, count = quarantine_records(
            spark, df, "order_id IS NULL",
            reason="Null order_id", source_table="orders",
        )
        assert count == 1
        out = read_quarantine(spark, "orders")
        assert "Null order_id" in out


class TestTraceRecord:
    def test_traces_across_layers(self, spark, bronze_db):
        _write_delta(spark, spark.createDataFrame([("o1",)], ["order_id"]), "olist_bronze.orders")
        _write_delta(spark, spark.createDataFrame([("o1",)], ["order_id"]), "olist_silver.orders")

        out = trace_record(spark, "o1")

        assert "Bronze" in out and "Silver" in out
        assert "found" in out
