"""
Shared test fixtures — local SparkSession for all tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing with Delta Lake support."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("olist-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/tmp/olist_test_warehouse")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/olist_test_derby")
        .getOrCreate()
    )

    # Create test databases
    spark.sql("CREATE DATABASE IF NOT EXISTS olist_silver")

    yield spark

    # Cleanup test artifacts
    spark.sql("DROP DATABASE IF EXISTS olist_silver CASCADE")
    spark.stop()
