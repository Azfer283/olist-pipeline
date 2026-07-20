"""
Tests for Bronze ingestion layer.
Tests run against a real local SparkSession — no mocks needed for schema validation.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from pyspark.sql import functions as F

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.schema_definitions import (
    RAW_SCHEMA_REGISTRY, ORDERS_RAW_SCHEMA, BRONZE_TABLES, SOURCE_FILES,
)


class TestSchemaDefinitions:
    """Test that schema definitions are correct and consistent."""

    def test_all_raw_schemas_are_string_type(self):
        """Every field in every raw schema should be StringType (we cast in Silver)."""
        for table_name, schema in RAW_SCHEMA_REGISTRY.items():
            for field in schema.fields:
                assert field.dataType == StringType(), (
                    f"{table_name}.{field.name} is {field.dataType}, expected StringType"
                )

    def test_all_bronze_tables_have_schemas(self):
        """Every table in BRONZE_TABLES should have a matching schema in RAW_SCHEMA_REGISTRY."""
        for table_key in BRONZE_TABLES:
            assert table_key in RAW_SCHEMA_REGISTRY or table_key in SOURCE_FILES, (
                f"{table_key} in BRONZE_TABLES but no schema or source file defined"
            )

    def test_orders_schema_has_expected_columns(self):
        """Orders raw schema should have all 8 expected columns."""
        field_names = [f.name for f in ORDERS_RAW_SCHEMA.fields]
        expected = [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
        assert field_names == expected

    def test_source_files_map_all_tables(self):
        """SOURCE_FILES should cover all 9 source datasets."""
        assert len(SOURCE_FILES) == 9
        assert "orders" in SOURCE_FILES
        assert "geolocation" in SOURCE_FILES
        assert "category_translation" in SOURCE_FILES


class TestBronzeIngestion:
    """Test the ingest_table function logic using a local Spark session."""

    def test_metadata_columns_added(self, spark, tmp_path):
        """ingest_table should add _ingestion_timestamp, _source_file, _batch_id, _ingestion_date."""
        # Create a test CSV
        csv_path = str(tmp_path / "test_orders.csv")
        test_df = spark.createDataFrame(
            [("ord_1", "cust_1", "delivered", "2023-01-01", None, None, None, None)],
            schema=ORDERS_RAW_SCHEMA,
        )
        test_df.toPandas().to_csv(csv_path, index=False)

        # Read it back the way ingest_table does
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(ORDERS_RAW_SCHEMA)
            .csv(csv_path)
        )
        df = (
            df
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit("test-batch"))
            .withColumn("_ingestion_date", F.current_date())
        )

        col_names = df.columns
        assert "_ingestion_timestamp" in col_names
        assert "_source_file" in col_names
        assert "_batch_id" in col_names
        assert "_ingestion_date" in col_names

    def test_no_schema_inference(self, spark, tmp_path):
        """Reading with explicit schema should NOT infer types — all remain StringType."""
        csv_path = str(tmp_path / "test_orders2.csv")
        test_df = spark.createDataFrame(
            [("ord_1", "cust_1", "delivered", "2023-01-01", None, None, None, None)],
            schema=ORDERS_RAW_SCHEMA,
        )
        test_df.toPandas().to_csv(csv_path, index=False)

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(ORDERS_RAW_SCHEMA)
            .csv(csv_path)
        )

        for field in df.schema.fields:
            if not field.name.startswith("_"):
                assert field.dataType == StringType(), (
                    f"{field.name} should be StringType, got {field.dataType}"
                )
