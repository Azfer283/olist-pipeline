"""
Integration tests — end-to-end Bronze → Silver → Gold pipeline.
Validates data flows through all three layers correctly with sample data.
Tests run against a real local SparkSession.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.schema_definitions import (
    ORDERS_RAW_SCHEMA, ORDER_ITEMS_RAW_SCHEMA, ORDER_PAYMENTS_RAW_SCHEMA,
    CUSTOMERS_RAW_SCHEMA, PRODUCTS_RAW_SCHEMA, SELLERS_RAW_SCHEMA,
    GEOLOCATION_RAW_SCHEMA, CATEGORY_TRANSLATION_RAW_SCHEMA,
    BRONZE_META_COLS,
)


@pytest.fixture(scope="class")
def pipeline_tables(spark):
    """Set up Bronze tables with sample data for integration testing."""
    # Create databases
    spark.sql("CREATE DATABASE IF NOT EXISTS olist_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS olist_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS olist_gold")

    # --- Seed Bronze tables ---
    orders_data = [
        ("ord_1", "cust_1", "delivered", "2023-06-01 10:00:00", "2023-06-01 11:00:00",
         "2023-06-02 08:00:00", "2023-06-05 14:00:00", "2023-06-10 00:00:00"),
        ("ord_2", "cust_2", "delivered", "2023-06-02 09:00:00", "2023-06-02 10:00:00",
         "2023-06-03 07:00:00", "2023-06-06 12:00:00", "2023-06-12 00:00:00"),
        ("ord_3", "cust_1", "shipped", "2023-06-03 08:00:00", None, None, None, "2023-06-15 00:00:00"),
    ]
    spark.createDataFrame(orders_data, schema=ORDERS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.orders")

    items_data = [
        ("ord_1", "1", "prod_1", "seller_1", "2023-06-03 00:00:00", "99.90", "15.50"),
        ("ord_1", "2", "prod_2", "seller_1", "2023-06-03 00:00:00", "49.90", "10.00"),
        ("ord_2", "1", "prod_1", "seller_2", "2023-06-04 00:00:00", "99.90", "15.50"),
    ]
    spark.createDataFrame(items_data, schema=ORDER_ITEMS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.order_items")

    payments_data = [
        ("ord_1", "1", "credit_card", "3", "165.30"),
        ("ord_2", "1", "boleto", "1", "115.40"),
    ]
    spark.createDataFrame(payments_data, schema=ORDER_PAYMENTS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.order_payments")

    customers_data = [
        ("cust_1", "uniq_1", "01001", "sao paulo", "SP"),
        ("cust_2", "uniq_2", "20040", "rio de janeiro", "RJ"),
    ]
    spark.createDataFrame(customers_data, schema=CUSTOMERS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.customers")

    sellers_data = [
        ("seller_1", "01001", "sao paulo", "SP"),
        ("seller_2", "30130", "belo horizonte", "MG"),
    ]
    spark.createDataFrame(sellers_data, schema=SELLERS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.sellers")

    products_data = [
        ("prod_1", "informatica", "10", "50", "2", "500", "30", "10", "20"),
        ("prod_2", "moveis_decoracao", "8", "30", "3", "2000", "50", "40", "60"),
    ]
    spark.createDataFrame(products_data, schema=PRODUCTS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.products")

    geo_data = [
        ("01001", "-23.55", "-46.63", "sao paulo", "SP"),
        ("20040", "-22.90", "-43.17", "rio de janeiro", "RJ"),
        ("30130", "-19.92", "-43.93", "belo horizonte", "MG"),
    ]
    spark.createDataFrame(geo_data, schema=GEOLOCATION_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.geolocation")

    translation_data = [
        ("informatica", "computers"),
        ("moveis_decoracao", "furniture_decor"),
    ]
    spark.createDataFrame(translation_data, schema=CATEGORY_TRANSLATION_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.category_translation")

    # Reviews (needed for Gold LTV/seller perf)
    from utils.schema_definitions import ORDER_REVIEWS_RAW_SCHEMA
    reviews_data = [
        ("rev_1", "ord_1", "5", None, "great", "2023-06-06", "2023-06-07 10:00:00"),
        ("rev_2", "ord_2", "4", None, "good", "2023-06-07", "2023-06-08 10:00:00"),
    ]
    spark.createDataFrame(reviews_data, schema=ORDER_REVIEWS_RAW_SCHEMA).write.mode("overwrite").saveAsTable("olist_bronze.order_reviews")

    # DQ support tables
    spark.sql("""CREATE TABLE IF NOT EXISTS olist_silver.dq_log (
        table_name STRING, layer STRING, input_count LONG,
        output_count LONG, quarantined_count LONG, null_counts STRING, run_timestamp STRING
    ) USING DELTA""")
    spark.sql("""CREATE TABLE IF NOT EXISTS olist_silver.quarantine (
        source_table STRING, rejection_reason STRING,
        quarantine_timestamp TIMESTAMP, record_json STRING
    ) USING DELTA""")

    yield

    # Cleanup
    for db in ["olist_bronze", "olist_silver", "olist_gold"]:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


class TestBronzeToSilverIntegration:
    """Test that Silver cleaning produces correctly typed, deduped data from Bronze."""

    def test_clean_orders_produces_typed_output(self, spark, pipeline_tables):
        from silver.clean_orders import clean_orders
        count = clean_orders(spark, "olist_bronze.orders", "olist_silver.orders")
        assert count == 3  # all 3 orders pass (no null order_id/customer_id)

        # Verify timestamp casting worked
        df = spark.table("olist_silver.orders")
        assert "TimestampType" in str(df.schema["order_purchase_timestamp"].dataType)

    def test_clean_order_items_decimal_types(self, spark, pipeline_tables):
        from silver.clean_order_tables import clean_order_items
        count = clean_order_items(spark, "olist_bronze.order_items", "olist_silver.order_items")
        assert count == 3

        # Verify DecimalType for monetary fields
        df = spark.table("olist_silver.order_items")
        assert isinstance(df.schema["price"].dataType, DecimalType)
        assert isinstance(df.schema["freight_value"].dataType, DecimalType)

    def test_clean_order_payments_decimal_types(self, spark, pipeline_tables):
        from silver.clean_order_tables import clean_order_payments
        count = clean_order_payments(spark, "olist_bronze.order_payments", "olist_silver.order_payments")
        assert count == 2

        df = spark.table("olist_silver.order_payments")
        assert isinstance(df.schema["payment_value"].dataType, DecimalType)

    def test_clean_products_translates_categories(self, spark, pipeline_tables):
        from silver.clean_products import clean_products
        count = clean_products(spark, "olist_bronze.products", "olist_silver.products")
        assert count == 2

        df = spark.table("olist_silver.products")
        row = df.filter(F.col("product_id") == "prod_1").collect()[0]
        assert row.product_category_name_english == "computers"


class TestSilverToGoldIntegration:
    """Test that Gold builders produce valid aggregations from Silver."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_silver(self, spark, pipeline_tables):
        """Ensure Silver tables exist before Gold tests run."""
        from silver.clean_orders import clean_orders
        from silver.clean_order_tables import clean_order_items, clean_order_payments, clean_order_reviews
        from silver.clean_products import clean_products
        from silver.clean_geo_entities import clean_customers, clean_sellers

        clean_orders(spark, "olist_bronze.orders", "olist_silver.orders")
        clean_order_items(spark, "olist_bronze.order_items", "olist_silver.order_items")
        clean_order_payments(spark, "olist_bronze.order_payments", "olist_silver.order_payments")
        clean_order_reviews(spark, "olist_bronze.order_reviews", "olist_silver.order_reviews")
        clean_products(spark, "olist_bronze.products", "olist_silver.products")
        clean_customers(spark, "olist_bronze.customers", "olist_silver.customers")
        clean_sellers(spark, "olist_bronze.sellers", "olist_silver.sellers")

    def test_daily_sales_summary(self, spark, pipeline_tables):
        from gold.builders import build_daily_sales_summary
        count = build_daily_sales_summary(spark, "olist_gold.daily_sales_summary")
        assert count > 0

        df = spark.table("olist_gold.daily_sales_summary")
        # Only delivered orders should appear (ord_3 is shipped)
        # ord_1 spans 2 categories (computers + furniture_decor) → 2 rows with order_count=1 each
        # ord_2 spans 1 category (computers) → 1 row with order_count=1
        total_orders = df.agg(F.sum("order_count")).collect()[0][0]
        assert total_orders == 3  # ord_1 (×2 categories) + ord_2 (×1 category)

    def test_customer_ltv(self, spark, pipeline_tables):
        from gold.builders import build_customer_ltv
        count = build_customer_ltv(spark, "olist_gold.customer_ltv")
        assert count > 0

        df = spark.table("olist_gold.customer_ltv")
        # cust_1 (uniq_1) has 1 delivered order (ord_1), cust_2 (uniq_2) has 1 (ord_2)
        assert df.count() == 2

    def test_seller_performance(self, spark, pipeline_tables):
        from gold.builders import build_seller_performance
        count = build_seller_performance(spark, "olist_gold.seller_performance")
        assert count > 0

    def test_product_category_performance(self, spark, pipeline_tables):
        from gold.builders import build_product_category_performance
        count = build_product_category_performance(spark, "olist_gold.product_category_performance")
        assert count > 0

        df = spark.table("olist_gold.product_category_performance")
        computers = df.filter(F.col("product_category_name_english") == "computers").collect()[0]
        assert computers.total_orders == 2  # prod_1 in ord_1 and ord_2
