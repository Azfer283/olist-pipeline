"""
Schema definitions for all Olist tables.
Defined explicitly — never infer schema from CSV.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DecimalType, TimestampType, DateType, LongType
)


# ─────────────────────────────────────────────
# Bronze Schemas (match CSV as-is, all strings)
# ─────────────────────────────────────────────

ORDERS_RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

ORDER_ITEMS_RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("freight_value", StringType(), True),
])

ORDER_PAYMENTS_RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
])

ORDER_REVIEWS_RAW_SCHEMA = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
])

CUSTOMERS_RAW_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

PRODUCTS_RAW_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", StringType(), True),
    StructField("product_description_lenght", StringType(), True),
    StructField("product_photos_qty", StringType(), True),
    StructField("product_weight_g", StringType(), True),
    StructField("product_length_cm", StringType(), True),
    StructField("product_height_cm", StringType(), True),
    StructField("product_width_cm", StringType(), True),
])

SELLERS_RAW_SCHEMA = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

GEOLOCATION_RAW_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", StringType(), True),
    StructField("geolocation_lng", StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

CATEGORY_TRANSLATION_RAW_SCHEMA = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])


# ─────────────────────────────────────────────
# Silver Schemas (typed, clean)
# ─────────────────────────────────────────────

ORDERS_SILVER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("order_purchase_year_month", StringType(), True),
])

ORDER_ITEMS_SILVER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("freight_value", DecimalType(10, 2), True),
])

CUSTOMERS_SILVER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("customer_lat", DoubleType(), True),
    StructField("customer_lng", DoubleType(), True),
])

PRODUCTS_SILVER_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
    StructField("product_name_length", IntegerType(), True),
    StructField("product_description_length", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
])

SELLERS_SILVER_SCHEMA = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("seller_lat", DoubleType(), True),
    StructField("seller_lng", DoubleType(), True),
])

ORDER_PAYMENTS_SILVER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DecimalType(10, 2), True),
])

ORDER_REVIEWS_SILVER_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])


# ─────────────────────────────────────────────
# Table name mappings
# ─────────────────────────────────────────────

# Metadata columns added during Bronze ingestion
BRONZE_META_COLS = ["_ingestion_timestamp", "_source_file", "_batch_id", "_ingestion_date"]

BRONZE_TABLES = {
    "orders": "olist_bronze.orders",
    "order_items": "olist_bronze.order_items",
    "order_payments": "olist_bronze.order_payments",
    "order_reviews": "olist_bronze.order_reviews",
    "customers": "olist_bronze.customers",
    "products": "olist_bronze.products",
    "sellers": "olist_bronze.sellers",
    "geolocation": "olist_bronze.geolocation",
    "category_translation": "olist_bronze.category_translation",
}

SILVER_TABLES = {
    "orders": "olist_silver.orders",
    "order_items": "olist_silver.order_items",
    "order_payments": "olist_silver.order_payments",
    "order_reviews": "olist_silver.order_reviews",
    "customers": "olist_silver.customers",
    "products": "olist_silver.products",
    "sellers": "olist_silver.sellers",
}

GOLD_TABLES = {
    "daily_sales_summary": "olist_gold.daily_sales_summary",
    "customer_ltv": "olist_gold.customer_ltv",
    "seller_performance": "olist_gold.seller_performance",
    "order_fulfillment_metrics": "olist_gold.order_fulfillment_metrics",
    "product_category_performance": "olist_gold.product_category_performance",
}

# Source CSV file names (without .csv extension)
SOURCE_FILES = {
    "orders": "olist_orders_dataset",
    "order_items": "olist_order_items_dataset",
    "order_payments": "olist_order_payments_dataset",
    "order_reviews": "olist_order_reviews_dataset",
    "customers": "olist_customers_dataset",
    "products": "olist_products_dataset",
    "sellers": "olist_sellers_dataset",
    "geolocation": "olist_geolocation_dataset",
    "category_translation": "product_category_name_translation",
}

# Schema registry — maps table name to raw schema
RAW_SCHEMA_REGISTRY = {
    "orders": ORDERS_RAW_SCHEMA,
    "order_items": ORDER_ITEMS_RAW_SCHEMA,
    "order_payments": ORDER_PAYMENTS_RAW_SCHEMA,
    "order_reviews": ORDER_REVIEWS_RAW_SCHEMA,
    "customers": CUSTOMERS_RAW_SCHEMA,
    "products": PRODUCTS_RAW_SCHEMA,
    "sellers": SELLERS_RAW_SCHEMA,
    "geolocation": GEOLOCATION_RAW_SCHEMA,
    "category_translation": CATEGORY_TRANSLATION_RAW_SCHEMA,
}
