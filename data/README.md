# Data Directory

## Source Data
Download the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.

### Expected Files
Upload these CSVs to `/FileStore/olist/raw/` on Databricks:

| File | Description | ~Rows |
|---|---|---|
| olist_orders_dataset.csv | Orders with status & timestamps | 99,441 |
| olist_order_items_dataset.csv | Items per order with price | 112,650 |
| olist_order_payments_dataset.csv | Payment info per order | 103,886 |
| olist_order_reviews_dataset.csv | Customer reviews & scores | 100,000 |
| olist_customers_dataset.csv | Customer profiles | 99,441 |
| olist_products_dataset.csv | Product catalog | 32,951 |
| olist_sellers_dataset.csv | Seller locations | 3,095 |
| olist_geolocation_dataset.csv | Brazilian zip code lat/lng | 1,000,163 |
| product_category_name_translation.csv | Portuguese → English | 71 |

## Note
Raw CSV files are excluded from git via `.gitignore`. Each team member downloads from Kaggle directly.
