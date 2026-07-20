"""
Common Spark utilities and helpers.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def setup_databases(spark: SparkSession):
    """Create all layer databases."""
    _ALLOWED_DBS = {"olist_bronze", "olist_silver", "olist_gold"}
    for db in _ALLOWED_DBS:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")


def get_geo_lookup(spark: SparkSession):
    """Load geolocation as avg lat/lng per zip code prefix."""
    from utils.schema_definitions import BRONZE_TABLES, BRONZE_META_COLS

    geo_df = spark.table(BRONZE_TABLES["geolocation"]).drop(*BRONZE_META_COLS)
    return (
        geo_df
        .withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            F.avg("geolocation_lat").alias("geo_lat"),
            F.avg("geolocation_lng").alias("geo_lng"),
        )
    )



