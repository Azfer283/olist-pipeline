"""
Inject deliberate data-quality bugs into Bronze tables so the agent has something to find.

Each bug reuses an existing row and mutates it, guaranteeing the schema still matches the
target Delta table (no manual schema construction needed).

Safety: Only runs in non-production environments.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.schema_definitions import BRONZE_TABLES

_ALLOWED_ENVS = {"local", "dev", "test", "demo"}


def inject_bugs(spark: SparkSession) -> str:
    """Append three classic bugs to Bronze: null order_id, future timestamp, duplicate customer."""
    env = os.environ.get("ENV", "").lower()
    if env not in _ALLOWED_ENVS:
        raise RuntimeError(
            f"inject_bugs blocked: ENV='{env}' is not in {_ALLOWED_ENVS}. "
            "Set ENV=dev/test/demo or unset it to proceed."
        )

    injected = []

    orders_tbl = BRONZE_TABLES["orders"]
    orders = spark.table(orders_tbl)

    # Bug 1: null primary key
    null_pk = orders.limit(1).withColumn("order_id", F.lit(None).cast("string"))
    null_pk.write.mode("append").format("delta").saveAsTable(orders_tbl)
    injected.append("null order_id")

    # Bug 2: future purchase timestamp
    future = orders.limit(1).withColumn(
        "order_purchase_timestamp", F.lit("2099-01-01 00:00:00")
    )
    future.write.mode("append").format("delta").saveAsTable(orders_tbl)
    injected.append("future order_purchase_timestamp")

    # Bug 3: duplicate customer row
    customers_tbl = BRONZE_TABLES["customers"]
    dup = spark.table(customers_tbl).limit(1)
    dup.write.mode("append").format("delta").saveAsTable(customers_tbl)
    injected.append("duplicate customer_id")

    return "Injected bugs: " + ", ".join(injected)
