"""
End-to-end demo: inject bugs into Bronze, run Silver cleaning, then let the agent investigate.

Run from the repo root with a local Ollama model available:
    python src/demo/run_demo.py

Assumes Bronze tables are already populated (run the ingestion notebooks first).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from utils.spark_utils import setup_databases
from utils.schema_definitions import BRONZE_TABLES, SILVER_TABLES
from silver.clean_orders import clean_orders
from demo.inject_bugs import inject_bugs
from agent.agent import build_agent


def main():
    spark = SparkSession.builder.getOrCreate()
    setup_databases(spark)

    print(inject_bugs(spark))

    # Re-run Silver cleaning so the bugs land in the DQ log / quarantine.
    clean_orders(spark, BRONZE_TABLES["orders"], SILVER_TABLES["orders"])

    agent = build_agent(spark)
    question = (
        "Something looks off in the orders pipeline after the latest load. "
        "Investigate the data quality, find the root cause, and suggest a fix."
    )
    result = agent.invoke({"input": question})
    print("\n=== Agent conclusion ===")
    print(result["output"])


if __name__ == "__main__":
    main()
