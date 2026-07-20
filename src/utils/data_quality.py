"""
Reusable data quality check functions.
Logs metrics to a DQ log Delta table and quarantines bad records.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from functools import reduce
from datetime import datetime, timezone


DQ_LOG_TABLE = "olist_silver.dq_log"
QUARANTINE_TABLE = "olist_silver.quarantine"


def log_dq_metrics(
    spark: SparkSession,
    table_name: str,
    layer: str,
    input_count: int,
    output_count: int,
    quarantined_count: int,
    null_counts: dict,
):
    """Log data quality metrics to the DQ log table."""
    metrics = [{
        "table_name": table_name,
        "layer": layer,
        "input_count": input_count,
        "output_count": output_count,
        "quarantined_count": quarantined_count,
        "null_counts": str(null_counts),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
    }]
    df = spark.createDataFrame(metrics)
    df.write.mode("append").format("delta").saveAsTable(DQ_LOG_TABLE)


def check_nulls(df: DataFrame, columns: list) -> dict:
    """Return null counts for specified columns (single Spark action)."""
    agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
    result = df.agg(*agg_exprs).collect()[0]
    return {c: result[c] for c in columns}


def quarantine_records(
    spark: SparkSession,
    df: DataFrame,
    condition,
    reason: str,
    source_table: str,
) -> tuple:
    """
    Split a DataFrame into good and bad records.
    Bad records are written to the quarantine table.
    Returns (good_df, quarantined_count).
    Condition can be a Column expression or a SQL string.
    """
    bad_df = df.filter(condition)
    good_df = df.filter(~condition if isinstance(condition, Column) else f"NOT ({condition})")

    quarantined_count = bad_df.count()

    if quarantined_count > 0:
        quarantine_df = bad_df.select(
            F.lit(source_table).alias("source_table"),
            F.lit(reason).alias("rejection_reason"),
            F.current_timestamp().alias("quarantine_timestamp"),
            F.to_json(F.struct("*")).alias("record_json"),
        )
        quarantine_df.write.mode("append").format("delta").saveAsTable(QUARANTINE_TABLE)

    return good_df, quarantined_count


def run_dq_checks(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    layer: str,
    primary_keys: list,
    not_null_columns: list,
) -> DataFrame:
    """
    Run standard DQ checks: dedup + null quarantine + logging.
    Returns the cleaned DataFrame.
    """
    input_count = df.count()

    # Deduplicate
    df = df.dropDuplicates(primary_keys)

    # Check nulls BEFORE quarantine so counts reflect actual bad data
    null_counts = check_nulls(df, not_null_columns)

    # Quarantine records with nulls in required columns
    null_condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in not_null_columns])
    df, quarantined_count = quarantine_records(
        spark, df, null_condition,
        reason=f"Null value in required column(s): {not_null_columns}",
        source_table=table_name,
    )

    output_count = df.count()

    # Log metrics
    log_dq_metrics(
        spark, table_name, layer,
        input_count, output_count, quarantined_count, null_counts,
    )

    return df


def enforce_schema(df: DataFrame, schema) -> DataFrame:
    """
    Select and cast columns to match a target schema.
    Ensures output has exactly the right columns in the right order with correct types.
    Missing columns raise Spark AnalysisException. Invalid casts silently return null.
    """
    selected = []
    for field in schema.fields:
        selected.append(F.col(field.name).cast(field.dataType).alias(field.name))
    return df.select(selected)
