"""
Agent tools — read-only Spark wrappers the AI uses to inspect and debug the Medallion pipeline.

Each function takes a SparkSession and returns a human-readable string. The strings are what
the LLM reads, so they are formatted for reasoning rather than for machine parsing.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import AnalysisException

from utils.data_quality import DQ_LOG_TABLE, QUARANTINE_TABLE
from utils.schema_definitions import BRONZE_TABLES, SILVER_TABLES, GOLD_TABLES

_ALLOWED_TABLES = (
    set(BRONZE_TABLES.values()) | set(SILVER_TABLES.values()) | set(GOLD_TABLES.values())
    | {DQ_LOG_TABLE, QUARANTINE_TABLE}
)
_MAX_OUTPUT_CHARS = 4000


def _exists(spark: SparkSession, table: str) -> bool:
    try:
        return spark.catalog.tableExists(table)
    except AnalysisException:
        return False


def inspect_table(spark: SparkSession, table_name: str) -> str:
    """Report row count, schema, and per-column null counts for a table."""
    # Resolve short name to fully-qualified table (Silver wins on ties)
    table = table_name
    if "." not in table_name:
        for mapping in (SILVER_TABLES, BRONZE_TABLES, GOLD_TABLES):
            if table_name in mapping:
                table = mapping[table_name]
                break
    if table not in _ALLOWED_TABLES:
        return f"Access denied: '{table}' is not in the allowed table list."
    if not _exists(spark, table):
        return f"Table '{table}' does not exist."

    df = spark.table(table)
    count = df.count()
    schema_lines = [f"  {f.name}: {f.dataType.simpleString()}" for f in df.schema.fields]

    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    nulls = df.agg(*null_exprs).collect()[0].asDict()
    null_lines = [f"  {c}: {n}" for c, n in nulls.items() if n]

    out = [f"Table: {table}", f"Row count: {count}", "Schema:", *schema_lines]
    out += ["Null counts:", *null_lines] if null_lines else ["No nulls found."]
    return "\n".join(out)


def read_dq_logs(spark: SparkSession, table_name: str = "", limit: int = 10) -> str:
    """Read recent data-quality metrics from the DQ log. Optionally filter by table name."""
    if not _exists(spark, DQ_LOG_TABLE):
        return f"DQ log table '{DQ_LOG_TABLE}' does not exist yet — run the pipeline first."

    limit = min(limit, 100)
    df = spark.table(DQ_LOG_TABLE)
    if table_name:
        df = df.filter(F.col("table_name") == table_name)

    rows = df.orderBy(F.col("run_timestamp").desc()).limit(limit).collect()
    if not rows:
        return "No DQ log entries found."

    lines = ["Recent DQ log entries (newest first):"]
    for r in rows:
        lines.append(
            f"- [{r['run_timestamp']}] {r['table_name']} ({r['layer']}): "
            f"in={r['input_count']} out={r['output_count']} "
            f"quarantined={r['quarantined_count']} nulls={r['null_counts']}"
        )
    return "\n".join(lines)


def read_quarantine(spark: SparkSession, source_table: str = "") -> str:
    """Summarize quarantined records by rejection reason, with a sample payload."""
    if not _exists(spark, QUARANTINE_TABLE):
        return f"Quarantine table '{QUARANTINE_TABLE}' does not exist yet."

    df = spark.table(QUARANTINE_TABLE)
    if source_table:
        df = df.filter(F.col("source_table") == source_table)

    total = df.count()
    if total == 0:
        return "Quarantine is empty — no bad records."

    reasons = (
        df.groupBy("source_table", "rejection_reason")
        .count()
        .orderBy(F.col("count").desc())
        .collect()
    )
    lines = [f"Quarantine holds {total} record(s):"]
    for r in reasons:
        lines.append(f"- {r['source_table']}: {r['rejection_reason']} → {r['count']}")

    return "\n".join(lines)


def compare_counts(spark: SparkSession) -> str:
    """Compare Bronze vs Silver row counts per table to reveal data loss."""
    lines = ["Layer row-count comparison (Bronze → Silver):"]
    for name, silver in SILVER_TABLES.items():
        bronze = BRONZE_TABLES.get(name)
        b = spark.table(bronze).count() if _exists(spark, bronze) else None
        s = spark.table(silver).count() if _exists(spark, silver) else None
        if b is None or s is None:
            lines.append(f"- {name}: bronze={b} silver={s} (missing table)")
            continue
        lost = b - s
        pct = (lost / b * 100) if b else 0
        lines.append(f"- {name}: bronze={b} silver={s} lost={lost} ({pct:.1f}%)")
    return "\n".join(lines)


def trace_record(spark: SparkSession, order_id: str) -> str:
    """Follow a single order_id through Bronze, Silver, and the quarantine table."""
    lines = [f"Tracing order_id='{order_id}':"]

    for layer, table in (("Bronze", BRONZE_TABLES["orders"]), ("Silver", SILVER_TABLES["orders"])):
        if not _exists(spark, table):
            lines.append(f"- {layer} ({table}): table missing")
            continue
        n = spark.table(table).filter(F.col("order_id") == order_id).count()
        lines.append(f"- {layer} ({table}): {'found' if n else 'NOT found'} ({n} row(s))")

    if _exists(spark, QUARANTINE_TABLE):
        q = spark.table(QUARANTINE_TABLE).filter(
            F.col("record_json").contains(f'"order_id": "{order_id}"')
        ).count()
        if q:
            lines.append(f"- Quarantine: {q} matching record(s) — this order was rejected.")

    result = "\n".join(lines)
    return result[:_MAX_OUTPUT_CHARS]



