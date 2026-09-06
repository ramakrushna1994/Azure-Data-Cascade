# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - validation, cleaning, deduplication, SCD Type 2
# MAGIC
# MAGIC **Ordering matters here, and it used to be wrong.** Validation ran against raw Bronze
# MAGIC values while normalization ran afterwards, so a status of `"COMPLETED"` was quarantined
# MAGIC as `invalid_status` even though the very next step would have lowercased it.
# MAGIC Normalization now runs first and validation judges the normalized values.
# MAGIC
# MAGIC The same reordering fixes malformed dates. `to_date()` previously turned an unparseable
# MAGIC date into NULL *after* validation had approved the raw string, so the row landed in the
# MAGIC NULL partition of a table partitioned by `order_date`. Date parsing is now part of
# MAGIC normalization and a parse failure is a validation failure, so the row is quarantined.
# MAGIC
# MAGIC Processing is **incremental**. The previous version read the entire Bronze table on
# MAGIC every 15-minute run and re-merged all of history.

# COMMAND ----------

import logging
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    lit,
    lower,
    md5,
    row_number,
    struct,
    to_date,
    to_json,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DoubleType

import pipeline_config as cfg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.getOrCreate()

VALID_STATUSES = ["pending", "completed", "failed", "cancelled"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table definitions

# COMMAND ----------


def create_silver_tables(spark: SparkSession) -> None:
    """Create Silver tables and the watermark tracking table."""
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("silver", "silver_orders")} (
            order_id STRING,
            user_id STRING,
            amount DOUBLE,
            order_date DATE,
            order_timestamp TIMESTAMP COMMENT 'Event time, carried from Bronze for hourly rollups',
            status STRING,
            order_key STRING COMMENT 'Hash of the natural key',
            dbt_valid_from TIMESTAMP COMMENT 'SCD2 validity start',
            dbt_valid_to TIMESTAMP COMMENT 'SCD2 validity end',
            dbt_is_current BOOLEAN COMMENT 'SCD2 current flag',
            _silver_processing_time TIMESTAMP,
            _load_id STRING
        )
        USING DELTA
        PARTITIONED BY (order_date)
        COMMENT 'Cleaned, deduplicated orders with SCD Type 2 history'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("silver", "silver_orders_quarantine")} (
            original_data STRING,
            error_message STRING,
            error_type STRING,
            bronze_load_id STRING,
            quarantine_timestamp TIMESTAMP
        )
        USING DELTA
        COMMENT 'Records rejected by Silver validation'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("monitoring", "silver_watermarks")} (
            source_table STRING,
            last_processed_time TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'High-water mark per source, so Silver only reads new Bronze rows'
        """,
    ]

    for sql in statements:
        spark.sql(sql)
    logger.info("Created/verified Silver tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental read

# COMMAND ----------


def get_watermark(spark: SparkSession, source_table: str) -> Optional[datetime]:
    """Latest Bronze ingestion time already folded into Silver."""
    rows = spark.sql(f"""
        SELECT last_processed_time
        FROM {cfg.table("monitoring", "silver_watermarks")}
        WHERE source_table = '{source_table}'
    """).collect()
    return rows[0]["last_processed_time"] if rows else None


def set_watermark(spark: SparkSession, source_table: str, value: datetime) -> None:
    """Advance the high-water mark for a source table."""
    watermark_table = cfg.table("monitoring", "silver_watermarks")
    spark.sql(f"""
        MERGE INTO {watermark_table} t
        USING (
            SELECT
                '{source_table}'              AS source_table,
                TIMESTAMP'{value}'            AS last_processed_time,
                current_timestamp()           AS updated_at
        ) s
        ON t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET
            last_processed_time = s.last_processed_time,
            updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def read_new_bronze(spark: SparkSession, source_table: str) -> DataFrame:
    """Read only Bronze rows newer than the watermark."""
    df = spark.table(source_table)
    watermark = get_watermark(spark, source_table)

    if watermark is None:
        logger.info(f"No watermark for {source_table} - processing all rows")
        return df

    logger.info(f"Reading {source_table} rows newer than {watermark}")
    return df.filter(col("_bronze_ingestion_time") > lit(watermark))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize, then validate
# MAGIC
# MAGIC The order of these two functions is the fix, not an accident.

# COMMAND ----------


def normalize_orders(df: DataFrame) -> DataFrame:
    """Standardize types and casing before anything is judged valid.

    order_date is parsed into order_date_parsed rather than overwritten, so
    validation can distinguish "absent" from "present but unparseable".
    """
    return (
        df
        .withColumn("order_id", trim(upper(col("order_id"))))
        .withColumn("user_id", trim(upper(col("user_id"))))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("order_date_parsed", to_date(col("order_date"), "yyyy-MM-dd"))
    )


def validate_orders(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split normalized rows into valid and invalid.

    Expects normalize_orders() to have run first.
    """
    checked = df.withColumn(
        "is_valid",
        col("order_id").isNotNull()
        & (col("order_id") != "")
        & col("user_id").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") >= 0)
        & col("order_date_parsed").isNotNull()
        & col("status").isin(VALID_STATUSES),
    ).withColumn(
        "failure_reason",
        when(col("order_id").isNull() | (col("order_id") == ""), "missing_order_id")
        .when(col("user_id").isNull(), "missing_user_id")
        .when(col("amount").isNull(), "missing_amount")
        .when(col("amount") < 0, "negative_amount")
        .when(col("order_date").isNull(), "missing_order_date")
        # Reached only when the raw value was present but would not parse.
        .when(col("order_date_parsed").isNull(), "unparseable_order_date")
        .when(~col("status").isin(VALID_STATUSES), "invalid_status")
        .otherwise(lit(None)),
    )

    valid = checked.filter(col("is_valid")).drop("is_valid", "failure_reason")
    invalid = checked.filter(~col("is_valid")).withColumnRenamed(
        "failure_reason", "error_message"
    ).drop("is_valid")

    return valid, invalid


def deduplicate_orders(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested row per natural key."""
    window = Window.partitionBy("order_id", "user_id").orderBy(
        col("_bronze_ingestion_time").desc()
    )
    return df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")


def quarantine_invalid_records(df: DataFrame, table: str) -> None:
    """Persist rejected rows with their reason for later investigation."""
    quarantine_df = df.select(
        to_json(struct("*")).alias("original_data"),
        col("error_message"),
        lit("validation_error").alias("error_type"),
        coalesce(col("_load_id"), lit("unknown")).alias("bronze_load_id"),
        current_timestamp().alias("quarantine_timestamp"),
    )

    quarantine_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 merge
# MAGIC
# MAGIC Two statements are required: a Delta MERGE can touch a given target row only once, so
# MAGIC it cannot both expire the old version and insert its replacement in one pass.

# COMMAND ----------


def scd_type_2_merge(spark: SparkSession, new_df: DataFrame, target_table: str) -> None:
    """Track attribute changes as SCD Type 2 rows."""
    new_df.createOrReplaceTempView("new_orders")

    # Expire changed rows, and insert keys that are genuinely new.
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING new_orders n
          ON  t.order_id = n.order_id
          AND t.user_id  = n.user_id
          AND t.dbt_is_current = TRUE
        WHEN MATCHED AND (
                t.amount     != n.amount OR
                t.status     != n.status OR
                t.order_date != n.order_date
            ) THEN UPDATE SET
                dbt_valid_to  = current_timestamp(),
                dbt_is_current = FALSE
        WHEN NOT MATCHED THEN INSERT (
            order_id, user_id, amount, order_date, order_timestamp, status, order_key,
            dbt_valid_from, dbt_valid_to, dbt_is_current,
            _silver_processing_time, _load_id
        ) VALUES (
            n.order_id, n.user_id, n.amount, n.order_date, n.order_timestamp, n.status, n.order_key,
            current_timestamp(), NULL, TRUE,
            current_timestamp(), n._load_id
        )
    """)

    # Insert the replacement version for keys just expired. The NOT EXISTS
    # guard keeps this idempotent if the same batch is reprocessed.
    spark.sql(f"""
        INSERT INTO {target_table}
        SELECT
            n.order_id, n.user_id, n.amount, n.order_date, n.order_timestamp, n.status, n.order_key,
            current_timestamp(), NULL, TRUE,
            current_timestamp(), n._load_id
        FROM new_orders n
        WHERE EXISTS (
            SELECT 1 FROM {target_table} t
            WHERE t.order_id = n.order_id AND t.user_id = n.user_id
              AND t.dbt_is_current = FALSE
        )
        AND NOT EXISTS (
            SELECT 1 FROM {target_table} t
            WHERE t.order_id = n.order_id AND t.user_id = n.user_id
              AND t.dbt_is_current = TRUE
        )
    """)

    logger.info(f"SCD Type 2 merge completed for {target_table}")


def log_quality_metrics(spark: SparkSession, target_table: str) -> None:
    """Record Silver quality metrics.

    These used to be computed FROM the Bronze source table while being written
    to a table named silver_quality_metrics.
    """
    metrics_table = cfg.table("monitoring", "silver_quality_metrics")

    spark.sql(f"""
        SELECT
            '{target_table}'                                            AS table_name,
            current_timestamp()                                         AS metric_time,
            COUNT(*)                                                    AS total_rows,
            SUM(CASE WHEN dbt_is_current THEN 1 ELSE 0 END)             AS current_rows,
            COUNT(DISTINCT order_id)                                    AS unique_orders,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END)                 AS negative_amounts,
            SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END)         AS null_order_dates
        FROM {target_table}
    """).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(metrics_table)

    logger.info(f"Logged quality metrics to {metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration

# COMMAND ----------


def process_orders(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    quarantine_table: str,
) -> None:
    """Bronze -> Silver for orders."""
    logger.info(f"Processing {source_table} -> {target_table}")

    bronze_df = read_new_bronze(spark, source_table)

    # One pass to find both the batch high-water mark and whether there is
    # anything to do, instead of repeatedly counting a lazy DataFrame.
    bounds = bronze_df.selectExpr(
        "COUNT(*) AS row_count",
        "MAX(_bronze_ingestion_time) AS max_ingestion_time",
    ).collect()[0]

    if bounds["row_count"] == 0:
        logger.info("No new Bronze rows - nothing to do")
        return

    logger.info(f"Read {bounds['row_count']} new Bronze rows")

    normalized = normalize_orders(bronze_df)
    valid_df, invalid_df = validate_orders(normalized)

    # Cached because both branches are consumed twice: once to count for the
    # log line, once to write. Without this the whole Bronze read replays.
    valid_df.persist()
    invalid_df.persist()

    try:
        invalid_count = invalid_df.count()
        if invalid_count:
            quarantine_invalid_records(invalid_df, quarantine_table)
            logger.info(f"Quarantined {invalid_count} invalid rows")

        final_df = (
            deduplicate_orders(valid_df)
            .withColumn("order_date", col("order_date_parsed"))
            .withColumn("order_timestamp", col("_bronze_ingestion_time"))
            .withColumn("order_key", md5(concat_ws("|", col("order_id"), col("user_id"))))
            .withColumn("_silver_processing_time", current_timestamp())
            .select(
                "order_id", "user_id", "amount", "order_date", "order_timestamp",
                "status", "order_key", "_silver_processing_time", "_load_id",
            )
        )

        scd_type_2_merge(spark, final_df, target_table)
    finally:
        valid_df.unpersist()
        invalid_df.unpersist()

    set_watermark(spark, source_table, bounds["max_ingestion_time"])
    log_quality_metrics(spark, target_table)

    logger.info(f"Completed {source_table} -> {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

create_silver_tables(spark)

process_orders(
    spark,
    source_table=cfg.table("bronze", "bronze_orders_kafka"),
    target_table=cfg.table("silver", "silver_orders"),
    quarantine_table=cfg.table("silver", "silver_orders_quarantine"),
)

logger.info("Silver layer transformation completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify - valid rows vs quarantined

# COMMAND ----------

display(spark.table(cfg.table("silver", "silver_orders")))

# COMMAND ----------

display(
    spark.table(cfg.table("silver", "silver_orders_quarantine"))
    .select("error_message", "bronze_load_id", "quarantine_timestamp")
)
