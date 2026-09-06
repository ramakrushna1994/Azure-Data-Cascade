# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - raw data landing zone
# MAGIC
# MAGIC Owns the Bronze table definitions and ingestion metrics. Actual ingestion lives in
# MAGIC the source-specific notebooks (`kafka_consumer`, `api_ingestion`, `file_ingestion`);
# MAGIC this notebook used to duplicate that logic against a hardcoded `localhost` broker,
# MAGIC which has been removed.
# MAGIC
# MAGIC **Partitioning note.** These tables previously partitioned by `_bronze_ingestion_time`,
# MAGIC a TIMESTAMP from `current_timestamp()`. That creates one partition directory per
# MAGIC distinct microsecond - a small-file explosion that makes the table unqueryable within
# MAGIC hours. Partitioning is now on `_bronze_ingestion_date` (DATE), which the writers derive
# MAGIC from the same timestamp.

# COMMAND ----------

import logging

from pyspark.sql import SparkSession

# pipeline_config is a workspace *file*, not a notebook, so it can be imported
# as a module. Every notebook builds table names through it.
import pipeline_config as cfg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table definitions

# COMMAND ----------


def _table_ddl(table_name: str, source_columns: str, comment: str) -> str:
    """Build a Bronze CREATE TABLE with the shared metadata columns."""
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id STRING,
            user_id STRING,
            amount DOUBLE,
            order_date STRING,
            status STRING,
{source_columns}
            _bronze_ingestion_time TIMESTAMP,
            _bronze_ingestion_date DATE COMMENT 'Partition key - date form of ingestion time',
            _source_system STRING,
            _processing_timestamp TIMESTAMP,
            _load_id STRING COMMENT 'Identifies the batch that wrote this row'
        )
        USING DELTA
        PARTITIONED BY (_bronze_ingestion_date)
        COMMENT '{comment}'
    """


def create_bronze_tables(spark: SparkSession) -> None:
    """Create Bronze layer tables if they do not exist."""
    tables = {
        cfg.table("bronze", "bronze_orders_kafka"): _table_ddl(
            cfg.table("bronze", "bronze_orders_kafka"),
            "            _ingested_at STRING,\n"
            "            _kafka_partition INT,\n"
            "            _kafka_offset LONG,",
            "Raw orders from the Kafka stream",
        ),
        cfg.table("bronze", "bronze_orders_api"): _table_ddl(
            cfg.table("bronze", "bronze_orders_api"),
            "            _api_source STRING,\n"
            "            _api_ingestion_timestamp STRING,",
            "Raw orders from the REST API",
        ),
        cfg.table("bronze", "bronze_orders_files"): _table_ddl(
            cfg.table("bronze", "bronze_orders_files"),
            "            _file_path STRING,",
            "Raw orders from cloud files",
        ),
    }

    for table_name, create_sql in tables.items():
        spark.sql(create_sql)
        logger.info(f"Created/verified table: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion metrics

# COMMAND ----------


def log_ingestion_metrics(spark: SparkSession) -> None:
    """Record per-table ingestion counts to the monitoring schema."""
    metrics_table = cfg.table("monitoring", "bronze_ingestion_metrics")
    source_table = cfg.table("bronze", "bronze_orders_kafka")

    # The old version grouped by current_timestamp() and took MIN/MAX of
    # order_date as a STRING, which sorts lexicographically. order_date is
    # cast before comparison here.
    metrics_df = spark.sql(f"""
        SELECT
            current_timestamp()                          AS metric_time,
            '{source_table}'                             AS table_name,
            COUNT(*)                                     AS record_count,
            COUNT(DISTINCT order_id)                     AS unique_orders,
            MIN(TRY_CAST(order_date AS DATE))            AS earliest_order,
            MAX(TRY_CAST(order_date AS DATE))            AS latest_order,
            MAX(_bronze_ingestion_time)                  AS last_ingestion_time
        FROM {source_table}
    """)

    metrics_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(metrics_table)

    logger.info(f"Logged ingestion metrics to {metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

create_bronze_tables(spark)
log_ingestion_metrics(spark)
logger.info("Bronze layer setup completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cfg.schema('bronze')}"))
