# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - aggregations, dimensions, BI views
# MAGIC
# MAGIC Two bugs shaped this rewrite:
# MAGIC
# MAGIC * Hourly aggregation called `HOUR(order_date)`, but Silver casts `order_date` to DATE,
# MAGIC   so `HOUR()` returned **0 for every row** and the whole table collapsed into one bucket
# MAGIC   per day. Silver now carries `order_timestamp`, and the hourly rollup reads that.
# MAGIC * The hourly table was written with `mode("append")` while re-aggregating all of Silver
# MAGIC   on every hourly run, so each run appended a complete duplicate set. All three
# MAGIC   aggregates are now overwritten, which is idempotent.

# COMMAND ----------

import logging

from pyspark.sql import DataFrame, SparkSession

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


def create_gold_tables(spark: SparkSession) -> None:
    """Create Gold fact and dimension tables."""
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("gold", "gold_orders_daily")} (
            order_date DATE,
            order_day_key STRING COMMENT 'yyyyMMdd, joins to dim_date.date_key',
            total_orders LONG,
            completed_orders LONG,
            failed_orders LONG,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            min_order_value DOUBLE,
            max_order_value DOUBLE,
            unique_customers LONG,
            processing_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (order_date)
        COMMENT 'Daily order metrics'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("gold", "gold_orders_by_user")} (
            user_id STRING,
            total_orders LONG,
            completed_orders LONG,
            total_spent DOUBLE,
            avg_completed_order_value DOUBLE,
            first_order_date DATE,
            last_order_date DATE,
            customer_lifetime_value DOUBLE,
            order_frequency_days DOUBLE,
            is_active BOOLEAN,
            processing_timestamp TIMESTAMP
        )
        USING DELTA
        COMMENT 'Customer-level aggregates for CRM and marketing'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("gold", "gold_orders_hourly")} (
            order_date DATE,
            order_hour INT,
            hour_timestamp TIMESTAMP,
            order_count LONG,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            processing_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (order_date)
        COMMENT 'Hourly rollup for real-time dashboards'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("gold", "dim_date")} (
            date_key STRING,
            full_date DATE,
            year INT,
            month INT,
            day INT,
            day_of_week INT,
            quarter INT,
            week_of_year INT,
            is_weekend BOOLEAN,
            month_name STRING,
            day_name STRING
        )
        USING DELTA
        COMMENT 'Date dimension for time-series joins'
        """,
    ]

    for sql in statements:
        spark.sql(sql)
    logger.info("Created/verified Gold tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations

# COMMAND ----------


def aggregate_daily_orders(spark: SparkSession, source_table: str) -> DataFrame:
    """Daily order metrics from current Silver rows."""
    return spark.sql(f"""
        SELECT
            order_date,
            DATE_FORMAT(order_date, 'yyyyMMdd')                                     AS order_day_key,
            COUNT(*)                                                                AS total_orders,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)                   AS completed_orders,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)                      AS failed_orders,
            ROUND(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 2)    AS total_revenue,
            ROUND(AVG(amount), 2)                                                   AS avg_order_value,
            ROUND(MIN(amount), 2)                                                   AS min_order_value,
            ROUND(MAX(amount), 2)                                                   AS max_order_value,
            COUNT(DISTINCT user_id)                                                 AS unique_customers,
            current_timestamp()                                                     AS processing_timestamp
        FROM {source_table}
        WHERE dbt_is_current = TRUE
        GROUP BY order_date
    """)


def aggregate_by_user(spark: SparkSession, source_table: str) -> DataFrame:
    """Customer-level aggregates.

    avg_completed_order_value is restricted to completed orders so it is
    consistent with total_spent; the old avg_order_value averaged across
    failed and cancelled orders while revenue counted only completed ones.
    """
    return spark.sql(f"""
        SELECT
            user_id,
            COUNT(*)                                                                AS total_orders,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)                   AS completed_orders,
            ROUND(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 2)    AS total_spent,
            ROUND(AVG(CASE WHEN status = 'completed' THEN amount END), 2)           AS avg_completed_order_value,
            MIN(order_date)                                                         AS first_order_date,
            MAX(order_date)                                                         AS last_order_date,
            ROUND(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 2)    AS customer_lifetime_value,
            ROUND(
                DATEDIFF(MAX(order_date), MIN(order_date)) / NULLIF(COUNT(*) - 1, 0),
                2
            )                                                                       AS order_frequency_days,
            DATEDIFF(CURRENT_DATE(), MAX(order_date)) <= 30                         AS is_active,
            current_timestamp()                                                     AS processing_timestamp
        FROM {source_table}
        WHERE dbt_is_current = TRUE
        GROUP BY user_id
    """)


def aggregate_hourly_orders(spark: SparkSession, source_table: str) -> DataFrame:
    """Hourly rollup keyed off order_timestamp, not order_date."""
    return spark.sql(f"""
        SELECT
            CAST(order_timestamp AS DATE)                                           AS order_date,
            HOUR(order_timestamp)                                                   AS order_hour,
            DATE_TRUNC('HOUR', order_timestamp)                                     AS hour_timestamp,
            COUNT(*)                                                                AS order_count,
            ROUND(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 2)    AS total_revenue,
            ROUND(AVG(amount), 2)                                                   AS avg_order_value,
            current_timestamp()                                                     AS processing_timestamp
        FROM {source_table}
        WHERE dbt_is_current = TRUE
          AND order_timestamp IS NOT NULL
        GROUP BY CAST(order_timestamp AS DATE), HOUR(order_timestamp), DATE_TRUNC('HOUR', order_timestamp)
    """)


def create_date_dimension(spark: SparkSession) -> DataFrame:
    """Date dimension spanning five years either side of today."""
    return spark.sql("""
        WITH dates AS (
            SELECT explode(
                sequence(
                    date_add(CURRENT_DATE(), -365 * 5),
                    date_add(CURRENT_DATE(),  365 * 5),
                    INTERVAL 1 DAY
                )
            ) AS full_date
        )
        SELECT
            DATE_FORMAT(full_date, 'yyyyMMdd')                  AS date_key,
            full_date,
            YEAR(full_date)                                     AS year,
            MONTH(full_date)                                    AS month,
            DAY(full_date)                                      AS day,
            DAYOFWEEK(full_date)                                AS day_of_week,
            QUARTER(full_date)                                  AS quarter,
            WEEKOFYEAR(full_date)                               AS week_of_year,
            DAYOFWEEK(full_date) IN (1, 7)                      AS is_weekend,
            DATE_FORMAT(full_date, 'MMMM')                      AS month_name,
            DATE_FORMAT(full_date, 'EEEE')                      AS day_name
        FROM dates
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BI views
# MAGIC
# MAGIC `ORDER BY` and `LIMIT` are deliberately left out of the view bodies. Both are the
# MAGIC consumer's business, and a `LIMIT` inside a view silently truncates results for every
# MAGIC caller - the previous `v_top_customers` capped at 1000 rows and `v_hourly_trends` at 24.

# COMMAND ----------


def create_analytics_views(spark: SparkSession) -> None:
    """BI-facing views over the Gold tables."""
    views = {
        cfg.table("analytics", "v_daily_orders"): f"""
            CREATE OR REPLACE VIEW {cfg.table("analytics", "v_daily_orders")} AS
            SELECT
                d.full_date,
                d.day_name,
                d.is_weekend,
                agg.total_orders,
                agg.completed_orders,
                agg.failed_orders,
                agg.total_revenue,
                agg.avg_order_value,
                agg.unique_customers
            FROM {cfg.table("gold", "gold_orders_daily")} agg
            INNER JOIN {cfg.table("gold", "dim_date")} d
                ON agg.order_day_key = d.date_key
        """,
        cfg.table("analytics", "v_top_customers"): f"""
            CREATE OR REPLACE VIEW {cfg.table("analytics", "v_top_customers")} AS
            SELECT
                user_id,
                total_orders,
                completed_orders,
                total_spent,
                avg_completed_order_value,
                first_order_date,
                last_order_date,
                customer_lifetime_value,
                order_frequency_days,
                is_active,
                RANK() OVER (ORDER BY customer_lifetime_value DESC) AS customer_rank
            FROM {cfg.table("gold", "gold_orders_by_user")}
            WHERE customer_lifetime_value > 0
        """,
        cfg.table("analytics", "v_hourly_trends"): f"""
            CREATE OR REPLACE VIEW {cfg.table("analytics", "v_hourly_trends")} AS
            SELECT
                hour_timestamp,
                order_date,
                order_hour,
                order_count,
                total_revenue,
                avg_order_value,
                LAG(order_count) OVER (ORDER BY hour_timestamp) AS prev_hour_count,
                ROUND(
                    100 * (order_count - LAG(order_count) OVER (ORDER BY hour_timestamp))
                        / NULLIF(LAG(order_count) OVER (ORDER BY hour_timestamp), 0),
                    2
                ) AS hour_over_hour_change_pct
            FROM {cfg.table("gold", "gold_orders_hourly")}
        """,
    }

    for view_name, sql in views.items():
        spark.sql(sql)
        logger.info(f"Created/replaced view: {view_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration

# COMMAND ----------


def process_gold_layer(spark: SparkSession, source_table: str) -> None:
    """Silver -> Gold.

    Every aggregate is a full recompute from current Silver rows and is written
    with overwrite, so re-running produces the same result rather than
    accumulating duplicates.
    """
    logger.info(f"Starting Gold aggregation from {source_table}")

    targets = [
        (aggregate_daily_orders(spark, source_table), cfg.table("gold", "gold_orders_daily")),
        (aggregate_by_user(spark, source_table), cfg.table("gold", "gold_orders_by_user")),
        (aggregate_hourly_orders(spark, source_table), cfg.table("gold", "gold_orders_hourly")),
    ]

    for df, table_name in targets:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        logger.info(f"Wrote {table_name}")

    create_date_dimension(spark).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(cfg.table("gold", "dim_date"))
    logger.info("Refreshed date dimension")

    create_analytics_views(spark)
    logger.info("Gold layer processing completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

create_gold_tables(spark)
process_gold_layer(spark, source_table=cfg.table("silver", "silver_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(spark.table(cfg.table("gold", "gold_orders_daily")))

# COMMAND ----------

display(spark.table(cfg.table("analytics", "v_top_customers")))
