"""Pure Silver-layer transforms, extracted so they can be tested.

These live outside silver_layer.py because that is a Databricks notebook: its
final cells call create_silver_tables() and process_orders(), which execute on
import. A notebook cannot be imported by a test without running the pipeline.

Everything here is a DataFrame -> DataFrame function with no I/O, no table
reads, and no SparkSession construction, so tests only need a local session.

Ordering note, which is the whole point of splitting normalize from validate:
normalize_orders() must run first. Validating raw values quarantined a status
of "COMPLETED" as invalid_status even though the next step would have
lowercased it, and approved an unparseable date that to_date() then silently
turned into NULL inside a partition column.
"""

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    lit,
    lower,
    md5,
    row_number,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DoubleType

VALID_STATUSES = ["pending", "completed", "failed", "cancelled"]


def normalize_orders(df: DataFrame) -> DataFrame:
    """Standardize types and casing before anything is judged valid.

    order_date is parsed into a separate order_date_parsed column rather than
    overwritten, so validation can tell "absent" apart from "present but
    unparseable".
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
    """Split normalized rows into (valid, invalid).

    Expects normalize_orders() to have run first. The invalid frame carries an
    error_message naming the first rule the row broke.
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
    invalid = (
        checked.filter(~col("is_valid"))
        .withColumnRenamed("failure_reason", "error_message")
        .drop("is_valid")
    )
    return valid, invalid


def deduplicate_orders(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested row per natural key."""
    window = Window.partitionBy("order_id", "user_id").orderBy(
        col("_bronze_ingestion_time").desc()
    )
    return (
        df.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def finalize_orders(df: DataFrame) -> DataFrame:
    """Shape deduplicated rows into the Silver table's columns.

    order_timestamp is carried from Bronze because Gold's hourly rollup needs
    an actual timestamp; order_date is a DATE, and HOUR() of a DATE is always
    zero.
    """
    return (
        df
        .withColumn("order_date", col("order_date_parsed"))
        .withColumn("order_timestamp", col("_bronze_ingestion_time"))
        .withColumn("order_key", md5(concat_ws("|", col("order_id"), col("user_id"))))
        .withColumn("_silver_processing_time", current_timestamp())
        .select(
            "order_id", "user_id", "amount", "order_date", "order_timestamp",
            "status", "order_key", "_silver_processing_time", "_load_id",
        )
    )
