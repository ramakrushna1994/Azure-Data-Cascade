"""Shared pytest fixtures.

Tests split into two groups:

* pipeline_config tests need no Spark at all and run in milliseconds.
* transform tests need a local SparkSession, which takes ~10s to start, so it
  is session-scoped and skipped entirely when pyspark is not installed.
"""

import os
import sys

import pytest

# Import modules from the repo root, since there is no package layout.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for transform tests."""
    pyspark = pytest.importorskip(
        "pyspark", reason="pyspark not installed; transform tests skipped"
    )

    # The JVM launches a Python worker that must connect back to the driver
    # over a local socket. Without pinning the interpreter it can pick a
    # different python (or none), and the driver fails with
    # "Python worker failed to connect back / Accept timed out" - which is
    # especially common on Windows and inside virtualenvs.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("cascade-tests")
        # A single shuffle partition keeps small-frame tests fast.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze_rows(spark):
    """A Bronze-shaped frame covering every validation outcome.

    Mirrors what kafka_producer.py emits, so these tests exercise the same
    cases the live pipeline was verified against.
    """
    from datetime import datetime

    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("_bronze_ingestion_time", TimestampType(), True),
        StructField("_load_id", StringType(), True),
    ])

    t = datetime(2026, 9, 6, 14, 30, 0)
    rows = [
        # valid
        ("ORD-001", "USR-123", 99.99, "2026-09-06", "completed", t, "load-1"),
        # valid, but uppercase status - the regression that started this
        ("ORD-002", "USR-124", 149.50, "2026-09-06", "PENDING", t, "load-1"),
        # invalid: negative amount
        ("ORD-003", "USR-125", -50.0, "2026-09-06", "failed", t, "load-1"),
        # invalid: null user
        ("ORD-004", None, 75.25, "2026-09-06", "completed", t, "load-1"),
        # invalid: date present but unparseable
        ("ORD-005", "USR-127", 200.0, "09/06/2026", "completed", t, "load-1"),
        # invalid: unknown status
        ("ORD-006", "USR-128", 30.0, "2026-09-06", "refunded", t, "load-1"),
        # valid, with padding and casing that normalization must fix
        ("  ord-007 ", " usr-129 ", 45.0, "2026-09-06", " Completed ", t, "load-1"),
    ]
    return spark.createDataFrame(rows, schema)
