"""Tests for the Silver transforms.

Every test here corresponds to a bug that reached production behaviour:

* Validation ran before normalization, so "COMPLETED" was quarantined as
  invalid_status even though the next step lowercased it.
* to_date() turned an unparseable date into NULL *after* validation approved
  the raw string, so the row landed in the NULL partition of a table
  partitioned by order_date instead of being quarantined.
* Gold's hourly rollup called HOUR(order_date) on a DATE, always returning 0,
  because Silver did not carry a timestamp.

Requires a local SparkSession; skipped automatically without pyspark.
"""

import pytest

# transforms imports pyspark at module level, so this must run before the
# import below - a fixture-level importorskip fires too late and the whole
# module fails collection.
pytest.importorskip("pyspark", reason="pyspark not installed; transform tests skipped")

from transforms import (  # noqa: E402
    VALID_STATUSES,
    deduplicate_orders,
    finalize_orders,
    normalize_orders,
    validate_orders,
)


def _rows(df, *cols):
    """Collect a frame into a set of tuples for order-independent assertions."""
    return {tuple(r[c] for c in cols) for r in df.select(*cols).collect()}


# --- normalization ---------------------------------------------------------


def test_normalize_lowercases_status(spark, bronze_rows):
    out = normalize_orders(bronze_rows)
    statuses = {r["status"] for r in out.select("status").collect()}
    assert "PENDING" not in statuses
    assert "pending" in statuses


def test_normalize_trims_and_uppercases_ids(spark, bronze_rows):
    out = normalize_orders(bronze_rows)
    ids = {r["order_id"] for r in out.select("order_id").collect()}
    # "  ord-007 " must become "ORD-007"
    assert "ORD-007" in ids
    assert not any(i != i.strip() for i in ids if i)


def test_normalize_parses_valid_date_and_nulls_bad_one(spark, bronze_rows):
    out = normalize_orders(bronze_rows)
    parsed = {
        r["order_id"]: r["order_date_parsed"]
        for r in out.select("order_id", "order_date_parsed").collect()
    }
    assert parsed["ORD-001"] is not None
    # "09/06/2026" does not match yyyy-MM-dd
    assert parsed["ORD-005"] is None


def test_normalize_keeps_raw_order_date(spark, bronze_rows):
    """The raw string must survive so validation can distinguish absent from
    unparseable."""
    out = normalize_orders(bronze_rows)
    raw = {r["order_id"]: r["order_date"] for r in out.select("order_id", "order_date").collect()}
    assert raw["ORD-005"] == "09/06/2026"


# --- validation ------------------------------------------------------------


def test_uppercase_status_is_valid_after_normalization(spark, bronze_rows):
    """The headline regression: old code quarantined ORD-002 as invalid_status."""
    valid, invalid = validate_orders(normalize_orders(bronze_rows))

    assert "ORD-002" in {r["order_id"] for r in valid.select("order_id").collect()}
    assert "ORD-002" not in {r["order_id"] for r in invalid.select("order_id").collect()}


def test_unparseable_date_is_quarantined_not_nulled(spark, bronze_rows):
    """Old code silently produced NULL in a partitioned column."""
    _, invalid = validate_orders(normalize_orders(bronze_rows))

    assert ("ORD-005", "unparseable_order_date") in _rows(
        invalid, "order_id", "error_message"
    )


@pytest.mark.parametrize(
    "order_id,expected",
    [
        ("ORD-003", "negative_amount"),
        ("ORD-004", "missing_user_id"),
        ("ORD-005", "unparseable_order_date"),
        ("ORD-006", "invalid_status"),
    ],
)
def test_each_failure_mode_gets_its_own_reason(spark, bronze_rows, order_id, expected):
    _, invalid = validate_orders(normalize_orders(bronze_rows))
    reasons = dict(_rows(invalid, "order_id", "error_message"))
    assert reasons[order_id] == expected


def test_valid_and_invalid_partition_the_input(spark, bronze_rows):
    """No row may be dropped or counted twice."""
    valid, invalid = validate_orders(normalize_orders(bronze_rows))
    assert valid.count() + invalid.count() == bronze_rows.count()


def test_valid_rows_are_exactly_the_expected_three(spark, bronze_rows):
    valid, _ = validate_orders(normalize_orders(bronze_rows))
    assert {r["order_id"] for r in valid.select("order_id").collect()} == {
        "ORD-001", "ORD-002", "ORD-007",
    }


def test_every_valid_row_has_an_allowed_status(spark, bronze_rows):
    valid, _ = validate_orders(normalize_orders(bronze_rows))
    for r in valid.select("status").collect():
        assert r["status"] in VALID_STATUSES


def test_zero_amount_is_valid(spark):
    """>= 0, not > 0 - a free order is legitimate."""
    from datetime import datetime
    df = spark.createDataFrame(
        [("ORD-Z", "USR-1", 0.0, "2026-09-06", "completed", datetime(2026, 9, 6), "l")],
        ["order_id", "user_id", "amount", "order_date", "status",
         "_bronze_ingestion_time", "_load_id"],
    )
    valid, invalid = validate_orders(normalize_orders(df))
    assert valid.count() == 1
    assert invalid.count() == 0


# --- deduplication ---------------------------------------------------------


def test_deduplicate_keeps_newest_row_per_key(spark):
    from datetime import datetime
    df = spark.createDataFrame(
        [
            ("ORD-1", "USR-1", 10.0, "2026-09-06", "pending", datetime(2026, 9, 6, 10), "l"),
            ("ORD-1", "USR-1", 20.0, "2026-09-06", "completed", datetime(2026, 9, 6, 12), "l"),
            ("ORD-2", "USR-2", 30.0, "2026-09-06", "completed", datetime(2026, 9, 6, 11), "l"),
        ],
        ["order_id", "user_id", "amount", "order_date", "status",
         "_bronze_ingestion_time", "_load_id"],
    )
    out = deduplicate_orders(normalize_orders(df))

    assert out.count() == 2
    amounts = dict(_rows(out, "order_id", "amount"))
    assert amounts["ORD-1"] == 20.0  # the 12:00 row, not the 10:00 one


def test_deduplicate_treats_different_users_as_different_keys(spark):
    from datetime import datetime
    df = spark.createDataFrame(
        [
            ("ORD-1", "USR-1", 10.0, "2026-09-06", "pending", datetime(2026, 9, 6, 10), "l"),
            ("ORD-1", "USR-2", 20.0, "2026-09-06", "pending", datetime(2026, 9, 6, 11), "l"),
        ],
        ["order_id", "user_id", "amount", "order_date", "status",
         "_bronze_ingestion_time", "_load_id"],
    )
    assert deduplicate_orders(normalize_orders(df)).count() == 2


# --- final shaping ---------------------------------------------------------


def test_finalize_carries_a_timestamp_for_the_hourly_rollup(spark, bronze_rows):
    """Gold's HOUR() needs a TIMESTAMP; order_date is a DATE and HOUR(DATE)=0."""
    valid, _ = validate_orders(normalize_orders(bronze_rows))
    out = finalize_orders(deduplicate_orders(valid))

    assert "order_timestamp" in out.columns
    ts = out.select("order_timestamp").first()["order_timestamp"]
    assert ts is not None
    assert ts.hour == 14  # from the fixture, not midnight


def test_finalize_emits_exactly_the_silver_columns(spark, bronze_rows):
    valid, _ = validate_orders(normalize_orders(bronze_rows))
    out = finalize_orders(deduplicate_orders(valid))

    assert out.columns == [
        "order_id", "user_id", "amount", "order_date", "order_timestamp",
        "status", "order_key", "_silver_processing_time", "_load_id",
    ]
    # Helper columns must not leak into the table.
    assert "order_date_parsed" not in out.columns
    assert "is_valid" not in out.columns


def test_order_key_is_stable_and_distinguishes_keys(spark, bronze_rows):
    valid, _ = validate_orders(normalize_orders(bronze_rows))
    out = finalize_orders(deduplicate_orders(valid))

    keys = _rows(out, "order_id", "order_key")
    assert len({k for _, k in keys}) == len(keys)  # no collisions
    assert all(k is not None for _, k in keys)
