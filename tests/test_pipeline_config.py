"""Tests for pipeline_config. No Spark required - these run in milliseconds.

Each test maps to a bug that actually shipped:

* Table names were hardcoded as main.bronze_layer.*, but the metastore has no
  `main` catalog, so every table operation failed.
* yaml.safe_load returns "${VAR}" as a literal string, which was then passed
  to a broker as an actual password.
* Auto Loader's schema location and checkpoint location shared one directory,
  which corrupts both.
"""

import os

import pytest
import yaml

import pipeline_config as cfg


CONFIG = {
    "databricks": {
        "catalog": "cascade",
        "schemas": {
            "bronze": "bronze_layer",
            "silver": "silver_layer",
            "gold": "gold_layer",
            "monitoring": "monitoring",
            "analytics": "analytics",
        },
        "checkpoint_path": "abfss://c@acct.dfs.core.windows.net/adb/cascade/checkpoints",
        "landing_path": "abfss://c@acct.dfs.core.windows.net/adb/cascade/raw",
    },
    "kafka": {
        "bootstrap_servers": "${CONFLUENT_BOOTSTRAP}",
        "sasl_plain_password": "${CONFLUENT_API_SECRET}",
    },
}


@pytest.fixture
def config_file(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(CONFIG))
    return str(p)


@pytest.fixture(autouse=True)
def _clean_cache():
    """load_config caches; every test must start from a clean slate."""
    cfg.reset_cache()
    yield
    cfg.reset_cache()


# --- placeholder expansion -------------------------------------------------


def test_expand_substitutes_from_environment(monkeypatch, config_file):
    monkeypatch.setenv("CONFLUENT_BOOTSTRAP", "pkc-test.confluent.cloud:9092")
    monkeypatch.setenv("CONFLUENT_API_SECRET", "s3cret")

    loaded = cfg.load_config(config_file)

    assert loaded["kafka"]["bootstrap_servers"] == "pkc-test.confluent.cloud:9092"
    assert loaded["kafka"]["sasl_plain_password"] == "s3cret"


def test_expand_leaves_unset_placeholder_intact(monkeypatch, config_file):
    monkeypatch.delenv("CONFLUENT_BOOTSTRAP", raising=False)

    loaded = cfg.load_config(config_file)

    # Left as-is so require() can produce a clear error, rather than silently
    # becoming an empty string that reaches the broker.
    assert loaded["kafka"]["bootstrap_servers"] == "${CONFLUENT_BOOTSTRAP}"


def test_expand_recurses_through_nested_structures(monkeypatch):
    monkeypatch.setenv("TOKEN", "abc123")

    result = cfg._expand({
        "a": "${TOKEN}",
        "b": {"c": "${TOKEN}"},
        "d": ["${TOKEN}", {"e": "${TOKEN}"}],
        "f": 42,
        "g": None,
    })

    assert result["a"] == "abc123"
    assert result["b"]["c"] == "abc123"
    assert result["d"][0] == "abc123"
    assert result["d"][1]["e"] == "abc123"
    # Non-string values must pass through untouched.
    assert result["f"] == 42
    assert result["g"] is None


# --- require ---------------------------------------------------------------


def test_require_returns_real_values():
    assert cfg.require("pkc-x.confluent.cloud:9092", "kafka.bootstrap") == \
        "pkc-x.confluent.cloud:9092"


@pytest.mark.parametrize("bad", ["${CONFLUENT_API_KEY}", "", None])
def test_require_rejects_unset_and_unexpanded(bad):
    """This is the guard that stops '${VAR}' reaching a broker as a password."""
    with pytest.raises(ValueError) as exc:
        cfg.require(bad, "kafka.sasl_plain_password")
    assert "kafka.sasl_plain_password" in str(exc.value)


def test_require_allows_value_merely_containing_braces():
    # Only a whole-string ${...} is a placeholder; a brace inside a real
    # password must not trip the guard.
    assert cfg.require("pa${s}word", "x") == "pa${s}word"


# --- Unity Catalog naming --------------------------------------------------


def test_table_builds_three_part_name(config_file, monkeypatch):
    monkeypatch.setenv("CASCADE_CONFIG", config_file)
    cfg.load_config()

    assert cfg.table("bronze", "bronze_orders_kafka") == \
        "cascade.bronze_layer.bronze_orders_kafka"
    assert cfg.table("silver", "silver_orders") == "cascade.silver_layer.silver_orders"
    assert cfg.table("analytics", "v_top_customers") == \
        "cascade.analytics.v_top_customers"


def test_catalog_and_schema_come_from_config(config_file, monkeypatch):
    monkeypatch.setenv("CASCADE_CONFIG", config_file)
    cfg.load_config()

    assert cfg.catalog() == "cascade"
    assert cfg.schema("gold") == "cascade.gold_layer"
    # No table name anywhere hardcodes a catalog, so switching is one edit.
    assert not cfg.table("bronze", "x").startswith("main.")


def test_unknown_layer_raises(config_file, monkeypatch):
    monkeypatch.setenv("CASCADE_CONFIG", config_file)
    cfg.load_config()

    with pytest.raises(KeyError):
        cfg.table("platinum", "nope")


# --- storage paths ---------------------------------------------------------


def test_checkpoint_and_schema_paths_do_not_collide(config_file, monkeypatch):
    """Auto Loader corrupts both if its schema and checkpoint share a dir."""
    monkeypatch.setenv("CASCADE_CONFIG", config_file)
    cfg.load_config()

    ckpt = cfg.checkpoint_path("orders_kafka")
    schema = cfg.schema_path("orders_kafka")

    assert ckpt != schema
    assert not ckpt.startswith(schema)
    assert not schema.startswith(ckpt)
    assert ckpt.endswith("/orders_kafka/checkpoint")
    assert schema.endswith("/orders_kafka/schema")


def test_landing_path_joins_without_double_slash(config_file, monkeypatch):
    monkeypatch.setenv("CASCADE_CONFIG", config_file)
    cfg.load_config()

    assert cfg.landing_path("orders").endswith("/adb/cascade/raw/orders")
    assert "//adb" not in cfg.landing_path("orders")
    assert cfg.landing_path() == "abfss://c@acct.dfs.core.windows.net/adb/cascade/raw"


# --- caching ---------------------------------------------------------------


def test_config_is_cached_after_first_load(config_file):
    first = cfg.load_config(config_file)
    # A path that does not exist would raise if it were re-read.
    second = cfg.load_config("/nonexistent/config.yaml")
    assert first is second
