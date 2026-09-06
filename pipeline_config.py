"""Shared configuration for the data-cascade pipeline.

Every module loads config.yaml through here, so the catalog name, storage
paths, and credentials are defined in exactly one place.

Two things this module exists to fix:

1. Table naming. Table names used to be hardcoded as ``main.bronze_layer.x``
   in every module. The Unity Catalog metastore has no ``main`` catalog, so
   all of them failed. Use ``table()`` instead of writing names by hand.

2. Secrets. ``yaml.safe_load`` returns ``${KAFKA_PASSWORD}`` as a literal
   string, which then got passed as an actual password. ``load_config()``
   expands those from the environment, and ``require()`` fails loudly if a
   placeholder was left unexpanded rather than letting it reach a broker.

On Databricks, job tasks inject secrets as cluster environment variables via
``{{secrets/scope/key}}`` - see jobs_config.json. Locally, export them or use
a .env file.
"""

import logging
import os
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_PATH = "config.yaml"
_cache: Optional[Dict[str, Any]] = None


def _expand(value: Any) -> Any:
    """Recursively expand ${VAR} placeholders from the environment."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Load and cache config.yaml with environment variables expanded.

    Path resolution order: explicit argument, then $CASCADE_CONFIG, then
    ./config.yaml. The Databricks jobs set CASCADE_CONFIG because the working
    directory of a job run is not the workspace folder holding the file.
    """
    global _cache
    if _cache is None:
        resolved = path or os.environ.get("CASCADE_CONFIG", _DEFAULT_PATH)
        with open(resolved) as f:
            _cache = _expand(yaml.safe_load(f))
        logger.info(f"Loaded config from {resolved}")
    return _cache


def reset_cache() -> None:
    """Drop the cached config. Only needed in tests."""
    global _cache
    _cache = None


def require(value: str, name: str) -> str:
    """Return value, or raise if it is empty or an unexpanded ${...} placeholder.

    Without this a missing secret silently becomes the literal string
    "${CONFLUENT_API_KEY}" and surfaces as a confusing auth error from the
    broker instead of a clear configuration error here.
    """
    if not value or (value.startswith("${") and value.endswith("}")):
        raise ValueError(
            f"Config value '{name}' is unset or unexpanded (got {value!r}). "
            f"Set the corresponding environment variable, or on Databricks "
            f"check the secret scope referenced in jobs_config.json."
        )
    return value


# --- Unity Catalog naming -------------------------------------------------


def catalog() -> str:
    """The Unity Catalog catalog holding every table in this pipeline."""
    return load_config()["databricks"]["catalog"]


def schema(layer: str) -> str:
    """Fully-qualified schema name, e.g. schema("bronze") -> cascade.bronze_layer."""
    return f"{catalog()}.{load_config()['databricks']['schemas'][layer]}"


def table(layer: str, name: str) -> str:
    """Fully-qualified table name, e.g. table("bronze", "bronze_orders_kafka")."""
    return f"{schema(layer)}.{name}"


# --- Storage paths --------------------------------------------------------


def checkpoint_path(name: str) -> str:
    """Streaming checkpoint location for a named stream.

    Auto Loader needs its schema location kept separate from its checkpoint;
    sharing one directory corrupts both, so they get sibling subdirectories.
    """
    return f"{load_config()['databricks']['checkpoint_path']}/{name}/checkpoint"


def schema_path(name: str) -> str:
    """Auto Loader schema-inference location for a named stream."""
    return f"{load_config()['databricks']['checkpoint_path']}/{name}/schema"


def landing_path(pattern: str = "") -> str:
    """Cloud storage prefix where raw files land."""
    root = load_config()["databricks"]["landing_path"].rstrip("/")
    return f"{root}/{pattern}" if pattern else root


# --- Databricks runtime helpers -------------------------------------------


def get_dbutils(spark):
    """Return dbutils, or None when running outside Databricks.

    dbutils is injected into notebooks but not into spark_python_task jobs,
    where it has to be constructed. file_ingestion.py used to reference a
    bare `dbutils` name that existed in neither context.
    """
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except (ImportError, AttributeError):
        logger.warning("dbutils unavailable - not running on Databricks")
        return None
