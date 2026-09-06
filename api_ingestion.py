# Databricks notebook source
# MAGIC %md
# MAGIC # REST API polling into Bronze
# MAGIC
# MAGIC Fixes carried over:
# MAGIC
# MAGIC * The Bronze target was built as `bronze_{endpoint_name}_api`, producing
# MAGIC   `bronze_external_orders_api` - a table nothing downstream read - while
# MAGIC   `bronze_layer` declared `bronze_orders_api`. The endpoint config now names its target.
# MAGIC * A successful poll returning zero rows was treated the same as a failed request, so an
# MAGIC   empty endpoint failed the whole job.
# MAGIC * Rows were written without the Bronze metadata columns the table declares, including
# MAGIC   the `_bronze_ingestion_date` partition key.
# MAGIC * Authorization headers held an unexpanded `${API_KEY}` literal, sent to the API verbatim.
# MAGIC
# MAGIC **Known limitation:** each run appends everything the endpoint returns. Without a
# MAGIC server-side incremental filter this accumulates duplicates in Bronze. Silver
# MAGIC deduplicates on the natural key so the pipeline stays correct, but Bronze grows faster
# MAGIC than it needs to.

# COMMAND ----------

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
from pyspark.sql.types import StringType

import pipeline_config as cfg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestor

# COMMAND ----------


class APIIngestor:
    """Polls configured REST endpoints and lands responses in Bronze."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = cfg.load_config()
        self.session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetch(self, endpoint: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Fetch rows from an endpoint.

        Returns a list on success - possibly empty - and None on failure, so
        callers can tell "no data" apart from "request failed".
        """
        url = endpoint["url"]
        method = endpoint.get("method", "GET").upper()
        headers = dict(endpoint.get("headers", {}))

        auth = headers.get("Authorization", "")
        if "${" in auth:
            logger.error(
                f"Authorization header for {endpoint.get('name')} contains an "
                f"unexpanded placeholder - set the API_KEY environment variable"
            )
            return None

        try:
            if method == "GET":
                response = self.session.get(url, headers=headers, timeout=30)
            elif method == "POST":
                response = self.session.post(url, headers=headers, timeout=30)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None

            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                logger.error(f"Expected JSON from {url}, got {content_type!r}")
                return None

            payload = response.json()
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict) and "data" in payload:
                return payload["data"]
            return [payload]

        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {url} failed: {e}")
            return None
        except ValueError as e:
            logger.error(f"Response from {url} was not valid JSON: {e}")
            return None

    def write_to_bronze(
        self, rows: List[Dict[str, Any]], endpoint_name: str, table_name: str
    ) -> bool:
        """Write rows to a Bronze table with the standard metadata columns."""
        try:
            fetched_at = datetime.now(timezone.utc).isoformat()
            df = (
                self.spark.createDataFrame(rows)
                .withColumn("_api_source", lit(endpoint_name))
                .withColumn("_api_ingestion_timestamp", lit(fetched_at))
                .withColumn("_bronze_ingestion_time", current_timestamp())
                .withColumn("_bronze_ingestion_date", to_date(current_timestamp()))
                .withColumn("_source_system", lit("api"))
                .withColumn("_processing_timestamp", current_timestamp())
                .withColumn("_load_id", lit(f"api_{fetched_at}").cast(StringType()))
            )

            df.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(table_name)

            logger.info(f"Wrote {len(rows)} rows to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed writing to {table_name}: {e}")
            return False

    def ingest_endpoint(self, endpoint: Dict[str, Any]) -> bool:
        name = endpoint.get("name", endpoint["url"])
        table_name = cfg.table("bronze", endpoint.get("table", f"bronze_{name}_api"))

        logger.info(f"Polling {name}")
        rows = self.fetch(endpoint)

        if rows is None:
            return False
        if not rows:
            # A successful poll with nothing new is not a failure.
            logger.info(f"{name} returned no rows")
            return True

        return self.write_to_bronze(rows, name, table_name)

    def ingest_all(self) -> Dict[str, bool]:
        return {
            endpoint.get("name", endpoint["url"]): self.ingest_endpoint(endpoint)
            for endpoint in self.config.get("api", {}).get("endpoints", [])
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

results = APIIngestor(spark).ingest_all()
for name, ok in results.items():
    logger.info(f"{name}: {'SUCCESS' if ok else 'FAILED'}")

if not all(results.values()):
    raise RuntimeError("Some API ingestions failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(spark.table(cfg.table("bronze", "bronze_orders_api")))
