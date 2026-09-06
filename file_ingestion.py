# Databricks notebook source
# MAGIC %md
# MAGIC # File ingestion into Bronze (Auto Loader)
# MAGIC
# MAGIC The previous version could not run. It called `.mode()` and `.table()` on a
# MAGIC `DataStreamWriter` (neither exists - they are `.outputMode()` and `.toTable()`), built a
# MAGIC streaming query it never started, and returned `True` regardless, so every run reported
# MAGIC SUCCESS while ingesting nothing.
# MAGIC
# MAGIC It also tried to mount blob storage by reading `spark.read.format("abfss")`, which is
# MAGIC not a Spark format, then fell into an `except` branch referencing a bare `dbutils` name
# MAGIC that was never defined. All of that is gone: access goes through the Unity Catalog
# MAGIC external location `cascade_root` and its managed-identity credential, so there is
# MAGIC nothing to mount and no account key to hold.
# MAGIC
# MAGIC Auto Loader gets a **separate** schema location and checkpoint location. Pointing both
# MAGIC at one directory, as before, corrupts both.

# COMMAND ----------

import logging
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, to_date
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


class FileIngestor:
    """Streams files from ADLS Gen2 into Bronze tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = cfg.load_config()

    @staticmethod
    def _add_metadata(df: DataFrame) -> DataFrame:
        """Attach the Bronze metadata columns the tables declare."""
        return (
            df
            .withColumn("_file_path", input_file_name())
            .withColumn("_bronze_ingestion_time", current_timestamp())
            .withColumn("_bronze_ingestion_date", to_date(current_timestamp()))
            .withColumn("_source_system", lit("cloud_files"))
            .withColumn("_processing_timestamp", current_timestamp())
            .withColumn("_load_id", lit(None).cast(StringType()))
        )

    def ingest(
        self,
        pattern: str,
        table_name: str,
        file_format: str,
        stream_name: str,
        use_file_events: bool = True,
    ) -> bool:
        """Ingest one configured path into one Bronze table.

        Runs with trigger(availableNow) so the call returns once the files
        present at start have been consumed - a scheduled job needs to finish.

        The cascade_root external location has file events enabled, so Auto
        Loader can use notification mode (Azure Queue) instead of listing the
        directory on every run.
        """
        source_path = cfg.landing_path(pattern)

        reader = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", cfg.schema_path(stream_name))
            # Widen rather than fail when a new column appears, and keep
            # anything unparseable instead of dropping it silently.
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        )

        if use_file_events:
            reader = reader.option("cloudFiles.useNotifications", "true")

        if file_format == "csv":
            reader = reader.option("header", "true")
            # cloudFiles has its own type inference; plain inferSchema is
            # ignored by the cloudFiles source.
            reader = reader.option("cloudFiles.inferColumnTypes", "true")

        try:
            df = self._add_metadata(reader.load(source_path))

            query = (
                df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", cfg.checkpoint_path(stream_name))
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
                .toTable(table_name)
            )

            query.awaitTermination()
            logger.info(f"Ingested {source_path} -> {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed ingesting {source_path}: {e}")
            return False

    def ingest_all(self) -> Dict[str, bool]:
        results = {}
        for entry in self.config["files"].get("paths", []):
            pattern = entry["pattern"]
            results[pattern] = self.ingest(
                pattern=pattern,
                table_name=cfg.table("bronze", entry["table"]),
                file_format=entry["format"],
                stream_name=f"files_{entry['table']}",
            )
        return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

results = FileIngestor(spark).ingest_all()
for pattern, ok in results.items():
    logger.info(f"{pattern}: {'SUCCESS' if ok else 'FAILED'}")

if not all(results.values()):
    raise RuntimeError("Some file ingestions failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(spark.table(cfg.table("bronze", "bronze_orders_files")))
