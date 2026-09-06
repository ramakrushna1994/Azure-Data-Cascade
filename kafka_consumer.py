# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka -> Bronze ingestion
# MAGIC
# MAGIC Reads from Confluent Cloud via Structured Streaming and lands rows in Bronze.
# MAGIC
# MAGIC Four fixes worth calling out:
# MAGIC
# MAGIC * **The JAAS login module must use Databricks' shaded class name.** DBR relocates the
# MAGIC   Kafka client into the `kafkashaded` package, and JAAS names the class as a string,
# MAGIC   so the unshaded name is not on the classpath. The symptom is a thoroughly
# MAGIC   misleading `Failed to create new KafkaAdminClient`; the real cause is three levels
# MAGIC   down - `No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule`.
# MAGIC * The writer called `.mode()` and `.table()`. Neither exists on `DataStreamWriter` -
# MAGIC   they are `.outputMode()` and `.toTable()` - so this raised `AttributeError` before
# MAGIC   writing anything.
# MAGIC * It now uses `trigger(availableNow=True)`: the task drains whatever is in the topic
# MAGIC   and exits, which is what a scheduled job needs. The previous `awaitTermination()` on
# MAGIC   a continuous stream never returned, so a job on a one-minute cron piled up
# MAGIC   overlapping runs fighting over a single checkpoint.
# MAGIC * Offsets come from the checkpoint. `startingOffsets` applies only on the first run
# MAGIC   against an empty checkpoint.

# COMMAND ----------

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_date
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

import pipeline_config as cfg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.getOrCreate()

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])

# Databricks Runtime ships a *shaded* Kafka client, relocated into the
# `kafkashaded` package. The JAAS config names a class by string, so it has to
# use the shaded name or the JVM cannot find it. Getting this wrong surfaces
# as a misleading "Failed to create new KafkaAdminClient", whose real cause is
# three levels down: "No LoginModule found for
# org.apache.kafka.common.security.plain.PlainLoginModule".
#
# Vanilla Spark (local runs, non-Databricks clusters) uses the unshaded name.
PLAIN_LOGIN_MODULE = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    if os.environ.get("DATABRICKS_RUNTIME_VERSION")
    else "org.apache.kafka.common.security.plain.PlainLoginModule"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestor

# COMMAND ----------


class KafkaBronzeIngestor:
    """Streams one Kafka topic into one Bronze table.

    Named to avoid shadowing kafka.KafkaConsumer, which kafka_producer.py
    imports from the same dependency.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = cfg.load_config()
        self.kafka_config = self.config["kafka"]

    def _kafka_options(self, topic: str) -> dict:
        bootstrap = cfg.require(
            self.kafka_config["bootstrap_servers"], "kafka.bootstrap_servers"
        )
        username = cfg.require(
            self.kafka_config["sasl_plain_username"], "kafka.sasl_plain_username"
        )
        password = cfg.require(
            self.kafka_config["sasl_plain_password"], "kafka.sasl_plain_password"
        )

        options = {
            "kafka.bootstrap.servers": bootstrap,
            "subscribe": topic,
            "startingOffsets": self.kafka_config.get("starting_offsets", "earliest"),
            "failOnDataLoss": "false",
        }

        if self.kafka_config.get("security_protocol") == "SASL_SSL":
            options.update({
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": self.kafka_config["sasl_mechanism"],
                "kafka.sasl.jaas.config": (
                    f"{PLAIN_LOGIN_MODULE} "
                    f'required username="{username}" password="{password}";'
                ),
            })

        return options

    def read_stream(self, topic: str) -> DataFrame:
        """Open the Kafka stream for a topic."""
        return (
            self.spark.readStream
            .format("kafka")
            .options(**self._kafka_options(topic))
            .load()
        )

    @staticmethod
    def parse_and_enrich(df: DataFrame) -> DataFrame:
        """Parse the JSON payload and attach Bronze metadata.

        Kafka partition and offset are retained so a row can be traced back to
        its position in the topic.
        """
        parsed = df.select(
            from_json(col("value").cast("string"), ORDER_SCHEMA).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
        ).select("data.*", "_kafka_partition", "_kafka_offset")

        return (
            parsed
            .withColumn("_bronze_ingestion_time", current_timestamp())
            .withColumn("_bronze_ingestion_date", to_date(current_timestamp()))
            .withColumn("_source_system", lit("kafka"))
            .withColumn("_processing_timestamp", current_timestamp())
            .withColumn("_load_id", lit(None).cast(StringType()))
        )

    def run(
        self,
        topic: str,
        table_name: str,
        stream_name: str,
        continuous: bool = False,
        processing_time: Optional[str] = None,
    ) -> None:
        """Ingest a topic into a Bronze table.

        continuous=False (the default) drains available records and returns,
        which suits a scheduled job. Set continuous=True for an always-on
        stream, in which case the job must not carry a cron schedule.
        """
        logger.info(f"Starting Kafka ingestion: {topic} -> {table_name}")

        df = self.parse_and_enrich(self.read_stream(topic))

        writer = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", cfg.checkpoint_path(stream_name))
            .option("mergeSchema", "true")
        )

        if continuous:
            writer = writer.trigger(processingTime=processing_time or "30 seconds")
        else:
            writer = writer.trigger(availableNow=True)

        query = writer.toTable(table_name)
        query.awaitTermination()

        logger.info(f"Kafka ingestion finished for {topic}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

config = cfg.load_config()
KafkaBronzeIngestor(spark).run(
    topic=config["kafka"]["topics"]["orders"],
    table_name=cfg.table("bronze", "bronze_orders_kafka"),
    stream_name="orders_kafka",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    spark.table(cfg.table("bronze", "bronze_orders_kafka"))
    .orderBy("_kafka_offset")
)
