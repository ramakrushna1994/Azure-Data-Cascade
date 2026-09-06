# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline monitoring - freshness SLAs, processing lag, health report
# MAGIC
# MAGIC Fixes carried over:
# MAGIC
# MAGIC * Freshness subtracted a Spark timestamp from `datetime.utcnow()`. Spark returns a naive
# MAGIC   datetime in the session timezone, so the age was off by the cluster's UTC offset.
# MAGIC   Ages are computed in Spark now.
# MAGIC * `PipelineMonitor` was constructed with no webhook, so `send_alert()` returned
# MAGIC   immediately every time and no alert was ever delivered.
# MAGIC * Ingestion lag compared raw `COUNT(*)` of Bronze against Silver. Silver both
# MAGIC   deduplicates and retains SCD versions, so the counts are not comparable and the
# MAGIC   result could go negative. Lag is now the **time gap** between the newest Bronze row
# MAGIC   and Silver's processing watermark.
# MAGIC * `log_metrics_table()` created an empty table and left a TODO. It now writes the
# MAGIC   metrics actually gathered, and creates the monitoring schema first.

# COMMAND ----------

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import SparkSession

import pipeline_config as cfg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor

# COMMAND ----------


class PipelineMonitor:
    """Checks freshness and lag across the medallion layers."""

    def __init__(self, spark: SparkSession, slack_webhook: Optional[str] = None):
        self.spark = spark
        self.slack_webhook = slack_webhook
        self.alerts: List[str] = []
        self.metrics: List[Dict[str, Any]] = []

    def _record(self, name: str, value: Optional[float], table: str, status: str) -> None:
        self.metrics.append({
            "metric_name": name,
            "metric_value": float(value) if value is not None else None,
            "table_name": table,
            "status": status,
            "measured_at": datetime.utcnow(),
        })

    def check_freshness(self, sla: Dict[str, Any]) -> Dict[str, Any]:
        """Compare a table's newest timestamp against its allowed age."""
        table_name = cfg.table(sla["layer"], sla["table"])
        column = sla["timestamp_column"]
        max_age = sla["max_age_minutes"]

        try:
            row = self.spark.sql(f"""
                SELECT
                    COUNT(*) AS row_count,
                    MAX(`{column}`) AS newest,
                    (unix_timestamp(current_timestamp())
                        - unix_timestamp(MAX(`{column}`))) / 60.0 AS age_minutes
                FROM {table_name}
            """).collect()[0]
        except Exception as e:
            logger.error(f"Freshness check failed for {table_name}: {e}")
            self._record("freshness_minutes", None, table_name, "ERROR")
            return {"table": table_name, "status": "ERROR", "error": str(e)}

        if row["row_count"] == 0 or row["newest"] is None:
            self.alerts.append(f"{table_name} is empty")
            self._record("freshness_minutes", None, table_name, "EMPTY")
            return {"table": table_name, "status": "EMPTY"}

        age = row["age_minutes"]
        stale = age > max_age
        status = "STALE" if stale else "FRESH"
        self._record("freshness_minutes", age, table_name, status)

        if stale:
            message = f"{table_name} is stale: {age:.0f}m old (SLA {max_age}m)"
            self.alerts.append(message)
            self.send_alert(message, severity="CRITICAL")

        return {
            "table": table_name,
            "status": status,
            "age_minutes": age,
            "row_count": row["row_count"],
        }

    def check_processing_lag(self, bronze_table: str, source_table: str) -> Dict[str, Any]:
        """Time gap between the newest Bronze row and Silver's watermark."""
        watermark_table = cfg.table("monitoring", "silver_watermarks")
        try:
            row = self.spark.sql(f"""
                SELECT
                    (SELECT MAX(_bronze_ingestion_time) FROM {bronze_table}) AS bronze_newest,
                    (SELECT last_processed_time FROM {watermark_table}
                      WHERE source_table = '{source_table}')                 AS silver_watermark
            """).collect()[0]
        except Exception as e:
            logger.error(f"Lag check failed: {e}")
            return {"status": "ERROR", "error": str(e)}

        if row["bronze_newest"] is None:
            return {"status": "NO_DATA"}

        if row["silver_watermark"] is None:
            message = f"{bronze_table} has data but Silver has never processed it"
            self.alerts.append(message)
            self.send_alert(message, severity="CRITICAL")
            self._record("processing_lag_minutes", None, bronze_table, "UNPROCESSED")
            return {"status": "UNPROCESSED"}

        lag_minutes = (row["bronze_newest"] - row["silver_watermark"]).total_seconds() / 60
        status = "WARN" if lag_minutes > 30 else "OK"
        self._record("processing_lag_minutes", lag_minutes, bronze_table, status)

        if status == "WARN":
            message = f"Silver is {lag_minutes:.0f}m behind {bronze_table}"
            self.alerts.append(message)
            self.send_alert(message, severity="WARNING")

        return {"status": status, "lag_minutes": lag_minutes}

    def send_alert(self, message: str, severity: str = "WARNING") -> None:
        if not self.slack_webhook or not message:
            return

        colors = {"INFO": "#36a64f", "WARNING": "#ff9900", "CRITICAL": "#ff0000"}
        try:
            response = requests.post(
                self.slack_webhook,
                json={
                    "attachments": [{
                        "color": colors.get(severity, "#808080"),
                        "title": f"Pipeline alert ({severity})",
                        "text": message,
                        "footer": "cascade pipeline monitor",
                        "ts": int(datetime.utcnow().timestamp()),
                    }]
                },
                timeout=10,
            )
            if response.status_code != 200:
                logger.error(f"Slack returned {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send alert: {e}")

    def save_metrics(self) -> None:
        """Write gathered metrics to the monitoring schema."""
        if not self.metrics:
            return

        target = cfg.table("monitoring", cfg.load_config()["monitoring"]["metrics_table"])
        self.spark.createDataFrame(self.metrics).write.format("delta").mode(
            "append"
        ).option("mergeSchema", "true").saveAsTable(target)
        logger.info(f"Wrote {len(self.metrics)} metrics to {target}")

    def health_report(self, results: List[Dict[str, Any]]) -> str:
        lines = [
            f"Pipeline health - {datetime.utcnow().isoformat()}Z",
            "=" * 55,
            "",
        ]
        for r in results:
            age = f"{r['age_minutes']:.0f}m" if r.get("age_minutes") is not None else "-"
            rows = r.get("row_count", "-")
            lines.append(f"  {r['status']:<8} {r['table']:<45} age={age:<8} rows={rows}")

        lines.append("")
        lines.append(f"Alerts: {len(self.alerts)}")
        lines.extend(f"  - {a}" for a in self.alerts)
        return "\n".join(lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

config = cfg.load_config()

# monitoring runs on its own schedule and may execute before any other task
# has created the schema.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('monitoring')}")

webhook = config.get("monitoring", {}).get("slack_webhook", "")
if webhook.startswith("${"):
    webhook = None
    logger.info("No Slack webhook configured - alerts disabled")

monitor = PipelineMonitor(spark, slack_webhook=webhook)

results = [monitor.check_freshness(sla) for sla in config["monitoring"]["slas"]]

monitor.check_processing_lag(
    bronze_table=cfg.table("bronze", "bronze_orders_kafka"),
    source_table=cfg.table("bronze", "bronze_orders_kafka"),
)

monitor.save_metrics()
print(monitor.health_report(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metrics history

# COMMAND ----------

display(
    spark.table(cfg.table("monitoring", config["monitoring"]["metrics_table"]))
    .orderBy("measured_at", ascending=False)
)
