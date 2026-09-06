# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality checks
# MAGIC
# MAGIC Driven by the `quality` block in `config.yaml`. Fixes carried over:
# MAGIC
# MAGIC * Freshness compared `datetime.utcnow()` against a Spark timestamp. Spark returns a
# MAGIC   naive datetime in the **session** timezone, not UTC, so the age was wrong by the
# MAGIC   cluster's offset - 5.5 hours on an Asia/Kolkata cluster, enough to invert the result.
# MAGIC   Ages are now computed in Spark, which knows its own zone.
# MAGIC * Range checks compared `None < min_val` on an empty or all-NULL column, raising
# MAGIC   `TypeError`. Empty inputs now fail the check with a message instead.
# MAGIC * Uniqueness ran against every row of an SCD Type 2 table, which keeps historical
# MAGIC   versions on purpose, so it could never pass once any order changed. Rules can now set
# MAGIC   `current_only`.
# MAGIC * The checker was constructed without a webhook, so every alert was silently discarded.

# COMMAND ----------

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

import pipeline_config as cfg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checker

# COMMAND ----------


class QualityChecker:
    """Runs configured checks against a table and records the outcome."""

    def __init__(self, spark: SparkSession, slack_webhook: Optional[str] = None):
        self.spark = spark
        self.slack_webhook = slack_webhook
        self.results: List[Dict[str, Any]] = []

    # --- individual checks ---

    @staticmethod
    def check_not_null(df: DataFrame, column: str, threshold: float = 0.0) -> Tuple[bool, str]:
        stats = df.selectExpr(
            "COUNT(*) AS total",
            f"SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) AS nulls",
        ).collect()[0]

        total, nulls = stats["total"], stats["nulls"] or 0
        if total == 0:
            return False, f"'{column}': table is empty"

        pct = nulls / total
        return pct <= threshold, (
            f"'{column}': {pct:.2%} null ({nulls}/{total}, threshold {threshold:.2%})"
        )

    @staticmethod
    def check_unique(df: DataFrame, column: str) -> Tuple[bool, str]:
        stats = df.select(countDistinct(col(column)).alias("distinct")).collect()[0]
        total = df.count()
        distinct = stats["distinct"]

        if total == 0:
            return False, f"'{column}': table is empty"
        return total == distinct, f"'{column}': {distinct} distinct of {total} rows"

    @staticmethod
    def check_range(
        df: DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> Tuple[bool, str]:
        stats = df.selectExpr(
            f"MIN(`{column}`) AS lo", f"MAX(`{column}`) AS hi", "COUNT(*) AS total"
        ).collect()[0]

        # An empty table or an all-NULL column yields NULL bounds. Comparing
        # those against a number is what used to raise TypeError.
        if stats["total"] == 0 or stats["lo"] is None:
            return False, f"'{column}': no non-null values to range-check"

        issues = []
        if min_val is not None and stats["lo"] < min_val:
            issues.append(f"min {stats['lo']} < {min_val}")
        if max_val is not None and stats["hi"] > max_val:
            issues.append(f"max {stats['hi']} > {max_val}")

        return not issues, f"'{column}': {', '.join(issues) if issues else 'within range'}"

    def check_freshness(
        self, df: DataFrame, column: str, max_age_hours: float
    ) -> Tuple[bool, str]:
        """Age computed inside Spark so the session timezone is respected."""
        stats = df.selectExpr(
            f"MAX(`{column}`) AS newest",
            f"(unix_timestamp(current_timestamp()) - unix_timestamp(MAX(`{column}`))) / 3600.0 AS age_hours",
        ).collect()[0]

        if stats["newest"] is None:
            return False, f"'{column}': no timestamps present"

        age = stats["age_hours"]
        return age <= max_age_hours, f"'{column}': {age:.2f}h old (max {max_age_hours}h)"

    # --- orchestration ---

    def record(self, check_name: str, table_name: str, passed: bool, message: str) -> None:
        self.results.append({
            "check_name": check_name,
            "table_name": table_name,
            "passed": passed,
            "message": message,
            "checked_at": datetime.utcnow(),
        })
        logger.info(f"[{table_name}] {check_name}: {'PASS' if passed else 'FAIL'} - {message}")

        if not passed:
            self._alert(check_name, table_name, message)

    def _alert(self, check_name: str, table_name: str, message: str) -> None:
        if not self.slack_webhook:
            return
        try:
            requests.post(
                self.slack_webhook,
                json={
                    "text": (
                        f":warning: *Data quality failure*\n"
                        f"*{check_name}* on `{table_name}`\n{message}"
                    )
                },
                timeout=10,
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Slack alert failed: {e}")

    def run_rule(self, rule: Dict[str, Any]) -> None:
        """Run every check defined for one configured table."""
        layer = rule.get("layer", "silver")
        table_name = cfg.table(layer, rule["table"])
        df = self.spark.table(table_name)

        # SCD Type 2 tables keep superseded versions; most checks are only
        # meaningful against the current ones.
        if rule.get("current_only"):
            df = df.filter(col("dbt_is_current"))

        df.persist()
        try:
            for check in rule["checks"]:
                kind, column = check["check"], check["column"]

                if kind == "not_null":
                    passed, msg = self.check_not_null(df, column, check.get("threshold", 0.0))
                elif kind == "unique":
                    passed, msg = self.check_unique(df, column)
                elif kind == "range":
                    passed, msg = self.check_range(df, column, check.get("min"), check.get("max"))
                elif kind == "freshness":
                    passed, msg = self.check_freshness(df, column, check.get("max_age_hours", 24))
                else:
                    logger.warning(f"Unknown check type '{kind}' - skipping")
                    continue

                self.record(f"{kind}:{column}", table_name, passed, msg)
        finally:
            df.unpersist()

    def save(self) -> None:
        """Persist results to the monitoring schema."""
        if not self.results:
            logger.warning("No quality results to save")
            return

        target = cfg.table("monitoring", cfg.load_config()["monitoring"]["quality_results_table"])
        self.spark.createDataFrame(self.results).write.format("delta").mode(
            "append"
        ).option("mergeSchema", "true").saveAsTable(target)

        failures = sum(1 for r in self.results if not r["passed"])
        logger.info(f"Saved {len(self.results)} results ({failures} failed) to {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

config = cfg.load_config()

webhook = config.get("monitoring", {}).get("slack_webhook", "")
if webhook.startswith("${"):
    # Placeholder left unexpanded means no webhook was configured.
    webhook = None
    logger.info("No Slack webhook configured - alerts disabled")

checker = QualityChecker(spark, slack_webhook=webhook)

for rule in config["quality"]["rules"]:
    checker.run_rule(rule)

checker.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify, then fail the task if any rule failed

# COMMAND ----------

display(spark.createDataFrame(checker.results))

# COMMAND ----------

failed = [r for r in checker.results if not r["passed"]]
if failed:
    # Fail the task so the job history reflects the data problem.
    raise RuntimeError(f"{len(failed)} quality check(s) failed")
