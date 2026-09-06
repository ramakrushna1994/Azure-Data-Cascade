"""Test producer - sends sample orders to the Confluent Cloud topic.

Not part of the scheduled pipeline; this is a local utility for putting data
into the topic so the rest of the pipeline has something to process.

Credentials come from the environment, matching config.yaml:

    export CONFLUENT_BOOTSTRAP="pkc-....confluent.cloud:9092"
    export CONFLUENT_API_KEY="..."
    export CONFLUENT_API_SECRET="..."
    python kafka_producer.py
"""

import argparse
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from kafka import KafkaProducer

import pipeline_config as cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

STATUSES = ["pending", "completed", "failed", "cancelled"]


class OrderProducer:
    def __init__(self):
        kafka_config = cfg.load_config()["kafka"]

        self.producer = KafkaProducer(
            bootstrap_servers=cfg.require(
                kafka_config["bootstrap_servers"], "kafka.bootstrap_servers"
            ),
            security_protocol=kafka_config.get("security_protocol", "SASL_SSL"),
            sasl_mechanism=kafka_config.get("sasl_mechanism", "PLAIN"),
            sasl_plain_username=cfg.require(
                kafka_config["sasl_plain_username"], "kafka.sasl_plain_username"
            ),
            sasl_plain_password=cfg.require(
                kafka_config["sasl_plain_password"], "kafka.sasl_plain_password"
            ),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Confluent Cloud rejects unencrypted connections; fail fast rather
            # than hanging on a retry loop if the protocol is misconfigured.
            request_timeout_ms=20000,
        )
        logger.info("Producer connected")

    def send(self, topic: str, events: List[Dict[str, Any]]) -> None:
        for event in events:
            event.setdefault(
                "_ingested_at", datetime.now(timezone.utc).isoformat()
            )
            self.producer.send(topic, value=event)
        self.producer.flush()
        logger.info(f"Sent {len(events)} events to {topic}")

    def close(self) -> None:
        self.producer.close()


def generate_orders(count: int, include_invalid: bool = True) -> List[Dict[str, Any]]:
    """Build sample orders, optionally seeding rows that must be quarantined."""
    orders = []
    for i in range(count):
        order_date = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 6))
        orders.append({
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "user_id": f"USR-{random.randint(100, 199)}",
            "amount": round(random.uniform(10, 500), 2),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": random.choice(STATUSES),
        })

    if include_invalid:
        # Each of these exercises a distinct quarantine path in silver_layer.py.
        orders.extend([
            {"order_id": "ORD-BAD-1", "user_id": "USR-500", "amount": -25.0,
             "order_date": "2026-09-06", "status": "completed"},
            {"order_id": "ORD-BAD-2", "user_id": None, "amount": 40.0,
             "order_date": "2026-09-06", "status": "completed"},
            {"order_id": "ORD-BAD-3", "user_id": "USR-501", "amount": 60.0,
             "order_date": "06/09/2026", "status": "completed"},
            {"order_id": "ORD-BAD-4", "user_id": "USR-502", "amount": 80.0,
             "order_date": "2026-09-06", "status": "refunded"},
        ])

    return orders


def main() -> None:
    parser = argparse.ArgumentParser(description="Send sample orders to Kafka")
    parser.add_argument("--count", type=int, default=10, help="valid orders to send")
    parser.add_argument("--clean", action="store_true", help="omit invalid records")
    args = parser.parse_args()

    topic = cfg.load_config()["kafka"]["topics"]["orders"]
    producer = OrderProducer()
    try:
        producer.send(topic, generate_orders(args.count, include_invalid=not args.clean))
    finally:
        producer.close()


if __name__ == "__main__":
    main()
