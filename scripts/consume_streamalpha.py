"""Consume one bounded StreamAlpha Kafka micro-batch into MarketForge."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict

from ingestion.streaming.kafka import KafkaMessage, consume_microbatch


class ConfluentConsumer:
    def __init__(self, brokers: str, topic: str, group_id: str):
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError("install the optional streaming dependency: pip install -e '.[streaming]'") from exc
        self.consumer = Consumer({
            "bootstrap.servers": brokers, "group.id": group_id,
            "enable.auto.commit": False, "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe([topic])

    def poll(self, timeout):
        message = self.consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(f"Kafka consumer error: {message.error()}")
        return KafkaMessage(message.topic(), message.partition(), message.offset(), message.value())

    def commit(self, _messages):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default=os.getenv("STREAMALPHA_KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("STREAMALPHA_KAFKA_TOPIC", "streamalpha.events"))
    parser.add_argument("--group-id", default="marketforge-local")
    parser.add_argument("--max-records", type=int, default=500)
    parser.add_argument("--max-wait-seconds", type=float, default=1.0)
    args = parser.parse_args()
    consumer = ConfluentConsumer(args.brokers, args.topic, args.group_id)
    try:
        result = consume_microbatch(
            consumer, max_records=args.max_records, max_wait_seconds=args.max_wait_seconds
        )
    finally:
        consumer.close()
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
