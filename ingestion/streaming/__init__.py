"""Optional streaming bridges; core MarketForge does not require Kafka."""

from .kafka import KafkaMessage, MicroBatchResult, consume_microbatch

__all__ = ["KafkaMessage", "MicroBatchResult", "consume_microbatch"]
