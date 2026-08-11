"""Bounded Kafka micro-batches into immutable intraday Parquet."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable, Protocol

import duckdb


@dataclass(frozen=True)
class KafkaMessage:
    topic: str
    partition: int
    offset: int
    value: bytes


class MicroBatchConsumer(Protocol):
    def poll(self, timeout: float) -> KafkaMessage | None: ...
    def commit(self, messages: list[KafkaMessage]) -> None: ...


@dataclass(frozen=True)
class MicroBatchResult:
    run_id: str
    status: str
    polled_messages: int
    accepted_events: int
    duplicate_events: int
    quarantined_events: int
    files_written: int
    committed_messages: int
    started_at: str
    completed_at: str


FailureHook = Callable[[str, Path], None]


def _event(message: KafkaMessage, ingested_at: datetime) -> dict[str, Any]:
    payload = json.loads(message.value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("event must be a JSON object")
    missing = {"event_id", "symbol", "event_timestamp", "event_type"} - set(payload)
    if missing:
        raise ValueError(f"missing event fields: {sorted(missing)}")
    timestamp = datetime.fromisoformat(str(payload["event_timestamp"]).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("event_timestamp must include a UTC offset")
    anomaly = payload.get("anomaly_score")
    price = payload.get("price")
    return {
        "event_id": str(payload["event_id"]),
        "symbol": str(payload["symbol"]).strip().upper(),
        "event_timestamp": timestamp.astimezone(timezone.utc),
        "event_type": str(payload["event_type"]),
        "price": float(price) if price is not None else None,
        "anomaly_score": float(anomaly) if anomaly is not None else None,
        "details_json": json.dumps(payload.get("details", {}), sort_keys=True, separators=(",", ":")),
        "detected_at": (
            datetime.fromisoformat(str(payload["detected_at"]).replace("Z", "+00:00")).astimezone(timezone.utc)
            if payload.get("detected_at") else None
        ),
        "source_topic": message.topic,
        "kafka_partition": int(message.partition),
        "kafka_offset": int(message.offset),
        "ingested_at": ingested_at,
    }


def _existing(root: Path) -> tuple[set[str], set[tuple[str, int, int]]]:
    files = list(root.glob("date=*/hour=*/*.parquet"))
    if not files:
        return set(), set()
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT event_id, source_topic, kafka_partition, kafka_offset FROM read_parquet(?)",
            [str(root / "date=*/hour=*/*.parquet")],
        ).fetchall()
    return {row[0] for row in rows}, {(row[1], row[2], row[3]) for row in rows}


def consume_microbatch(
    consumer: MicroBatchConsumer, *, max_records: int = 500, max_wait_seconds: float = 1.0,
    raw_root: Path = Path("data/raw"), quarantine_root: Path = Path("data/quarantine/streamalpha"),
    metadata_root: Path = Path("warehouse/metadata/stream_batches"),
    run_id: str | None = None, now: datetime | None = None,
    failure_hook: FailureHook | None = None,
) -> MicroBatchResult:
    if max_records < 1 or max_wait_seconds < 0:
        raise ValueError("micro-batch bounds must be positive")
    run_id = run_id or str(uuid.uuid4())
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    started = now.astimezone(timezone.utc)
    deadline = monotonic() + max_wait_seconds
    messages = []
    while len(messages) < max_records:
        remaining = deadline - monotonic()
        if remaining <= 0 and messages:
            break
        message = consumer.poll(max(0.0, remaining))
        if message is None:
            break
        messages.append(message)

    accepted, rejected = [], []
    for message in messages:
        try:
            accepted.append((message, _event(message, started)))
        except (UnicodeError, json.JSONDecodeError, TypeError, ValueError, OverflowError) as exc:
            rejected.append({
                "topic": message.topic, "partition": message.partition, "offset": message.offset,
                "error": str(exc), "raw_value": message.value.decode("utf-8", errors="replace"),
                "received_at": started.isoformat(),
            })
    if rejected:
        quarantine_root.mkdir(parents=True, exist_ok=True)
        target = quarantine_root / f"run={run_id}.jsonl"
        with target.open("x", encoding="utf-8") as stream:
            for item in rejected:
                stream.write(json.dumps(item, separators=(",", ":")) + "\n")

    event_ids, coordinates = _existing(raw_root / "stream_events")
    new_events = []
    duplicates = 0
    for message, event in accepted:
        coordinate = (message.topic, message.partition, message.offset)
        if event["event_id"] in event_ids or coordinate in coordinates:
            duplicates += 1
        else:
            new_events.append(event)
            event_ids.add(event["event_id"])
            coordinates.add(coordinate)

    written = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in new_events:
        key = (event["event_timestamp"].date().isoformat(), f"{event['event_timestamp'].hour:02d}")
        groups.setdefault(key, []).append(event)
    for (day, hour), rows in groups.items():
        final = raw_root / "stream_events" / f"date={day}" / f"hour={hour}" / f"part-{run_id}.parquet"
        temporary = raw_root / ".tmp" / run_id / final.relative_to(raw_root)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        if failure_hook:
            failure_hook("before_write", temporary)
        with duckdb.connect() as connection:
            connection.execute("""CREATE TABLE payload (
                event_id VARCHAR, symbol VARCHAR, event_timestamp TIMESTAMPTZ,
                event_type VARCHAR, price DOUBLE, anomaly_score DOUBLE,
                details_json VARCHAR, detected_at TIMESTAMPTZ,
                source_topic VARCHAR, kafka_partition INTEGER, kafka_offset BIGINT,
                ingested_at TIMESTAMPTZ)""")
            connection.executemany(
                "INSERT INTO payload VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [[row[key] for key in row] for row in rows],
            )
            escaped = str(temporary).replace("'", "''")
            connection.execute(f"COPY payload TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, final)
        written.append(final)
    if failure_hook:
        failure_hook("after_write_before_commit", metadata_root / f"{run_id}.json")

    # Offsets are acknowledged only after every final file is durable. Rejected
    # messages are acknowledged because their immutable quarantine record is durable.
    consumer.commit(messages)
    completed = datetime.now(timezone.utc)
    result = MicroBatchResult(
        run_id, "degraded" if rejected else "success", len(messages), len(new_events),
        duplicates, len(rejected), len(written), len(messages),
        started.isoformat(), completed.isoformat(),
    )
    metadata_root.mkdir(parents=True, exist_ok=True)
    target = metadata_root / f"{run_id}.json"
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(result), indent=2) + "\n")
    os.replace(temporary, target)
    return result
