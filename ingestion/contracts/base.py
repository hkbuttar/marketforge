"""Small explicit contract engine used before records enter canonical storage."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


Normalizer = Callable[[Any], Any]
Rule = Callable[[Mapping[str, Any]], str | None]


class MissingRequiredFieldError(ValueError):
    """A required column disappeared from the entire provider batch."""


def text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("must be a string")
    value = value.strip()
    if not value:
        raise ValueError("must not be blank")
    return value


def upper_text(value: Any) -> str:
    return text(value).upper()


def finite_float(value: Any) -> float:
    if isinstance(value, bool):
        raise TypeError("must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("must be finite")
    return result


def integer(value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError("must be an integer")
    result = int(value)
    if float(value) != result:
        raise ValueError("must be a whole number")
    return result


def iso_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError("must be an ISO date")
    return date.fromisoformat(value.strip())


def utc_datetime(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise TypeError("must be an ISO timestamp")
    if value.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class Field:
    normalizer: Normalizer
    nullable: bool = False
    description: str = ""


@dataclass(frozen=True)
class QuarantineRecord:
    source: str
    ingestion_run_id: str
    error_type: str
    error_message: str
    raw_payload: Mapping[str, Any]
    received_at: datetime

    def as_json(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "ingestion_run_id": self.ingestion_run_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "raw_payload": self.raw_payload,
            "received_at": self.received_at.isoformat(),
        }


@dataclass(frozen=True)
class ValidationResult:
    accepted: tuple[dict[str, Any], ...] = ()
    rejected: tuple[QuarantineRecord, ...] = ()


@dataclass(frozen=True)
class Contract:
    name: str
    fields: Mapping[str, Field]
    unique_by: tuple[str, ...]
    idempotency_by: tuple[str, ...]
    version: int = 1
    unknown_field_policy: str = "quarantine"
    rules: tuple[Rule, ...] = ()
    aliases: Mapping[str, str] = field(default_factory=dict)
    source_metadata: Mapping[str, Any] = field(default_factory=dict)

    def validate(
        self,
        records: Iterable[Mapping[str, Any]],
        *,
        source: str,
        ingestion_run_id: str,
        received_at: datetime | None = None,
    ) -> ValidationResult:
        """Normalize and validate a batch; a row is either accepted or quarantined."""
        received_at = received_at or datetime.now(timezone.utc)
        if received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")
        records = tuple(records)
        if records:
            batch_columns = {
                self.aliases.get(column, column)
                for record in records
                for column in record
            }
            missing_from_batch = set(self.fields) - batch_columns
            if missing_from_batch:
                raise MissingRequiredFieldError(
                    f"{self.name} contract v{self.version}: required columns absent from entire "
                    f"batch: {sorted(missing_from_batch)}"
                )
        accepted: list[dict[str, Any]] = []
        rejected: list[QuarantineRecord] = []
        seen: set[tuple[Any, ...]] = set()

        for raw in records:
            normalized_input = {self.aliases.get(key, key): value for key, value in raw.items()}
            errors: list[str] = []
            duplicate_aliases = len(normalized_input) != len(raw)
            if duplicate_aliases:
                errors.append("alias normalization produced duplicate columns")
            missing = set(self.fields) - set(normalized_input)
            unexpected = set(normalized_input) - set(self.fields)
            if missing:
                errors.append(f"missing columns: {sorted(missing)}")
            if unexpected:
                errors.append(
                    f"unexpected columns under {self.unknown_field_policy} policy: "
                    f"{sorted(unexpected)}"
                )

            row: dict[str, Any] = {}
            for name, spec in self.fields.items():
                if name not in normalized_input:
                    continue
                value = normalized_input[name]
                if value is None:
                    if not spec.nullable:
                        errors.append(f"{name}: null is not allowed")
                    row[name] = None
                    continue
                try:
                    row[name] = spec.normalizer(value)
                except (TypeError, ValueError, OverflowError) as exc:
                    errors.append(f"{name}: {exc}")

            if not errors:
                errors.extend(message for rule in self.rules if (message := rule(row)))
            if not errors and row.get("source") != source:
                errors.append(f"source: expected {source!r}, got {row.get('source')!r}")
            if not errors:
                key = tuple(row[name] for name in self.unique_by)
                if key in seen:
                    errors.append(f"duplicate batch key {self.unique_by}: {key}")
                else:
                    seen.add(key)

            if errors:
                rejected.append(
                    QuarantineRecord(
                        source=source,
                        ingestion_run_id=ingestion_run_id,
                        error_type="contract_violation",
                        error_message="; ".join(errors),
                        raw_payload=dict(raw),
                        received_at=received_at.astimezone(timezone.utc),
                    )
                )
            else:
                accepted.append(row)
        return ValidationResult(tuple(accepted), tuple(rejected))


def write_quarantine(records: Iterable[QuarantineRecord], root: Path = Path("data/quarantine")) -> Path | None:
    """Write one immutable JSONL quarantine artifact for a source/run batch."""
    records = tuple(records)
    if not records:
        return None
    identities = {(record.source, record.ingestion_run_id) for record in records}
    if len(identities) != 1:
        raise ValueError("one quarantine file may contain only one source/run pair")
    source, run_id = next(iter(identities))
    safe = lambda value: "".join(char if char.isalnum() or char in "-_" else "_" for char in value)
    directory = root / f"source={safe(source)}"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"run={safe(run_id)}.jsonl"
    with target.open("x", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record.as_json(), default=str, separators=(",", ":")) + "\n")
    return target
