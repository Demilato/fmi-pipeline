"""Kafka consumer: collects observations into batches and loads each batch into BigQuery.

A batch is normally one producer round (all stations), loaded as one DataFrame with a
single load job. Offsets are committed only after BigQuery has accepted the batch, so a
crash never loses data; it may re-load a batch (at-least-once), and those duplicates are
removed by the daily processing.

Run: python -m fmi_pipeline.consumer
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from fmi_pipeline.config import Settings

log = logging.getLogger("fmi.consumer")


@dataclass
class Batch:
    rows: list[dict[str, Any]] = field(default_factory=list)
    first_at: float = 0.0
    last_at: float = 0.0

    def add(self, row: dict[str, Any], now: float) -> None:
        if not self.rows:
            self.first_at = now
        self.rows.append(row)
        self.last_at = now

    def should_flush(self, now: float, idle_seconds: float, max_wait_seconds: float, max_rows: int) -> bool:
        if not self.rows:
            return False
        return (
            now - self.last_at >= idle_seconds  # the producer round has finished
            or now - self.first_at >= max_wait_seconds
            or len(self.rows) >= max_rows
        )

    def clear(self) -> None:
        self.rows.clear()
        self.first_at = self.last_at = 0.0


def parse_message(value: bytes, partition: int, offset: int) -> dict[str, Any] | None:
    try:
        row = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(row, dict) or "fmisid" not in row or "obs_time" not in row:
        return None
    row["kafka_partition"] = partition
    row["kafka_offset"] = offset
    return row


def to_dataframe(rows: list[dict[str, Any]], ingested_at: datetime) -> pd.DataFrame:
    from fmi_pipeline.bq import RAW_COLUMNS, TIMESTAMP_COLUMNS

    df = pd.DataFrame(rows)
    df["ingested_at"] = ingested_at
    for column in RAW_COLUMNS:
        if column not in df:
            df[column] = None
    df = df[RAW_COLUMNS]
    for column in TIMESTAMP_COLUMNS:
        df[column] = pd.to_datetime(df[column], utc=True, format="ISO8601")
    for column in ("fmisid", "kafka_partition", "kafka_offset"):
        df[column] = df[column].astype("Int64")
    for column in ("station_name", "round_id"):
        df[column] = df[column].astype("string")
    measurement_columns = [c for c in RAW_COLUMNS if c not in TIMESTAMP_COLUMNS
                           and c not in ("fmisid", "kafka_partition", "kafka_offset", "station_name", "round_id")]
    for column in measurement_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")
    return df


def load_batch(client, settings: Settings, df: pd.DataFrame, retries: int = 5,
               sleep: Callable[[float], None] = time.sleep) -> int:
    """Append the DataFrame to the raw table with a (free) load job. Returns rows loaded."""
    from google.cloud import bigquery

    from fmi_pipeline.bq import RAW_SCHEMA

    job_config = bigquery.LoadJobConfig(
        schema=RAW_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    delay = 5.0
    for attempt in range(1, retries + 1):
        try:
            job = client.load_table_from_dataframe(df, settings.raw_table_id, job_config=job_config)
            job.result(timeout=300)
            return int(job.output_rows or len(df))
        except Exception as err:  # noqa: BLE001
            if attempt == retries:
                raise
            log.warning("BigQuery load failed (attempt %d/%d): %r; retrying in %.0fs", attempt, retries, err, delay)
            sleep(delay)
            delay = min(delay * 2, 120)
    return 0


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from confluent_kafka import Consumer, KafkaError

    from fmi_pipeline.bq import ensure_raw_table, make_client

    settings = Settings()
    client = make_client(settings)
    table = ensure_raw_table(client, settings)
    log.info("Loading into %s.%s.%s", table.project, table.dataset_id, table.table_id)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit manually after BigQuery accepted the batch
    })
    consumer.subscribe([settings.kafka_topic])

    stopping = False

    def stop(signum, _frame) -> None:
        nonlocal stopping
        log.info("Received signal %s, flushing and stopping", signum)
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    batch = Batch()
    skipped = 0

    def flush() -> None:
        nonlocal skipped
        df = to_dataframe(batch.rows, datetime.now(timezone.utc))
        loaded = load_batch(client, settings, df)
        consumer.commit(asynchronous=False)
        log.info("Loaded %d rows (%d stations) into BigQuery; skipped %d bad messages",
                 loaded, df["fmisid"].nunique(), skipped)
        batch.clear()
        skipped = 0

    try:
        while not stopping:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", msg.error())
                else:
                    row = parse_message(msg.value(), msg.partition(), msg.offset())
                    if row is None:
                        skipped += 1
                        log.warning("Skipping malformed message at offset %s", msg.offset())
                    else:
                        batch.add(row, now)
            if batch.should_flush(now, settings.flush_idle_seconds, settings.flush_max_wait_seconds,
                                  settings.flush_max_rows):
                flush()
        if batch.rows:
            flush()
    finally:
        consumer.close()
        log.info("Consumer stopped")


if __name__ == "__main__":
    main()
