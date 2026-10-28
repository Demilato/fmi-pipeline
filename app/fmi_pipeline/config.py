"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def _int_list(raw: str) -> list[int]:
    return [int(part) for part in raw.split(",") if part.strip()]


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "fmi-observations"))
    kafka_consumer_group: str = field(default_factory=lambda: _env("KAFKA_CONSUMER_GROUP", "fmi-bigquery-loader"))

    # Producer / FMI
    stations_csv: str = field(default_factory=lambda: _env("STATIONS_CSV", "/data/FMI_stations.csv"))
    poll_interval_minutes: int = field(default_factory=lambda: int(_env("POLL_INTERVAL_MINUTES", "20")))
    poll_offset_minutes: int = field(default_factory=lambda: int(_env("POLL_OFFSET_MINUTES", "8")))
    fmi_max_requests_per_second: float = field(
        default_factory=lambda: float(_env("FMI_MAX_REQUESTS_PER_SECOND", "1.5"))
    )
    fmi_daily_request_budget: int = field(default_factory=lambda: int(_env("FMI_DAILY_REQUEST_BUDGET", "19000")))
    inactive_recheck_hours: float = field(default_factory=lambda: float(_env("INACTIVE_RECHECK_HOURS", "24")))
    max_inactive_rechecks_per_round: int = field(
        default_factory=lambda: int(_env("MAX_INACTIVE_RECHECKS_PER_ROUND", "40"))
    )
    state_file: str = field(default_factory=lambda: _env("PRODUCER_STATE_FILE", "/state/station_state.json"))

    # Consumer batching
    flush_idle_seconds: float = field(default_factory=lambda: float(_env("CONSUMER_FLUSH_IDLE_SECONDS", "30")))
    flush_max_wait_seconds: float = field(default_factory=lambda: float(_env("CONSUMER_FLUSH_MAX_WAIT_SECONDS", "600")))
    flush_max_rows: int = field(default_factory=lambda: int(_env("CONSUMER_FLUSH_MAX_ROWS", "20000")))

    # BigQuery
    gcp_project_id: str = field(default_factory=lambda: _env("GCP_PROJECT_ID", ""))
    bq_location: str = field(default_factory=lambda: _env("BQ_LOCATION", "europe-north1"))
    bq_raw_dataset: str = field(default_factory=lambda: _env("BQ_RAW_DATASET", "fmi_raw"))
    bq_raw_table: str = field(default_factory=lambda: _env("BQ_RAW_TABLE", "observations"))
    bq_daily_dataset: str = field(default_factory=lambda: _env("BQ_DAILY_DATASET", "fmi_daily"))
    bq_longterm_dataset: str = field(default_factory=lambda: _env("BQ_LONGTERM_DATASET", "fmi_longterm"))
    # Upper limit for bytes a single query may process. A query that would scan more fails instead
    # of eating the free 1 TiB/month allowance. Set to 0 to disable.
    bq_max_bytes_billed: int = field(default_factory=lambda: int(_env("BQ_MAX_BYTES_BILLED", "1000000000")))

    longterm_stations: list[int] = field(
        default_factory=lambda: _int_list(_env("LONGTERM_STATIONS", "100971,101124,101339,101786,101932"))
    )

    @property
    def raw_table_id(self) -> str:
        return f"{self.gcp_project_id}.{self.bq_raw_dataset}.{self.bq_raw_table}"

    def require_gcp(self) -> None:
        if not self.gcp_project_id:
            raise RuntimeError("GCP_PROJECT_ID is not set (see .env.example)")
