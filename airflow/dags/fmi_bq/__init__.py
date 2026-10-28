"""BigQuery helpers for the daily processing DAG.

Sandbox-safe by design: every step is a SELECT whose result is written to a destination
table (WRITE_TRUNCATE). No DML (INSERT/UPDATE/MERGE) and no streaming, which the
BigQuery sandbox does not support. Every query carries a maximum_bytes_billed cap.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

SQL_DIR = Path(__file__).parent / "sql"
_IDENT = re.compile(r"^[A-Za-z0-9_\-]+$")

# All NULLABLE: WRITE_TRUNCATE replaces the schema with the query result schema, which is nullable.
LONGTERM_SCHEMA = [
    ("obs_time", "TIMESTAMP", "NULLABLE"),
    ("fmisid", "INT64", "NULLABLE"),
    ("station_name", "STRING", "NULLABLE"),
    ("temperature", "FLOAT64", "NULLABLE"),
    ("humidity", "FLOAT64", "NULLABLE"),
    ("dew_point", "FLOAT64", "NULLABLE"),
    ("pressure", "FLOAT64", "NULLABLE"),
    ("wind_speed", "FLOAT64", "NULLABLE"),
    ("wind_gust", "FLOAT64", "NULLABLE"),
    ("precipitation_1h", "FLOAT64", "NULLABLE"),
    ("qc_ok", "BOOL", "NULLABLE"),
    ("processed_at", "TIMESTAMP", "NULLABLE"),
]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _ident(value: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"Invalid BigQuery identifier: {value!r}")
    return value


@dataclass(frozen=True)
class BQConfig:
    project: str = field(default_factory=lambda: _ident(_env("GCP_PROJECT_ID", "fmi-pipeline")))
    location: str = field(default_factory=lambda: _env("BQ_LOCATION", "europe-north1"))
    raw_dataset: str = field(default_factory=lambda: _ident(_env("BQ_RAW_DATASET", "fmi_raw")))
    raw_table_name: str = field(default_factory=lambda: _ident(_env("BQ_RAW_TABLE", "observations")))
    daily_dataset: str = field(default_factory=lambda: _ident(_env("BQ_DAILY_DATASET", "fmi_daily")))
    longterm_dataset: str = field(default_factory=lambda: _ident(_env("BQ_LONGTERM_DATASET", "fmi_longterm")))
    max_bytes_billed: int = field(default_factory=lambda: int(_env("BQ_MAX_BYTES_BILLED", "1000000000")))

    @property
    def raw_table(self) -> str:
        return f"{self.project}.{self.raw_dataset}.{self.raw_table_name}"

    def daily_table(self, day: date) -> str:
        return f"{self.project}.{self.daily_dataset}.obs_{day:%Y%m%d}"

    def quality_table(self, day: date) -> str:
        return f"{self.project}.{self.daily_dataset}.quality_{day:%Y%m%d}"

    def longterm_table(self, fmisid: int) -> str:
        return f"{self.project}.{self.longterm_dataset}.station_{int(fmisid)}"


def longterm_stations() -> list[int]:
    raw = _env("LONGTERM_STATIONS", "100971,101124,101339,101786,101932")
    return [int(part) for part in raw.split(",") if part.strip()]


def render(name: str, **tables: str) -> str:
    """Load sql/<name>.sql and fill in table names (identifiers only; values use query parameters)."""
    for value in tables.values():
        for part in value.split("."):
            _ident(part)
    return (SQL_DIR / f"{name}.sql").read_text().format(**tables)


def make_client(cfg: BQConfig):
    from google.cloud import bigquery

    return bigquery.Client(project=cfg.project, location=cfg.location)


def _job_config(cfg: BQConfig, params: dict[str, Any], **kwargs):
    from google.cloud import bigquery

    query_params = []
    for key, value in params.items():
        if isinstance(value, date):
            query_params.append(bigquery.ScalarQueryParameter(key, "DATE", value))
        elif isinstance(value, int):
            query_params.append(bigquery.ScalarQueryParameter(key, "INT64", value))
        else:
            query_params.append(bigquery.ScalarQueryParameter(key, "STRING", str(value)))
    config = bigquery.QueryJobConfig(query_parameters=query_params, **kwargs)
    if cfg.max_bytes_billed > 0:
        config.maximum_bytes_billed = cfg.max_bytes_billed
    return config


def query_rows(client, cfg: BQConfig, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    job = client.query(sql, job_config=_job_config(cfg, params))
    return [dict(row.items()) for row in job.result()]


def query_to_table(client, cfg: BQConfig, sql: str, destination: str, params: dict[str, Any],
                   clustering_fields: list[str] | None = None) -> dict[str, Any]:
    """Run a SELECT and replace ``destination`` with its result. Returns job statistics."""
    from google.cloud import bigquery

    config = _job_config(
        cfg,
        params,
        destination=destination,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        clustering_fields=clustering_fields,
    )
    job = client.query(sql, job_config=config)
    job.result()
    table = client.get_table(destination)
    return {
        "destination": destination,
        "rows": table.num_rows,
        "bytes_processed": job.total_bytes_processed,
        "job_id": job.job_id,
    }


def ensure_longterm_table(client, cfg: BQConfig, fmisid: int):
    from google.cloud import bigquery

    schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in LONGTERM_SCHEMA]
    table = bigquery.Table(cfg.longterm_table(fmisid), schema=schema)
    table.description = f"Long-term observations of FMI station {fmisid} (updated daily by Airflow)."
    return client.create_table(table, exists_ok=True)
