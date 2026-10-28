"""BigQuery schema and helpers shared by the consumer, dashboard and setup check.

Cost notes (BigQuery sandbox, no billing account):
- Data is written with *load jobs*, which are free. Streaming inserts and DML
  (INSERT/UPDATE/MERGE) are not available in the sandbox and are not used anywhere.
- The raw table requires a partition filter on obs_time, so a query cannot
  accidentally scan the whole table.
"""

from __future__ import annotations

from google.cloud import bigquery

from fmi_pipeline.config import Settings

RAW_SCHEMA = [
    bigquery.SchemaField("fmisid", "INT64", mode="REQUIRED", description="FMI station id"),
    bigquery.SchemaField("station_name", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT64"),
    bigquery.SchemaField("longitude", "FLOAT64"),
    bigquery.SchemaField("obs_time", "TIMESTAMP", mode="REQUIRED", description="Observation time (UTC)"),
    bigquery.SchemaField("temperature", "FLOAT64", description="Air temperature, degC"),
    bigquery.SchemaField("dew_point", "FLOAT64", description="Dew point, degC"),
    bigquery.SchemaField("humidity", "FLOAT64", description="Relative humidity, %"),
    bigquery.SchemaField("pressure", "FLOAT64", description="Sea-level pressure, hPa"),
    bigquery.SchemaField("wind_speed", "FLOAT64", description="Wind speed (10 min avg), m/s"),
    bigquery.SchemaField("wind_gust", "FLOAT64", description="Wind gust, m/s"),
    bigquery.SchemaField("wind_direction", "FLOAT64", description="Wind direction, degrees"),
    bigquery.SchemaField("cloud_cover", "FLOAT64", description="Total cloud cover, oktas (0-8, 9 = obscured)"),
    bigquery.SchemaField("precipitation_1h", "FLOAT64", description="Precipitation in the last hour, mm"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP", description="When the producer fetched the observation"),
    bigquery.SchemaField("round_id", "STRING", description="Producer polling round"),
    bigquery.SchemaField("kafka_partition", "INT64"),
    bigquery.SchemaField("kafka_offset", "INT64"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", description="When the consumer loaded the batch"),
]

RAW_COLUMNS = [f.name for f in RAW_SCHEMA]
TIMESTAMP_COLUMNS = [f.name for f in RAW_SCHEMA if f.field_type == "TIMESTAMP"]


def make_client(settings: Settings) -> bigquery.Client:
    settings.require_gcp()
    return bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)


def query_config(settings: Settings, **kwargs) -> bigquery.QueryJobConfig:
    """QueryJobConfig with the byte cap applied (see BQ_MAX_BYTES_BILLED)."""
    config = bigquery.QueryJobConfig(**kwargs)
    if settings.bq_max_bytes_billed > 0:
        config.maximum_bytes_billed = settings.bq_max_bytes_billed
    return config


def ensure_raw_table(client: bigquery.Client, settings: Settings) -> bigquery.Table:
    """Create the raw table (day-partitioned on obs_time, clustered by fmisid) if it is missing."""
    table = bigquery.Table(settings.raw_table_id, schema=RAW_SCHEMA)
    table.description = "Raw FMI observations as received from Kafka (may contain duplicates)."
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="obs_time"
    )
    table.require_partition_filter = True
    table.clustering_fields = ["fmisid"]
    return client.create_table(table, exists_ok=True)
