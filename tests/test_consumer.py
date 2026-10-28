import json
from datetime import datetime, timezone

from google.cloud.bigquery import _pandas_helpers

from fmi_pipeline.bq import RAW_COLUMNS, RAW_SCHEMA
from fmi_pipeline.config import Settings
from fmi_pipeline.consumer import Batch, load_batch, parse_message, to_dataframe

ROW = {
    "fmisid": 101004, "station_name": "Helsinki Kumpula", "latitude": 60.2, "longitude": 24.96,
    "obs_time": "2026-10-28T12:20:00+00:00", "temperature": 6.3, "dew_point": 3.1, "humidity": 80.0,
    "pressure": 1003.3, "wind_speed": 4.4, "wind_gust": 7.9, "wind_direction": 235.0, "cloud_cover": 8.0,
    "precipitation_1h": None, "fetched_at": "2026-10-28T12:28:01.123456+00:00", "round_id": "r1",
}


def test_parse_message_adds_kafka_metadata_and_rejects_garbage():
    row = parse_message(json.dumps(ROW).encode(), 0, 42)
    assert row["kafka_offset"] == 42 and row["kafka_partition"] == 0
    assert parse_message(b"not json", 0, 1) is None
    assert parse_message(b'{"x": 1}', 0, 1) is None


def test_batch_flush_rules():
    b = Batch()
    assert not b.should_flush(100, 30, 600, 10)
    b.add({}, 100)
    assert not b.should_flush(110, 30, 600, 10)
    assert b.should_flush(131, 30, 600, 10)  # idle: the producer round is over
    for i in range(9):
        b.add({}, 101 + i)
    assert b.should_flush(111, 30, 600, 10)  # max rows


def test_dataframe_matches_bigquery_schema():
    rows = [parse_message(json.dumps(ROW).encode(), 0, i) for i in range(3)]
    rows[1]["temperature"] = None
    df = to_dataframe(rows, datetime(2026, 10, 28, 12, 29, tzinfo=timezone.utc))
    assert list(df.columns) == RAW_COLUMNS
    assert str(df["obs_time"].dtype).startswith("datetime64") and str(df["obs_time"].dt.tz) == "UTC"
    # The exact conversion google-cloud-bigquery runs before a load job:
    arrow = _pandas_helpers.dataframe_to_arrow(df, RAW_SCHEMA)
    assert arrow.num_rows == 3
    assert arrow.schema.field("fmisid").type == "int64"


def test_load_batch_uses_append_load_job_and_retries():
    calls = []

    class Job:
        output_rows = 3

        def result(self, timeout):
            return self

    class Client:
        def load_table_from_dataframe(self, df, table_id, job_config):
            calls.append((table_id, job_config.write_disposition))
            if len(calls) == 1:
                raise RuntimeError("transient")
            return Job()

    settings = Settings(gcp_project_id="p")
    df = to_dataframe([parse_message(json.dumps(ROW).encode(), 0, 1)], datetime.now(timezone.utc))
    assert load_batch(Client(), settings, df, sleep=lambda s: None) == 3
    assert calls == [("p.fmi_raw.observations", "WRITE_APPEND")] * 2
