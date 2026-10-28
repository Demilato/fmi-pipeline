"""Synthetic raw observations with known duplicates, gaps and outliers."""

from __future__ import annotations

from datetime import datetime, timedelta

from google.cloud import bigquery

from fake_bigquery import FakeBigQuery
from fmi_pipeline.bq import RAW_SCHEMA

DAY = datetime(2026, 10, 27)
RAW_TABLE = "fmi-pipeline.fmi_raw.observations"


def seed_raw(fake: FakeBigQuery) -> dict:
    fake.create_table(bigquery.Table(RAW_TABLE, schema=RAW_SCHEMA))
    rows = []
    offset = 0

    def add(fmisid, name, obs_time, temperature, humidity=80.0, fetched_delay=8, dew_point=None, pressure=1005.0):
        nonlocal offset
        offset += 1
        rows.append((fmisid, name, 60.0, 25.0, obs_time, temperature,
                     temperature - 2 if dew_point is None and temperature is not None else dew_point,
                     humidity, pressure, 3.0, 6.0, 180.0, 8.0, 0.0,
                     obs_time + timedelta(minutes=fetched_delay), "r", 0, offset, obs_time + timedelta(minutes=9)))

    # Station 100971: full day of 10-minute data (144 obs), every obs fetched twice (overlapping windows)
    for i in range(144):
        t = DAY + timedelta(minutes=10 * i)
        add(100971, "Helsinki Kaisaniemi", t, 5.0 + (i % 6) * 0.1)
        add(100971, "Helsinki Kaisaniemi", t, 5.0 + (i % 6) * 0.1, fetched_delay=18)
    # Station 101124: 72 obs (every other slot missing), one outlier, one spike, one missing temperature
    for i in range(0, 144, 2):
        t = DAY + timedelta(minutes=10 * i)
        temp = 2.0
        if i == 10:
            temp = 99.0  # out of range -> also a spike vs the previous and next value
        if i == 50:
            temp = None
        add(101124, "Tampere Härmälä", t, temp)
    # Station 101932: humidity out of range once
    for i in range(10):
        add(101932, "Sodankylä Tähtelä", DAY + timedelta(minutes=10 * i), -3.0, humidity=120.0 if i == 3 else 90.0)
    # Data from the next day must not leak into the processed day
    add(100971, "Helsinki Kaisaniemi", DAY + timedelta(days=1, minutes=10), 4.0)

    placeholders = ", ".join(["?"] * len(RAW_SCHEMA))
    fake.con.executemany(f"INSERT INTO fmi_raw__observations VALUES ({placeholders})", rows)
    return {"raw_rows_day": len(rows) - 1}
