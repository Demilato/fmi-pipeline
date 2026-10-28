"""Daily processing of FMI observations in BigQuery.

Runs every day at 00:15 UTC and processes the previous UTC day:

    resolve_day -> check_raw_data -> build_daily_table -> quality_report
                                                      \\-> update_longterm_table (x5, one per station)

Trigger manually with {"day": "YYYY-MM-DD"} to (re)process any day, e.g. today for a demo.
Every step is idempotent: re-running a day replaces that day's tables.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from fmi_bq import (
    BQConfig,
    ensure_longterm_table,
    longterm_stations,
    make_client,
    query_rows,
    query_to_table,
    render,
)

log = logging.getLogger(__name__)


@dag(
    dag_id="fmi_daily_processing",
    description="Deduplicate and quality-check FMI raw data into daily and long-term BigQuery tables",
    schedule=CronTriggerTimetable("15 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "day": Param(
            None,
            type=["null", "string"],
            description="UTC day to process (YYYY-MM-DD). Empty = the day before the run.",
        )
    },
    tags=["fmi", "bigquery"],
)
def fmi_daily_processing():
    @task
    def resolve_day() -> str:
        ctx = get_current_context()
        requested = ctx["params"].get("day")
        if requested:
            try:
                return date.fromisoformat(requested).isoformat()
            except ValueError as err:
                raise AirflowFailException(f"Invalid day {requested!r}, expected YYYY-MM-DD") from err
        reference = ctx.get("logical_date") or ctx["dag_run"].run_after
        return (reference - timedelta(days=1)).date().isoformat()

    @task
    def check_raw_data(day: str) -> int:
        cfg = BQConfig()
        rows = query_rows(
            make_client(cfg),
            cfg,
            f"SELECT COUNT(*) AS n, COUNT(DISTINCT fmisid) AS stations FROM `{cfg.raw_table}` "
            "WHERE obs_time >= TIMESTAMP(@day) AND obs_time < TIMESTAMP(DATE_ADD(@day, INTERVAL 1 DAY))",
            {"day": date.fromisoformat(day)},
        )
        n, stations = rows[0]["n"], rows[0]["stations"]
        log.info("Raw data for %s: %d rows from %d stations", day, n, stations)
        if n == 0:
            raise AirflowFailException(f"No raw observations for {day}: is the producer/consumer running?")
        return n

    @task
    def build_daily_table(day: str, raw_rows: int) -> dict:
        cfg = BQConfig()
        d = date.fromisoformat(day)
        sql = render("daily_observations", raw_table=cfg.raw_table, daily_table=cfg.daily_table(d))
        stats = query_to_table(make_client(cfg), cfg, sql, cfg.daily_table(d), {"day": d},
                               clustering_fields=["fmisid"])
        stats["duplicates_removed"] = raw_rows - stats["rows"]
        log.info("Daily table %(destination)s: %(rows)s rows, %(duplicates_removed)s duplicates removed", stats)
        return stats

    @task
    def quality_report(day: str, daily: dict) -> dict:
        cfg = BQConfig()
        d = date.fromisoformat(day)
        client = make_client(cfg)
        sql = render("daily_quality_report", raw_table=cfg.raw_table, daily_table=cfg.daily_table(d),
                     quality_table=cfg.quality_table(d))
        stats = query_to_table(client, cfg, sql, cfg.quality_table(d), {"day": d})
        summary = query_rows(
            client,
            cfg,
            "SELECT COUNT(*) AS stations, SUM(rows_after_dedup) AS total_rows, SUM(duplicates_removed) AS duplicates, "
            "SUM(rows_flagged) AS flagged, SUM(missing_temperature) AS missing_temperature, "
            "ROUND(AVG(completeness_pct), 1) AS avg_completeness_pct "
            f"FROM `{cfg.quality_table(d)}`",
            {},
        )[0]
        log.info("Quality summary for %s: %s", day, summary)
        return {**stats, **{k: (float(v) if v is not None else None) for k, v in summary.items()}}

    @task
    def update_longterm_table(fmisid: int, day: str, daily: dict) -> dict:
        cfg = BQConfig()
        d = date.fromisoformat(day)
        client = make_client(cfg)
        ensure_longterm_table(client, cfg, fmisid)
        sql = render("longterm_station", daily_table=cfg.daily_table(d),
                     longterm_table=cfg.longterm_table(fmisid))
        stats = query_to_table(client, cfg, sql, cfg.longterm_table(fmisid), {"fmisid": int(fmisid)})
        log.info("Long-term table %(destination)s now has %(rows)s rows", stats)
        return stats

    day = resolve_day()
    raw_rows = check_raw_data(day)
    daily = build_daily_table(day, raw_rows)
    quality_report(day, daily)
    update_longterm_table.partial(day=day, daily=daily).expand(fmisid=longterm_stations())


fmi_daily_processing()
