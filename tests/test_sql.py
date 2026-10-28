from datetime import date

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("sqlglot")

import fmi_bq  # noqa: E402
from fake_bigquery import FakeBigQuery  # noqa: E402
from sample_data import seed_raw  # noqa: E402

DAY = date(2026, 10, 27)


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "fmi-pipeline")
    fake = FakeBigQuery()
    info = seed_raw(fake)
    return fake, fmi_bq.BQConfig(), info


def build_daily(fake, cfg):
    sql = fmi_bq.render("daily_observations", raw_table=cfg.raw_table, daily_table=cfg.daily_table(DAY))
    return fmi_bq.query_to_table(fake, cfg, sql, cfg.daily_table(DAY), {"day": DAY}, ["fmisid"])


def test_daily_table_dedups_filters_day_and_flags(env):
    fake, cfg, info = env
    stats = build_daily(fake, cfg)
    assert stats["rows"] == 144 + 72 + 10  # duplicates and next-day rows removed
    assert fake.byte_caps[-1] == 1_000_000_000  # byte cap applied

    rows = {(r["fmisid"]): r for r in fake.rows(
        "SELECT fmisid, COUNT(*) n, COUNT_IF(qc_temperature_range) tr, COUNT_IF(qc_temperature_spike) sp, "
        "COUNT_IF(qc_humidity_range) hr, COUNT_IF(NOT qc_ok) bad, SUM(missing_core_params) miss "
        "FROM fmi_daily__obs_20261027 GROUP BY fmisid")}
    assert rows[100971]["n"] == 144 and rows[100971]["bad"] == 0
    assert rows[101124]["tr"] == 1
    assert rows[101124]["sp"] == 2  # jump up to 99 and back down
    assert rows[101124]["miss"] == 2  # missing temperature -> temperature and dew point missing
    assert rows[101932]["hr"] == 1


def test_quality_report(env):
    fake, cfg, info = env
    build_daily(fake, cfg)
    sql = fmi_bq.render("daily_quality_report", raw_table=cfg.raw_table, daily_table=cfg.daily_table(DAY),
                        quality_table=cfg.quality_table(DAY))
    fmi_bq.query_to_table(fake, cfg, sql, cfg.quality_table(DAY), {"day": DAY})
    report = {r["fmisid"]: r for r in fake.rows("SELECT * FROM fmi_daily__quality_20261027")}
    assert report[100971]["duplicates_removed"] == 144
    assert report[100971]["completeness_pct"] == 100.0
    assert report[100971]["max_gap_minutes"] == 10
    assert report[101124]["completeness_pct"] == 50.0
    assert report[101124]["max_gap_minutes"] == 20
    assert report[101124]["missing_temperature"] == 1
    assert report[101124]["rows_flagged"] == 2
    assert str(report[100971]["report_day"]) == "2026-10-27"


def test_longterm_merge_is_idempotent(env):
    fake, cfg, _ = env
    build_daily(fake, cfg)
    fmi_bq.ensure_longterm_table(fake, cfg, 100971)
    sql = fmi_bq.render("longterm_station", daily_table=cfg.daily_table(DAY),
                        longterm_table=cfg.longterm_table(100971))
    for _ in range(2):  # processing the same day twice must not duplicate rows
        stats = fmi_bq.query_to_table(fake, cfg, sql, cfg.longterm_table(100971), {"fmisid": 100971})
        assert stats["rows"] == 144
    assert fake.rows("SELECT COUNT(DISTINCT fmisid) n FROM fmi_longterm__station_100971")[0]["n"] == 1


def test_identifiers_are_validated():
    with pytest.raises(ValueError):
        fmi_bq.render("daily_observations", raw_table="p.d.t`; DROP", daily_table="p.d.x")
