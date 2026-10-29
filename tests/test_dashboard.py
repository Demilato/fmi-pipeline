"""Smoke test: the Streamlit app renders all three tabs against the DuckDB fake."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("duckdb")

from streamlit.testing.v1 import AppTest  # noqa: E402

import fmi_bq  # noqa: E402
import fmi_pipeline.bq  # noqa: E402
from fake_bigquery import FakeBigQuery  # noqa: E402
from sample_data import seed_raw  # noqa: E402

APP = Path(__file__).resolve().parents[1] / "app" / "dashboard" / "app.py"


def prepared_fake(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "fmi-pipeline")
    fake = FakeBigQuery()
    seed_raw(fake)
    # a few "live" rows so the latest-observations tab has data
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    for i, fmisid in enumerate((100971, 101124, 101932)):
        fake.con.execute(
            "INSERT INTO fmi_raw__observations (fmisid, station_name, latitude, longitude, obs_time, temperature, "
            "humidity, fetched_at, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [fmisid, f"Station {fmisid}", 60.2 + i, 24.9 + i, now - timedelta(minutes=20), 3.0 + i, 85.0, now, now],
        )
    cfg, day = fmi_bq.BQConfig(), date(2026, 10, 27)
    fmi_bq.query_to_table(fake, cfg, fmi_bq.render("daily_observations", raw_table=cfg.raw_table,
                          daily_table=cfg.daily_table(day)), cfg.daily_table(day), {"day": day})
    fmi_bq.query_to_table(fake, cfg, fmi_bq.render("daily_quality_report", raw_table=cfg.raw_table,
                          daily_table=cfg.daily_table(day), quality_table=cfg.quality_table(day)),
                          cfg.quality_table(day), {"day": day})
    for fmisid in (100971, 101124):
        fmi_bq.ensure_longterm_table(fake, cfg, fmisid)
        fmi_bq.query_to_table(fake, cfg, fmi_bq.render("longterm_station", daily_table=cfg.daily_table(day),
                              longterm_table=cfg.longterm_table(fmisid)), cfg.longterm_table(fmisid),
                              {"fmisid": fmisid})
    monkeypatch.setattr(fmi_pipeline.bq, "make_client", lambda settings: fake)
    return fake


def test_dashboard_renders(monkeypatch):
    prepared_fake(monkeypatch)
    at = AppTest.from_file(str(APP), default_timeout=60).run()
    assert not at.exception, at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Stations reporting (3 h)"] == "3"
    assert metrics["Stations"] == "3"  # quality report of 2026-10-27
    assert metrics["Duplicates removed"] == "144"
    assert len(at.dataframe) == 2  # recent records + quality report
    assert not [i.value for i in at.info]  # every tab had data


def test_dashboard_imports_work_like_in_the_container():
    """`streamlit run dashboard/app.py` puts only dashboard/ on sys.path (not the working directory),
    so the image must set PYTHONPATH for `import fmi_pipeline` to work."""
    import re
    import subprocess
    import sys

    app_dir = APP.parents[1]
    dockerfile = (app_dir / "Dockerfile").read_text()
    match = re.search(r"PYTHONPATH=(\S+)", dockerfile)
    assert match and match.group(1) == "/app", "Dockerfile must set PYTHONPATH=/app"

    # /app in the image corresponds to app/ in the repo
    code = "import sys; sys.path[0] = 'dashboard'; import fmi_pipeline.bq, fmi_pipeline.config"
    env = {"PATH": "", "PYTHONPATH": str(app_dir)}
    result = subprocess.run([sys.executable, "-c", code], cwd=app_dir, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
