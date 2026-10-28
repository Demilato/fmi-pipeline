"""DAG structure + an end-to-end dag.test() run against the DuckDB fake.

Needs Apache Airflow installed and an initialised metadata DB:
    AIRFLOW_HOME=$(mktemp -d) airflow db migrate && AIRFLOW_HOME=... pytest tests/test_dag.py
"""

import pytest

pytest.importorskip("airflow.sdk")  # the repo has an airflow/ folder, so check the real package
pytest.importorskip("duckdb")


@pytest.fixture()
def dag_module(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "fmi-pipeline")
    monkeypatch.setenv("LONGTERM_STATIONS", "100971,101124,101339,101786,101932")
    import fmi_daily_processing

    return fmi_daily_processing


def test_dag_structure(dag_module):
    dag = dag_module.fmi_daily_processing()
    assert set(dag.task_ids) == {
        "resolve_day", "check_raw_data", "build_daily_table", "quality_report", "update_longterm_table",
    }
    assert dag.get_task("build_daily_table").upstream_task_ids == {"resolve_day", "check_raw_data"}
    assert dag.get_task("update_longterm_table").upstream_task_ids == {"resolve_day", "build_daily_table"}
    assert dag.catchup is False and dag.max_active_runs == 1


def test_dag_run_end_to_end(dag_module, monkeypatch):
    from fake_bigquery import FakeBigQuery
    from sample_data import seed_raw

    fake = FakeBigQuery()
    seed_raw(fake)
    monkeypatch.setattr(dag_module, "make_client", lambda cfg: fake)

    dag = dag_module.fmi_daily_processing()
    run = dag.test(run_conf={"day": "2026-10-27"})
    assert str(run.state) == "success"

    tables = {r["table_name"] for r in fake.rows("SELECT table_name FROM information_schema.tables")}
    assert {"fmi_daily__obs_20261027", "fmi_daily__quality_20261027"} <= tables
    for fmisid in (100971, 101124, 101339, 101786, 101932):
        assert f"fmi_longterm__station_{fmisid}" in tables
    counts = {fmisid: fake.rows(f"SELECT COUNT(*) n FROM fmi_longterm__station_{fmisid}")[0]["n"]
              for fmisid in (100971, 101124, 101339)}
    assert counts == {100971: 144, 101124: 72, 101339: 0}  # 101339 had no data in the sample


def test_resolve_day_defaults_to_previous_utc_day(dag_module, monkeypatch):
    from types import SimpleNamespace

    import pendulum

    dag = dag_module.fmi_daily_processing()
    resolve = dag.get_task("resolve_day").python_callable
    run_time = pendulum.datetime(2026, 10, 28, 0, 15, tz="UTC")

    monkeypatch.setattr(dag_module, "get_current_context",
                        lambda: {"params": {"day": None}, "logical_date": run_time})
    assert resolve() == "2026-10-27"
    # manual runs in Airflow 3 may have no logical_date: fall back to run_after
    monkeypatch.setattr(dag_module, "get_current_context",
                        lambda: {"params": {}, "logical_date": None,
                                 "dag_run": SimpleNamespace(run_after=run_time)})
    assert resolve() == "2026-10-27"
    monkeypatch.setattr(dag_module, "get_current_context", lambda: {"params": {"day": "2026-10-28"}})
    assert resolve() == "2026-10-28"
