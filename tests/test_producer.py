import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fmi_weather_client.errors import ClientError, ServerError

from fmi_pipeline import fmi_client
from fmi_pipeline.fmi_client import StationNoData, fetch_station_observations
from fmi_pipeline.producer import RateLimiter, StationTracker, next_run_time, run_round
from fmi_pipeline.stations import Station, load_stations

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (ROOT / "tests" / "fixtures" / "fmi_observation_101004.xml").read_text()


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, value, key, on_delivery=None):
        self.messages.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


def test_load_stations_from_course_csv():
    stations = load_stations(ROOT / "data" / "FMI_stations.csv")
    ids = {s.fmisid for s in stations}
    assert len(stations) == len(ids)  # duplicates removed
    assert 400 < len(stations) < 500  # only stations without a finish year
    kaisaniemi = next(s for s in stations if s.fmisid == 100971)
    assert kaisaniemi.name == "Helsinki Kaisaniemi"
    assert any("ä" in s.name for s in stations)  # Latin-1 decoded correctly
    # the five long-term stations are all in the list
    assert {100971, 101124, 101339, 101786, 101932} <= ids


def test_fetch_keeps_every_time_step_and_cleans_nan(monkeypatch):
    monkeypatch.setattr(fmi_client.fmi_http, "request_observation_by_station_id", lambda sid: FIXTURE)
    result = fetch_station_observations(101004)
    assert result.place == "Helsinki Kumpula"
    assert len(result.observations) == 2  # the all-NaN time step is dropped by the library

    fetched = datetime(2026, 10, 28, 12, 28, tzinfo=timezone.utc)
    rec = fmi_client.to_record(101004, result, result.observations[1], fetched, "r1")
    assert rec["temperature"] == 6.3
    assert rec["humidity"] == 80.0
    assert rec["precipitation_1h"] is None  # NaN -> None
    assert rec["cloud_cover"] == 8.0
    assert rec["obs_time"].endswith("+00:00")
    json.dumps(rec, allow_nan=False)  # must be valid JSON


def test_fetch_no_data_is_station_no_data(monkeypatch):
    def raise_no_data(sid):
        raise ClientError(200, "Valid data source not found with given parameters")

    monkeypatch.setattr(fmi_client.fmi_http, "request_observation_by_station_id", raise_no_data)
    with pytest.raises(StationNoData):
        fetch_station_observations(1)


def test_round_publishes_and_tracks_station_status(monkeypatch):
    monkeypatch.setattr(fmi_client.fmi_http, "request_observation_by_station_id", lambda sid: FIXTURE)
    stations = [Station(1, "ok", 60, 25), Station(2, "closed", 61, 25), Station(3, "flaky", 62, 25)]

    def fetch(sid):
        if sid == 2:
            raise StationNoData("none")
        if sid == 3:
            raise ServerError(503, "busy")
        return fetch_station_observations(sid)

    tracker = StationTracker(recheck_seconds=3600, max_inactive_per_round=10)
    producer = FakeProducer()
    now = datetime(2026, 10, 28, 12, 28, tzinfo=timezone.utc)
    stats = run_round(stations, tracker, producer, "topic", RateLimiter(0), fetch=fetch, now=lambda: now)

    assert (stats.stations_ok, stats.stations_no_data, stats.stations_error) == (1, 1, 1)
    assert stats.messages == 2 and len(producer.messages) == 2
    assert producer.messages[0][1] == b"1"
    assert tracker.states[1].status == "active"
    assert tracker.states[2].status == "inactive"
    assert tracker.states[3].status == "unknown"  # transient error does not deactivate

    # next round: the inactive station is skipped until its re-check time
    due = [s.fmisid for s in tracker.due(stations, now.timestamp() + 60)]
    assert due == [1, 3]
    due_later = [s.fmisid for s in tracker.due(stations, now.timestamp() + 3601)]
    assert due_later == [1, 2, 3]


def test_tracker_state_roundtrip(tmp_path):
    tracker = StationTracker(3600, 5)
    tracker.mark_ok(1)
    tracker.mark_no_data(2, 100.0)
    tracker.take_request(datetime(2026, 10, 28, 12, tzinfo=timezone.utc))
    path = tmp_path / "state" / "s.json"
    tracker.save(str(path))
    restored = StationTracker(3600, 5)
    restored.load(str(path))
    assert restored.states[1].status == "active"
    assert restored.states[2].next_check == 3700.0
    assert (restored.requests_day, restored.requests_today) == ("2026-10-28", 1)


def test_daily_request_budget_stops_round_and_resets_next_day(monkeypatch):
    monkeypatch.setattr(fmi_client.fmi_http, "request_observation_by_station_id", lambda sid: FIXTURE)
    stations = [Station(i, f"s{i}", 60, 25) for i in range(1, 6)]
    tracker = StationTracker(3600, 10, daily_budget=3)
    day1 = datetime(2026, 10, 28, 23, 50, tzinfo=timezone.utc)
    stats = run_round(stations, tracker, FakeProducer(), "t", RateLimiter(0), now=lambda: day1)
    assert stats.requested == 3 and stats.budget_exhausted
    day2 = datetime(2026, 10, 29, 0, 8, tzinfo=timezone.utc)
    stats = run_round(stations, tracker, FakeProducer(), "t", RateLimiter(0), now=lambda: day2)
    assert stats.requested == 3 and tracker.requests_day == "2026-10-29"


def test_next_run_time_aligns_to_slots():
    t = datetime(2026, 10, 28, 12, 9, 30, tzinfo=timezone.utc)
    assert next_run_time(t, 20, 8) == datetime(2026, 10, 28, 12, 28, tzinfo=timezone.utc)
    t = datetime(2026, 10, 28, 12, 50, tzinfo=timezone.utc)
    assert next_run_time(t, 20, 8) == datetime(2026, 10, 28, 13, 8, tzinfo=timezone.utc)


def test_rate_limiter_spacing():
    clock = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        clock[0] += s

    limiter = RateLimiter(2.0, clock=lambda: clock[0], sleep=sleep)
    for _ in range(3):
        limiter.wait()
    assert slept == [0.5, 0.5]
