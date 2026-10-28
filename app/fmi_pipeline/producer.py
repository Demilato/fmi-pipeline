"""Kafka producer: polls the FMI API for all open stations and publishes each observation.

One Kafka message = one observation of one station (JSON, key = FMISID).
Run: python -m fmi_pipeline.producer
"""

from __future__ import annotations

import json
import logging
import os
import signal
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from fmi_pipeline.config import Settings
from fmi_pipeline.fmi_client import StationNoData, StationObservations, fetch_station_observations, to_record
from fmi_pipeline.stations import Station, load_stations

log = logging.getLogger("fmi.producer")

FMI_DAILY_REQUEST_LIMIT = 20_000  # FMI open data download service terms
DAILY_REQUEST_BUDGET = 19_000  # hard stop below the limit, resets at 00:00 UTC


class KafkaProducerLike(Protocol):
    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Callable | None = None) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


class RateLimiter:
    """Keeps requests at or below ``per_second`` (FMI allows 600 requests per 5 minutes)."""

    def __init__(self, per_second: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._interval = 1.0 / per_second if per_second > 0 else 0.0
        self._clock = clock
        self._sleep = sleep
        self._next = 0.0

    def wait(self) -> None:
        now = self._clock()
        if now < self._next:
            self._sleep(self._next - now)
            now = self._next
        self._next = now + self._interval


@dataclass
class StationState:
    status: str = "unknown"  # unknown | active | inactive
    next_check: float = 0.0  # epoch seconds; only used for inactive stations
    consecutive_errors: int = 0


@dataclass
class StationTracker:
    """Remembers which stations return data (inactive ones are only re-checked occasionally)
    and how many FMI requests were made today (UTC), so the daily limit is never exceeded."""

    recheck_seconds: float
    max_inactive_per_round: int
    daily_budget: int = DAILY_REQUEST_BUDGET
    states: dict[int, StationState] = field(default_factory=dict)
    requests_day: str = ""
    requests_today: int = 0

    def take_request(self, now: datetime) -> bool:
        """Count one FMI request; False if today's budget is used up."""
        day = now.date().isoformat()
        if day != self.requests_day:
            self.requests_day, self.requests_today = day, 0
        if self.requests_today >= self.daily_budget:
            return False
        self.requests_today += 1
        return True

    def due(self, stations: list[Station], now: float) -> list[Station]:
        due, rechecks = [], 0
        for station in stations:
            state = self.states.setdefault(station.fmisid, StationState())
            if state.status != "inactive":
                due.append(station)
            elif state.next_check <= now and rechecks < self.max_inactive_per_round:
                due.append(station)
                rechecks += 1
        return due

    def mark_ok(self, fmisid: int) -> None:
        self.states[fmisid] = StationState(status="active")

    def mark_no_data(self, fmisid: int, now: float) -> None:
        self.states[fmisid] = StationState(status="inactive", next_check=now + self.recheck_seconds)

    def mark_error(self, fmisid: int) -> None:
        # Network or server problem: not the station's fault, keep its status and retry next round.
        self.states.setdefault(fmisid, StationState()).consecutive_errors += 1

    def count(self, status: str) -> int:
        return sum(1 for s in self.states.values() if s.status == status)

    def save(self, path: str) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "stations": {str(k): vars(v) for k, v in self.states.items()},
            "requests": {"day": self.requests_day, "count": self.requests_today},
        }
        with tempfile.NamedTemporaryFile("w", dir=target.parent, delete=False, suffix=".tmp") as tmp:
            json.dump(data, tmp)
        os.replace(tmp.name, target)

    def load(self, path: str) -> None:
        try:
            data = json.loads(Path(path).read_text())
        except FileNotFoundError:
            return
        except (OSError, ValueError) as err:
            log.warning("Ignoring unreadable state file %s: %s", path, err)
            return
        self.states = {int(k): StationState(**v) for k, v in data.get("stations", {}).items()}
        self.requests_day = data.get("requests", {}).get("day", "")
        self.requests_today = int(data.get("requests", {}).get("count", 0))


@dataclass
class RoundStats:
    round_id: str
    requested: int = 0
    stations_ok: int = 0
    stations_no_data: int = 0
    stations_error: int = 0
    messages: int = 0
    delivery_failures: int = 0
    budget_exhausted: bool = False


def run_round(
    stations: list[Station],
    tracker: StationTracker,
    producer: KafkaProducerLike,
    topic: str,
    limiter: RateLimiter,
    fetch: Callable[[int], StationObservations] = fetch_station_observations,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    should_stop: Callable[[], bool] = lambda: False,
) -> RoundStats:
    started = now()
    stats = RoundStats(round_id=started.strftime("%Y%m%dT%H%M%SZ"))

    def on_delivery(err, _msg) -> None:
        if err is not None:
            stats.delivery_failures += 1
            log.error("Kafka delivery failed: %s", err)

    for station in tracker.due(stations, started.timestamp()):
        if should_stop():
            break
        if not tracker.take_request(now()):
            stats.budget_exhausted = True
            log.warning("Daily FMI request budget (%d) used up; skipping the rest until 00:00 UTC",
                        tracker.daily_budget)
            break
        limiter.wait()
        stats.requested += 1
        try:
            result = fetch(station.fmisid)
        except StationNoData:
            tracker.mark_no_data(station.fmisid, now().timestamp())
            stats.stations_no_data += 1
            continue
        except Exception as err:  # noqa: BLE001 - network/server/parse errors must not stop the round
            tracker.mark_error(station.fmisid)
            stats.stations_error += 1
            log.warning("Station %s (%s) failed: %r", station.fmisid, station.name, err)
            continue

        tracker.mark_ok(station.fmisid)
        stats.stations_ok += 1
        fetched_at = now()
        for weather in result.observations:
            record = to_record(station.fmisid, result, weather, fetched_at, stats.round_id, station.name)
            producer.produce(
                topic,
                value=json.dumps(record).encode("utf-8"),
                key=str(station.fmisid).encode("utf-8"),
                on_delivery=on_delivery,
            )
            stats.messages += 1
        producer.poll(0)  # serve delivery callbacks

    remaining = producer.flush(60)
    if remaining:
        log.error("%d messages were still undelivered after flush", remaining)
    return stats


def next_run_time(now: datetime, interval_minutes: int, offset_minutes: int) -> datetime:
    """Next wall-clock slot, e.g. interval 20 / offset 8 -> hh:08, hh:28, hh:48 (UTC).

    The offset gives FMI a few minutes to publish the latest 10-minute observation.
    """
    period = interval_minutes * 60
    offset = offset_minutes * 60
    ts = now.timestamp()
    nxt = ((ts - offset) // period + 1) * period + offset
    return datetime.fromtimestamp(nxt, timezone.utc)


def estimated_daily_requests(active_stations: int, inactive_stations: int, settings: Settings) -> int:
    rounds_per_day = 24 * 60 / settings.poll_interval_minutes
    inactive_rechecks = inactive_stations * 24 / settings.inactive_recheck_hours
    return int(active_stations * rounds_per_day + inactive_rechecks)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from confluent_kafka import Producer  # imported here so tests don't need librdkafka

    settings = Settings()
    stations = load_stations(settings.stations_csv)
    log.info("Loaded %d open stations from %s", len(stations), settings.stations_csv)

    tracker = StationTracker(settings.inactive_recheck_hours * 3600, settings.max_inactive_rechecks_per_round,
                             daily_budget=settings.fmi_daily_request_budget)
    tracker.load(settings.state_file)
    if tracker.states:
        log.info("Restored station state: %d active, %d inactive",
                 tracker.count("active"), tracker.count("inactive"))

    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "fmi-producer",
        "acks": "all",
        "enable.idempotence": True,  # broker drops duplicates caused by producer retries
        "linger.ms": 100,
        "compression.type": "gzip",
    })
    limiter = RateLimiter(settings.fmi_max_requests_per_second)

    stopping = False

    def stop(signum, _frame) -> None:
        nonlocal stopping
        log.info("Received signal %s, stopping after the current station", signum)
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while not stopping:
        stats = run_round(stations, tracker, producer, settings.kafka_topic, limiter,
                          should_stop=lambda: stopping)
        tracker.save(settings.state_file)
        active, inactive = tracker.count("active"), tracker.count("inactive")
        estimate = estimated_daily_requests(active, inactive, settings)
        log.info(
            "Round %s: requested=%d ok=%d no_data=%d errors=%d messages=%d | active=%d inactive=%d | "
            "FMI requests today=%d, estimated per day=%d (limit %d)",
            stats.round_id, stats.requested, stats.stations_ok, stats.stations_no_data, stats.stations_error,
            stats.messages, active, inactive, tracker.requests_today, estimate, FMI_DAILY_REQUEST_LIMIT,
        )
        if estimate > 0.9 * FMI_DAILY_REQUEST_LIMIT:
            log.warning("Close to FMI's daily request limit: increase POLL_INTERVAL_MINUTES")

        wake = next_run_time(datetime.now(timezone.utc), settings.poll_interval_minutes,
                             settings.poll_offset_minutes)
        log.info("Next round at %s", wake.isoformat())
        while not stopping and datetime.now(timezone.utc) < wake:
            time.sleep(min(5.0, max(0.0, (wake - datetime.now(timezone.utc)).total_seconds())))

    producer.flush(30)
    log.info("Producer stopped")


if __name__ == "__main__":
    main()
