"""Thin wrapper around the FMI Python API (fmi-weather-client).

``fmi_weather_client.observation_by_station_id()`` requests the last 20 minutes of
observations (10-minute time steps) and returns only the newest one. Here the same
request and parser are used, but every time step in the window is kept. That lets the
producer poll every 20 minutes, which keeps it inside FMI's open-data limit of
20 000 requests per day, while still collecting every 10-minute observation. Rows that
show up in two consecutive windows are removed later by the daily deduplication.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fmi_weather_client import http as fmi_http
from fmi_weather_client.errors import ClientError
from fmi_weather_client.models import RequestType, WeatherData
from fmi_weather_client.parsers import forecast as fmi_parser

# Column name in BigQuery -> attribute of fmi_weather_client.models.WeatherData
MEASUREMENTS: dict[str, str] = {
    "temperature": "temperature",  # degC
    "dew_point": "dew_point",  # degC
    "humidity": "humidity",  # %
    "pressure": "pressure",  # hPa, sea level
    "wind_speed": "wind_speed",  # m/s, 10 min average
    "wind_gust": "wind_gust",  # m/s
    "wind_direction": "wind_direction",  # degrees
    "cloud_cover": "cloud_cover",  # FMI TotalCloudCover, oktas 0-8 (9 = sky obscured)
    "precipitation_1h": "precipitation_amount",  # mm in the last hour
}


class StationNoData(Exception):
    """The station returned no weather observations (closed or not a weather station)."""


@dataclass(frozen=True)
class StationObservations:
    place: str
    lat: float
    lon: float
    observations: list[WeatherData]


def fetch_station_observations(fmisid: int) -> StationObservations:
    """Fetch the latest observations of one station. Raises StationNoData if it has none."""
    try:
        body = fmi_http.request_observation_by_station_id(fmisid)
    except ClientError as err:
        # The library raises ClientError(200, ...) when FMI matched no data for the station.
        if err.status_code == 200:
            raise StationNoData(err.message) from err
        raise
    parsed = fmi_parser.parse_fmi_response(body, RequestType.OBSERVATION)
    if parsed is None or not parsed.forecasts:
        raise StationNoData(f"no observations for station {fmisid}")
    return StationObservations(parsed.place, parsed.lat, parsed.lon, list(parsed.forecasts))


def _clean(value: Any) -> float | None:
    """FMI uses NaN for missing values; JSON and BigQuery need None."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) or math.isinf(number) else number


def to_record(
    fmisid: int,
    station: StationObservations,
    weather: WeatherData,
    fetched_at: datetime,
    round_id: str,
    fallback_name: str = "",
) -> dict[str, Any]:
    """Flatten one observation into the message / raw-table schema."""
    record: dict[str, Any] = {
        "fmisid": fmisid,
        "station_name": station.place or fallback_name,
        "latitude": _clean(station.lat),
        "longitude": _clean(station.lon),
        "obs_time": weather.time.isoformat(),
    }
    for column, attribute in MEASUREMENTS.items():
        value = getattr(weather, attribute, None)
        record[column] = _clean(getattr(value, "value", None))
    record["fetched_at"] = fetched_at.isoformat()
    record["round_id"] = round_id
    return record
