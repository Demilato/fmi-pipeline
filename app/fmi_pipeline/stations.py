"""Load the FMI station list (FMI_stations.csv from the course material)."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Station:
    fmisid: int
    name: str
    lat: float
    lon: float


def _decode(raw: bytes) -> str:
    # The course CSV is ISO-8859-1 (Latin-1). Accept UTF-8 too in case it gets re-saved.
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def load_stations(path: str | Path, only_open: bool = True) -> list[Station]:
    """Return stations from the CSV (columns: Name, FMISID, lat, lon, start, finish).

    Stations with a ``finish`` year have been closed, so by default only stations without
    one are returned. The CSV does not say which open stations measure weather (some are
    air-quality, sea-level or precipitation stations), so the producer discovers that at
    runtime: stations that return no weather data are marked inactive.
    """
    text = _decode(Path(path).read_bytes())
    reader = csv.DictReader(io.StringIO(text))
    stations: dict[int, Station] = {}
    for row in reader:
        try:
            fmisid = int(row["FMISID"])
        except (KeyError, TypeError, ValueError):
            continue
        if only_open and (row.get("finish") or "").strip():
            continue
        if fmisid in stations:  # the CSV contains a few duplicate rows
            continue
        stations[fmisid] = Station(
            fmisid=fmisid,
            name=(row.get("Name") or "").strip(),
            lat=float(row["lat"]) if row.get("lat") else float("nan"),
            lon=float(row["lon"]) if row.get("lon") else float("nan"),
        )
    return list(stations.values())
