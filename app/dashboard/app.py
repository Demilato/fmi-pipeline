"""Streamlit dashboard: shows that the pipeline works end to end.

Run: streamlit run dashboard/app.py   (inside the app container, see docker-compose.yml)
All queries are cached for 5 minutes and capped with maximum_bytes_billed.
"""

from __future__ import annotations

import re

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st
from google.cloud import bigquery

from fmi_pipeline.bq import make_client, query_config
from fmi_pipeline.config import Settings

LOCAL_TZ = "Europe/Helsinki"
settings = Settings()
P = settings.gcp_project_id
RAW = f"`{settings.raw_table_id}`"
STATIONS = [int(s) for s in settings.longterm_stations]
STATION_LIST = ", ".join(str(s) for s in STATIONS)

st.set_page_config(page_title="FMI weather pipeline", layout="wide")


@st.cache_resource
def client() -> bigquery.Client:
    return make_client(settings)


@st.cache_data(ttl=300, show_spinner="Querying BigQuery...")
def run_query(sql: str) -> pd.DataFrame:
    return client().query(sql, job_config=query_config(settings)).to_dataframe()


@st.cache_data(ttl=300)
def list_tables(dataset: str) -> list[str]:
    return sorted(t.table_id for t in client().list_tables(f"{P}.{dataset}"))


def local_time(df: pd.DataFrame, column: str = "obs_time") -> pd.DataFrame:
    if not df.empty:
        df[column] = pd.to_datetime(df[column], utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
    return df


def temperature_color(t: float | None) -> list[int]:
    if t is None or pd.isna(t):
        return [150, 150, 150, 160]
    x = min(max((t + 25) / 50, 0), 1)  # -25 degC -> blue, +25 degC -> red
    return [int(255 * x), int(80 + 60 * (1 - abs(x - 0.5) * 2)), int(255 * (1 - x)), 200]


st.title("FMI weather data pipeline")
st.caption(f"FMI API → Kafka → BigQuery (`{P}`) → Airflow → Streamlit. Times shown in {LOCAL_TZ} time.")

tab_latest, tab_daily, tab_longterm = st.tabs(["Latest observations", "Daily tables", "Long-term"])

# --- Latest observations (raw table) -----------------------------------------------------------
with tab_latest:
    latest = run_query(f"""
        SELECT * EXCEPT (rn) FROM (
          SELECT fmisid, station_name, latitude, longitude, obs_time, temperature, humidity,
                 wind_speed, pressure, ingested_at,
                 ROW_NUMBER() OVER (PARTITION BY fmisid ORDER BY obs_time DESC, fetched_at DESC) AS rn
          FROM {RAW}
          WHERE obs_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
        ) WHERE rn = 1""")
    if latest.empty:
        st.info("No observations from the last 3 hours yet. Check that the producer and consumer are running.")
    else:
        latest = local_time(latest)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Stations reporting (3 h)", len(latest))
        c2.metric("Newest observation", latest["obs_time"].max().strftime("%d.%m. %H:%M"))
        c3.metric("Median temperature", f"{latest['temperature'].median():.1f} °C")
        c4.metric("Last load to BigQuery",
                  pd.to_datetime(latest["ingested_at"], utc=True).max().tz_convert(LOCAL_TZ).strftime("%H:%M"))

        map_df = latest.dropna(subset=["latitude", "longitude"]).copy()
        map_df["color"] = map_df["temperature"].map(temperature_color)
        map_df["label"] = map_df["temperature"].map(lambda t: "–" if pd.isna(t) else f"{t:.1f} °C")
        map_df["time"] = map_df["obs_time"].dt.strftime("%H:%M")
        st.pydeck_chart(pdk.Deck(
            initial_view_state=pdk.ViewState(latitude=64.8, longitude=26.0, zoom=4.2),
            layers=[pdk.Layer("ScatterplotLayer", data=map_df, get_position="[longitude, latitude]",
                              get_fill_color="color", get_radius=9000, pickable=True)],
            tooltip={"text": "{station_name}\n{label} at {time}"},
        ), height=560)
        st.caption("Colour: temperature from blue (−25 °C) to red (+25 °C). Grey = no temperature.")

        st.subheader("Most recent records of the selected stations")
        recent = local_time(run_query(f"""
            SELECT station_name, fmisid, obs_time, temperature, humidity, dew_point, pressure,
                   wind_speed, wind_gust, precipitation_1h
            FROM {RAW}
            WHERE obs_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
              AND fmisid IN ({STATION_LIST})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY fmisid, obs_time ORDER BY fetched_at DESC) = 1
            ORDER BY station_name, obs_time DESC"""))
        st.dataframe(recent, hide_index=True, width="stretch")

# --- Daily tables -----------------------------------------------------------------------------
with tab_daily:
    days = sorted((m.group(1) for t in list_tables(settings.bq_daily_dataset)
                   if (m := re.fullmatch(r"quality_(\d{8})", t))), reverse=True)
    if not days:
        st.info("No daily tables yet. Trigger the `fmi_daily_processing` DAG in Airflow.")
    else:
        day = st.selectbox("Day (UTC)", days, format_func=lambda d: f"{d[:4]}-{d[4:6]}-{d[6:]}")
        quality = run_query(f"SELECT * FROM `{P}.{settings.bq_daily_dataset}.quality_{day}` ORDER BY station_name")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Stations", len(quality))
        c2.metric("Rows after dedup", int(quality["rows_after_dedup"].sum()))
        c3.metric("Duplicates removed", int(quality["duplicates_removed"].fillna(0).sum()))
        c4.metric("Rows flagged by QC", int(quality["rows_flagged"].sum()))
        st.subheader("Data-quality report")
        st.dataframe(quality.drop(columns=["processed_at"]), hide_index=True, width="stretch")

        st.subheader("Observations of one station")
        names = dict(zip(quality["fmisid"].astype(int), quality["station_name"], strict=True))
        options = sorted(names, key=lambda f: names[f] or "")
        default = next((i for i, f in enumerate(options) if f in STATIONS), 0)
        pick = st.selectbox("Station", options, index=default, format_func=lambda f: f"{names[f]} ({f})")
        obs = local_time(run_query(f"""
            SELECT obs_time, temperature, humidity, qc_ok
            FROM `{P}.{settings.bq_daily_dataset}.obs_{day}`
            WHERE fmisid = {int(pick)} ORDER BY obs_time"""))
        st.altair_chart(
            alt.Chart(obs).mark_line(point=True).encode(
                x=alt.X("obs_time:T", title="Local time"),
                y=alt.Y("temperature:Q", title="Temperature (°C)", scale=alt.Scale(zero=False)),
                color=alt.Color("qc_ok:N", title="QC ok"),
                tooltip=["obs_time:T", "temperature:Q", "humidity:Q", "qc_ok:N"],
            ),
            width="stretch",
        )

# --- Long-term tables -------------------------------------------------------------------------
with tab_longterm:
    existing = set(list_tables(settings.bq_longterm_dataset))
    tables = [f"station_{s}" for s in STATIONS if f"station_{s}" in existing]
    if not tables:
        st.info("No long-term tables yet. They are created by the daily Airflow DAG.")
    else:
        union = "\nUNION ALL\n".join(
            f"SELECT obs_time, station_name, fmisid, temperature, humidity, qc_ok "
            f"FROM `{P}.{settings.bq_longterm_dataset}.{t}`" for t in tables)
        hist = local_time(run_query(union))
        only_ok = st.toggle("Hide rows flagged by quality checks", value=True)
        if only_ok:
            hist = hist[hist["qc_ok"].fillna(True)]
        st.caption(f"{len(hist)} rows from {hist['station_name'].nunique()} stations, "
                   f"{hist['obs_time'].min():%d.%m.%Y} – {hist['obs_time'].max():%d.%m.%Y}"
                   if not hist.empty else "No rows yet.")
        for column, title in (("temperature", "Temperature (°C)"), ("humidity", "Relative humidity (%)")):
            st.subheader(title)
            st.altair_chart(
                alt.Chart(hist).mark_line().encode(
                    x=alt.X("obs_time:T", title="Local time"),
                    y=alt.Y(f"{column}:Q", title=title, scale=alt.Scale(zero=False)),
                    color=alt.Color("station_name:N", title="Station"),
                    tooltip=["station_name:N", "obs_time:T", f"{column}:Q"],
                ).interactive(),
                width="stretch",
            )
