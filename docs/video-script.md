# Video outline (max 5 minutes)

Record the screen with the SSH tunnel open. Don't show `.env`, the key file, or the floating IP.

| Time | Show | Say |
|---|---|---|
| 0:00–0:30 | `docs/images/architecture.png` | ELT pipeline: FMI API → Kafka → BigQuery raw → Airflow transforms in BigQuery → Streamlit. Everything except BigQuery runs in Docker on a CSC cPouta VM. |
| 0:30–1:00 | Pouta web UI: instance (standard.large, Ubuntu 24.04), security group with only SSH from my IP, floating IP. Terminal: `docker compose ps` | CSC configuration: one VM, SSH-only firewall, UIs bound to localhost and reached through an SSH tunnel. |
| 1:00–1:50 | `app/fmi_pipeline/producer.py` (run_round), then `docker compose logs --tail 20 producer` | Producer reads 478 open stations from the CSV, discovers which return weather data, polls every 20 min to respect FMI's 20 000 requests/day, keeps both 10-minute steps per request, one JSON message per observation, idempotent producer with acks=all. |
| 1:50–2:20 | Terminal: `kafka-console-consumer ... --max-messages 3` and `kafka-consumer-groups --describe` (commands in README) | Messages in the topic; consumer group lag ≈ 0. |
| 2:20–2:50 | `consumer.py` (Batch, load_batch), consumer logs, BigQuery console: `fmi_raw.observations` schema + preview | Consumer batches one round into a DataFrame and appends it with one load job; offsets committed after the load (at-least-once). Raw table partitioned by day, partition filter required. |
| 2:50–3:40 | Airflow UI: DAG graph, a successful run, task log of `quality_report`. `daily_observations.sql` | Daily DAG: check raw data, deduplicate, QC flags (ranges, spikes, missing values), quality report, five long-term station tables via dynamic task mapping. Idempotent; sandbox has no DML so everything is SELECT → destination table. |
| 3:40–4:10 | BigQuery console: `fmi_daily.quality_YYYYMMDD` (duplicates_removed, completeness, flags) and `fmi_longterm.station_100971` | Results of processing and quality investigation. |
| 4:10–4:50 | Streamlit: map, recent records, daily QC tab, long-term charts | Visualisation proves the pipeline works end to end. |
| 4:50–5:00 | — | Summary + cost: BigQuery sandbox (no billing), CSC academic billing units. |
