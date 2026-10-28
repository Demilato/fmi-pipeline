"""Setup check: credentials, datasets, raw table. Uses only free operations (metadata + dry run).

Run: docker compose run --rm consumer python -m fmi_pipeline.bq_check
"""

from __future__ import annotations

import sys

from google.api_core.exceptions import NotFound

from fmi_pipeline.bq import ensure_raw_table, make_client, query_config
from fmi_pipeline.config import Settings


def main() -> int:
    settings = Settings()
    client = make_client(settings)
    print(f"Project: {settings.gcp_project_id}   location: {settings.bq_location}")

    ok = True
    for name in (settings.bq_raw_dataset, settings.bq_daily_dataset, settings.bq_longterm_dataset):
        try:
            ds = client.get_dataset(f"{settings.gcp_project_id}.{name}")
        except NotFound:
            print(f"  MISSING dataset {name}: create it in the BigQuery console (location {settings.bq_location})")
            ok = False
            continue
        expiry = ds.default_table_expiration_ms
        expiry_txt = f"{expiry / 86_400_000:.0f} days" if expiry else "none"
        flag = "" if ds.location.lower() == settings.bq_location.lower() else "  <-- location differs from BQ_LOCATION!"
        print(f"  dataset {name}: location={ds.location} default table expiration={expiry_txt}{flag}")
        ok = ok and not flag
    if not ok:
        return 1

    table = ensure_raw_table(client, settings)
    print(f"  raw table {table.full_table_id}: ok (partitioned by obs_time, partition filter required)")

    job = client.query(
        f"SELECT COUNT(*) FROM `{settings.raw_table_id}` "
        "WHERE obs_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
        job_config=query_config(settings, dry_run=True, use_query_cache=False),
    )
    print(f"  dry-run query ok, would process {job.total_bytes_processed} bytes (dry runs are free)")
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
