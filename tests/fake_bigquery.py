"""A tiny DuckDB-backed stand-in for google.cloud.bigquery.Client, for offline tests only.

BigQuery SQL is translated with sqlglot. It checks the logic of our queries (dedup, QC
flags, long-term merge) and the DAG wiring; it is not a full BigQuery emulator.
"""

from __future__ import annotations

import re
from types import SimpleNamespace

import duckdb
import sqlglot

TABLE_RE = re.compile(r"`([A-Za-z0-9_\-]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)`")
DUCK_TYPES = {"INT64": "BIGINT", "INTEGER": "BIGINT", "FLOAT64": "DOUBLE", "FLOAT": "DOUBLE",
              "STRING": "VARCHAR", "TIMESTAMP": "TIMESTAMP", "BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN"}


def local_name(full_id: str) -> str:
    _project, dataset, table = str(full_id).replace(":", ".").split(".")
    return f"{dataset}__{table}"


class FakeJob:
    def __init__(self, df=None, job_id="job"):
        self._df = df
        self.job_id = job_id
        self.total_bytes_processed = 0
        self.output_rows = 0 if df is None else len(df)

    def result(self, timeout=None):
        if self._df is None:
            return []
        records = [{k: (v.item() if hasattr(v, "item") else v) for k, v in r.items()}  # numpy -> Python
                   for r in self._df.to_dict("records")]
        return [SimpleNamespace(items=lambda r=r: list(r.items())) for r in records]

    def to_dataframe(self):
        return self._df


class FakeBigQuery:
    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone = 'UTC'")
        self.queries: list[str] = []
        self.byte_caps: list[int | None] = []

    def translate(self, sql: str, params: dict) -> str:
        sql = TABLE_RE.sub(lambda m: f"{m.group(2)}__{m.group(3)}", sql)
        for name, (type_, value) in params.items():
            literal = f"DATE '{value}'" if type_ == "DATE" else str(int(value)) if type_ == "INT64" else f"'{value}'"
            sql = re.sub(rf"@{name}\b", literal, sql)
        return sqlglot.transpile(sql, read="bigquery", write="duckdb")[0]

    def query(self, sql, job_config=None):
        params = {}
        destination = None
        if job_config is not None:
            for p in job_config.query_parameters or []:
                params[p.name] = (p.type_, p.value)
            destination = job_config.destination
            self.byte_caps.append(job_config.maximum_bytes_billed)
            if job_config.dry_run:
                return FakeJob()
        duck_sql = self.translate(sql, params)
        self.queries.append(duck_sql)
        if destination is not None:
            name = local_name(f"{destination.project}.{destination.dataset_id}.{destination.table_id}")
            self.con.execute(f"CREATE OR REPLACE TEMP TABLE __result AS {duck_sql}")
            self.con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM __result")
            return FakeJob()
        return FakeJob(self.con.execute(duck_sql).df())

    def get_table(self, table_id):
        n = self.con.execute(f"SELECT COUNT(*) FROM {local_name(table_id)}").fetchone()[0]
        return SimpleNamespace(num_rows=n)

    def create_table(self, table, exists_ok=False):
        cols = ", ".join(f"{f.name} {DUCK_TYPES[f.field_type]}" for f in table.schema)
        name = local_name(f"{table.project}.{table.dataset_id}.{table.table_id}")
        self.con.execute(f"CREATE TABLE {'IF NOT EXISTS ' if exists_ok else ''}{name} ({cols})")
        return table

    def list_tables(self, dataset_id):
        dataset = str(dataset_id).split(".")[-1]
        names = self.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE ?", [f"{dataset}__%"]
        ).fetchall()
        return [SimpleNamespace(table_id=n[0].split("__", 1)[1]) for n in names]

    def rows(self, sql):
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
