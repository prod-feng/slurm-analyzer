"""Persistent DuckDB storage for normalized Slurm accounting data."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import duckdb
import polars as pl

DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "slurm.duckdb"


def database_path(path=None):
    return Path(path or os.environ.get("SLURM_DUCKDB_PATH") or DEFAULT_DB_PATH)


def connect(path=None):
    path = database_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def _empty_like(df):
    return df.head(0)


def fetch_polars(con, query, parameters=None):
    if parameters is None:
        result = con.execute(query)
    else:
        result = con.execute(query, parameters)

    return result.pl()

def fetch_polars_old(con, sql, params=None):
    """Execute SQL and return a Polars DataFrame without DuckDB's Arrow bridge.

    DuckDB's ``.pl()`` helper may import PyArrow depending on the installed
    DuckDB version.  The analytics package does not otherwise require PyArrow,
    so build the frame directly from DB-API rows instead.
    """
    result = con.execute(sql, params or [])
    columns = [desc[0] for desc in result.description or []]
    rows = result.fetchall()
    if not columns:
        return pl.DataFrame()
    if not rows:
        return pl.DataFrame({name: pl.Series(name, [], dtype=pl.Null) for name in columns})
    return pl.DataFrame(rows, schema=columns, orient="row")


def _ensure_table(con, name, df):
    if df.is_empty() and name not in {r[0] for r in con.execute("SHOW TABLES").fetchall()}:
        return False
    con.register("_frame", df)
    con.execute("CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM _frame LIMIT 0".format(name))
    con.unregister("_frame")
    return True


def _add_missing_columns(con, name, df):
    existing = {row[0]: row[1] for row in con.execute("DESCRIBE {}".format(name)).fetchall()}
    con.register("_frame", df)
    incoming = {row[0]: row[1] for row in con.execute("DESCRIBE _frame").fetchall()}
    con.unregister("_frame")
    for col, dtype in incoming.items():
        if col not in existing:
            con.execute('ALTER TABLE {} ADD COLUMN "{}" {}'.format(name, col, dtype))


def _record_keys(df, step=False):
    if df.is_empty():
        return df.with_columns(pl.Series("record_key", [], dtype=pl.String))
    identity = [c for c in ("cluster", "jobidraw", "jobid", "canonical_job_id", "step_id") if c in df.columns]
    if not identity:
        identity = [df.columns[0]]
    return df.with_columns(
        pl.struct(identity).map_elements(
            lambda row: hashlib.sha256("|".join("" if row.get(c) is None else str(row.get(c)) for c in identity).encode()).hexdigest(),
            return_dtype=pl.String,
        ).alias("record_key")
    )


def _upsert(con, name, df):
    if df.is_empty():
        return 0
    df = _record_keys(df, step=(name == "job_steps"))
    _ensure_table(con, name, df)
    _add_missing_columns(con, name, df)
    con.register("_incoming", df)
    con.execute("DELETE FROM {} WHERE record_key IN (SELECT record_key FROM _incoming)".format(name))
    columns = [r[0] for r in con.execute("DESCRIBE _incoming").fetchall()]
    quoted = ", ".join('"{}"'.format(c) for c in columns)
    con.execute("INSERT INTO {} ({}) SELECT {} FROM _incoming".format(name, quoted, quoted))
    con.unregister("_incoming")
    return df.height


def ensure_metadata(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            account_name VARCHAR PRIMARY KEY,
            parent_name VARCHAR,
            description VARCHAR,
            level INTEGER,
            path VARCHAR,
            refreshed_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS associations (
            association_key VARCHAR PRIMARY KEY,
            cluster VARCHAR,
            account_name VARCHAR,
            user_name VARCHAR,
            refreshed_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS account_history (
            snapshot_time TIMESTAMP NOT NULL,
            account_name VARCHAR NOT NULL,
            parent_name VARCHAR,
            description VARCHAR,
            level INTEGER,
            path VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS association_history (
            snapshot_time TIMESTAMP NOT NULL,
            association_key VARCHAR NOT NULL,
            cluster VARCHAR,
            account_name VARCHAR,
            user_name VARCHAR
        )
    """)

    # Migrate databases created by the earlier schema.  A composite PRIMARY KEY
    # implicitly makes user_name NOT NULL in DuckDB, but sacctmgr legitimately
    # emits account-level associations with no User.  Keep user_name nullable
    # and use a deterministic surrogate key instead.
    cols = {row[0] for row in con.execute("DESCRIBE associations").fetchall()}
    if "association_key" not in cols:
        con.execute("""
            CREATE TABLE associations_new (
                association_key VARCHAR PRIMARY KEY,
                cluster VARCHAR,
                account_name VARCHAR,
                user_name VARCHAR,
                refreshed_at TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO associations_new
                (association_key, cluster, account_name, user_name, refreshed_at)
            SELECT
                md5(coalesce(cluster, '') || '|' || coalesce(account_name, '') || '|' || coalesce(user_name, '')),
                cluster, account_name, user_name, refreshed_at
            FROM associations
        """)
        con.execute("DROP TABLE associations")
        con.execute("ALTER TABLE associations_new RENAME TO associations")


def set_metadata(con, key, value):
    con.execute("DELETE FROM ingestion_metadata WHERE key = ?", [key])
    con.execute("INSERT INTO ingestion_metadata VALUES (?, ?, current_timestamp)", [key, str(value)])


def get_metadata(con, key, default=None):
    row = con.execute("SELECT value FROM ingestion_metadata WHERE key = ?", [key]).fetchone()
    return default if row is None else row[0]


def store_ingestion(ingestion, path=None):
    con = connect(path)
    try:
        ensure_metadata(con)
        jobs = ingestion.consolidated_jobs
        steps = ingestion.steps
        job_count = _upsert(con, "jobs", jobs)
        step_count = _upsert(con, "job_steps", steps)
        return job_count, step_count
    finally:
        con.close()


def replace_accounts(accounts, associations, path=None):
    con = connect(path)
    try:
        ensure_metadata(con)
        con.execute("DELETE FROM accounts")
        con.execute("DELETE FROM associations")
        if accounts:
            con.executemany(
                "INSERT INTO accounts(account_name,parent_name,description,level,path,refreshed_at) VALUES (?, ?, ?, ?, ?, current_timestamp)",
                accounts,
            )
            con.executemany(
                "INSERT INTO account_history(snapshot_time,account_name,parent_name,description,level,path) VALUES (current_timestamp, ?, ?, ?, ?, ?)",
                accounts,
            )
        if associations:
            values = [
                (
                    __import__("hashlib").md5(
                        (str(cluster or "") + "|" + str(account or "") + "|" + str(user or "")).encode()
                    ).hexdigest(),
                    cluster, account, user,
                )
                for cluster, account, user in associations
            ]
            con.executemany(
                "INSERT INTO associations(association_key,cluster,account_name,user_name,refreshed_at) VALUES (?, ?, ?, ?, current_timestamp)",
                values,
            )
            con.executemany(
                "INSERT INTO association_history(snapshot_time,association_key,cluster,account_name,user_name) VALUES (current_timestamp, ?, ?, ?, ?)",
                values,
            )
    finally:
        con.close()


def query_df(sql, params=None, path=None):
    con = connect(path)
    try:
        return fetch_polars(con, sql, params)
    finally:
        con.close()
