"""Django-facing application services for Slurm analytics."""

from __future__ import absolute_import

from datetime import datetime, timedelta, timezone

import polars as pl

from slurm_analytics.analytics import analyze_jobs
from slurm_analytics.node_inventory import refresh_node_inventory
from slurm_analytics.pipeline import IngestionResult, ingest_sacct
from slurm_analytics.sacct import SacctConfig
from slurm_analytics.sacctmgr import add_hierarchy, fetch_accounts, fetch_associations
from slurm_analytics.storage import (
    connect,
    ensure_metadata,
    fetch_polars,
    get_metadata,
    replace_accounts,
    set_metadata,
    store_ingestion,
    database_path,
)

from .state import SlurmSnapshot, state
import logging

logger = logging.getLogger(__name__)
UTC = timezone.utc
DEFAULT_OVERLAP_MINUTES = 60


def _parse_datetime(value):
    if not value:
        return None
    value = str(value).strip().replace("Z", "+00:00")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def _load_stored_frame(table_name):
    """Load a complete persistent table, omitting its internal upsert key."""
    con = connect()
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if table_name not in tables:
            return None
        columns = [row[0] for row in con.execute("DESCRIBE {}".format(table_name)).fetchall()]
        columns = [column for column in columns if column != "record_key"]
        if not columns:
            return pl.DataFrame()
        quoted = ", ".join('"{}"'.format(column) for column in columns)
        return fetch_polars(con, "SELECT {} FROM {}".format(quoted, table_name))
    finally:
        con.close()


def _query_config(config, through, now):
    """Use a bounded incremental query unless the caller explicitly chose a range."""
    if config.start is not None or config.end is not None:
        return config, config.start, config.end or now.isoformat()

    if through:
        start = _parse_datetime(through) - timedelta(minutes=DEFAULT_OVERLAP_MINUTES)
        start_value = start.isoformat()
    else:
        # First run against an uninitialized store: explicitly request all
        # available history. Slurm's sacct default may otherwise start at
        # midnight today when -S is omitted.
        start_value = "1970-01-01T00:00:00+00:00"

    end_value = now.isoformat()
    return SacctConfig(
        clusters=config.clusters,
        start=start_value,
        end=end_value,
        extra_args=config.extra_args,
        timeout=config.timeout,
    ), start_value, end_value


def refresh_slurm(sacct_config=None, refresh_nodes=True):
    """Incrementally ingest Slurm jobs, then publish a snapshot of all stored history.

    The DuckDB tables are the durable source of truth. Routine refreshes query
    only the last saved accounting cursor minus an overlap window, upsert those
    records, and rebuild the in-memory snapshot from the complete database.
    """
    config = sacct_config or SacctConfig()
    now = datetime.now(UTC)

    con = connect()
    try:
        ensure_metadata(con)
        through = get_metadata(con, "initialized_through")
    finally:
        con.close()

    query_config, query_start, query_end = _query_config(config, through, now)
    incoming = ingest_sacct(query_config)

    # Upsert before advancing the cursor. If ingestion/storage fails, the old
    # cursor remains and the next run will retry the same window.
    store_ingestion(incoming)

    effective_end = _parse_datetime(query_end) or now
    con = connect()
    try:
        ensure_metadata(con)
        old_through = _parse_datetime(get_metadata(con, "initialized_through"))
        if old_through is None or effective_end > old_through:
            set_metadata(con, "initialized_through", effective_end.isoformat())
        set_metadata(con, "initialized", "true")
        set_metadata(con, "last_refresh_start", query_start or "all-history")
        set_metadata(con, "last_refresh_end", effective_end.isoformat())
    finally:
        con.close()

    # Keep account hierarchy/associations current along with the job store.
    accounts = add_hierarchy(fetch_accounts())
    associations = fetch_associations()
    replace_accounts(accounts, associations)

    # The snapshot is rebuilt from the full persistent store, not just the
    # incremental query. This preserves old jobs while adding/updating recent ones.
    all_jobs = _load_stored_frame("jobs")
    all_steps = _load_stored_frame("job_steps")
    if all_jobs is None:
        # Defensive fallback for a successful empty first query, where DuckDB
        # has not created a jobs table yet.
        all_jobs = incoming.consolidated_jobs
    if all_steps is None:
        all_steps = incoming.steps

    ingestion = IngestionResult(
        raw=incoming.raw,
        jobs=all_jobs,
        steps=all_steps,
        consolidated_jobs=all_jobs,
    )
    analytics = analyze_jobs(all_jobs)

    nodes = None
    if refresh_nodes:
        nodes = refresh_node_inventory()

    snapshot = SlurmSnapshot(
        ingestion=ingestion,
        analytics=analytics,
        nodes=nodes,
        refreshed_at=datetime.now(UTC),
    )
    state.replace(snapshot)
    return snapshot



def _build_snapshot_from_db():
    """Rebuild the snapshot from persistent data without querying Slurm."""
    db_path = database_path()

    # Do not create a new, empty DuckDB just to check readiness.
    if not db_path.is_file():
        return None

    con = connect()
    try:
        tables = {
            row[0] for row in con.execute("SHOW TABLES").fetchall()
        }

        if "ingestion_metadata" not in tables:
            return None

        if get_metadata(con, "initialized", "false") != "true":
            return None

        # Both tables are required by the current snapshot/API contract.
        if not {"jobs", "job_steps"}.issubset(tables):
            logger.warning(
                "DuckDB is initialized, but jobs or job_steps is missing."
            )
            return None
    finally:
        con.close()

    all_jobs = _load_stored_frame("jobs")
    all_steps = _load_stored_frame("job_steps")

    if all_jobs is None or all_steps is None:
        return None

    ingestion = IngestionResult(
        raw=pl.DataFrame(),
        jobs=all_jobs,
        steps=all_steps,
        consolidated_jobs=all_jobs,
    )

    snapshot = SlurmSnapshot(
        ingestion=ingestion,
        analytics=analyze_jobs(all_jobs),
        nodes=None,
        refreshed_at=datetime.now(UTC),
    )

    logger.info(
        "Rebuilt Slurm snapshot from DuckDB: %d jobs, %d steps.",
        all_jobs.height,
        all_steps.height,
    )
    return snapshot


def get_snapshot():
    """Return the current snapshot, recovering from DuckDB if needed."""
    try:
        return state.get_or_build(_build_snapshot_from_db)
    except Exception:
        logger.exception("Unable to rebuild Slurm snapshot from DuckDB.")
        return None

