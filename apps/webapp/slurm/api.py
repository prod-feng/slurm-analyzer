"""HTTP API for the current Slurm analytics snapshot."""

from __future__ import absolute_import

from django.http import JsonResponse
import polars as pl
from django.views.decorators.http import require_GET, require_POST

from slurm_analytics.sacct import SacctConfig
from slurm_analytics.squeue import fetch_squeue
from slurm_analytics.analytics import time_series
from slurm_analytics.storage import query_df

from .services import get_snapshot, refresh_slurm


def dataframe_to_records(df, limit=None):
    if df is None or df.is_empty():
        return []
    if limit is not None:
        df = df.head(limit)
    return df.to_dicts()


def _not_ready():
    return JsonResponse(
        {"ready": False, "message": "Slurm data has not been loaded yet."},
        status=503,
    )


def _snapshot_or_response():
    snapshot = get_snapshot()
    if snapshot is None:
        return None, _not_ready()
    return snapshot, None


@require_GET
def api_metadata(request):
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    jobs = snapshot.ingestion.consolidated_jobs
    return JsonResponse({
        "ready": True,
        "refreshed_at": snapshot.refreshed_at.isoformat(),
        "job_count": jobs.height,
        "step_count": snapshot.ingestion.steps.height,
        "node_count": snapshot.nodes.height if snapshot.nodes is not None else None,
    })


@require_GET
def api_summary(request):
    """Return date-range job outcomes/resource totals and live queue counts."""
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    start = request.GET.get("start")
    end = request.GET.get("end") or now.isoformat()
    if not start:
        start = (now - timedelta(days=7)).isoformat()

    # Outcome counts use end time, while CPU/GPU hours use allocation start
    # time. Running and pending counts are queried live from squeue below.
    # The persistent jobs table contains one consolidated row per job, not steps.
    try:
        historical = query_df(
            """
            WITH job_rows AS (
                SELECT *,
                    COALESCE(
                        TRY_CAST(submit_time AS TIMESTAMPTZ),
                        TRY_CAST(start_time AS TIMESTAMPTZ),
                        TRY_CAST(end_time AS TIMESTAMPTZ)
                    ) AS submitted_at,
                    TRY_CAST(start_time AS TIMESTAMPTZ) AS started_at,
                    TRY_CAST(end_time AS TIMESTAMPTZ) AS ended_at,
                    CASE
                        WHEN upper(COALESCE(state, '')) LIKE 'COMPLETED%' THEN 'completed'
                        WHEN upper(COALESCE(state, '')) LIKE 'CANCELLED%' THEN 'cancelled'
                        WHEN upper(COALESCE(state, '')) LIKE 'FAILED%'
                          OR upper(COALESCE(state, '')) LIKE 'TIMEOUT%'
                          OR upper(COALESCE(state, '')) LIKE 'NODE_FAIL%'
                          OR upper(COALESCE(state, '')) LIKE 'OUT_OF_MEMORY%'
                          OR upper(COALESCE(state, '')) LIKE 'PREEMPTED%'
                          OR upper(COALESCE(state, '')) LIKE 'BOOT_FAIL%'
                          OR upper(COALESCE(state, '')) LIKE 'DEADLINE%'
                          OR upper(COALESCE(state, '')) LIKE 'REVOKED%'
                          OR upper(COALESCE(state, '')) LIKE 'SPECIAL_EXIT%'
                            THEN 'failed'
                        WHEN upper(COALESCE(state, '')) LIKE 'RUNNING%' THEN 'running'
                        WHEN upper(COALESCE(state, '')) LIKE 'PENDING%' THEN 'pending'
                        ELSE 'other'
                    END AS summary_category
                FROM jobs
            )
            SELECT
                COUNT(*) FILTER (
                    WHERE submitted_at >= TRY_CAST(? AS TIMESTAMPTZ)
                      AND submitted_at < TRY_CAST(? AS TIMESTAMPTZ)
                ) AS job_count,
                COUNT(*) FILTER (
                    WHERE ended_at >= TRY_CAST(? AS TIMESTAMPTZ)
                      AND ended_at < TRY_CAST(? AS TIMESTAMPTZ)
                      AND summary_category = 'completed'
                ) AS completed_count,
                COUNT(*) FILTER (
                    WHERE ended_at >= TRY_CAST(? AS TIMESTAMPTZ)
                      AND ended_at < TRY_CAST(? AS TIMESTAMPTZ)
                      AND summary_category = 'cancelled'
                ) AS cancelled_count,
                COUNT(*) FILTER (
                    WHERE ended_at >= TRY_CAST(? AS TIMESTAMPTZ)
                      AND ended_at < TRY_CAST(? AS TIMESTAMPTZ)
                      AND summary_category = 'failed'
                ) AS failed_count,
                COUNT(*) FILTER (
                    WHERE ended_at >= TRY_CAST(? AS TIMESTAMPTZ)
                      AND ended_at < TRY_CAST(? AS TIMESTAMPTZ)
                      AND summary_category = 'other'
                ) AS other_count,
                COALESCE(SUM(
                    CASE WHEN started_at >= TRY_CAST(? AS TIMESTAMPTZ)
                               AND started_at < TRY_CAST(? AS TIMESTAMPTZ)
                         THEN COALESCE(TRY_CAST(ncpus AS DOUBLE), 0)
                              * COALESCE(TRY_CAST(elapsed_seconds AS DOUBLE), 0)
                         ELSE 0 END
                ), 0) / 3600.0 AS cpu_hours,
                COALESCE(SUM(
                    CASE WHEN started_at >= TRY_CAST(? AS TIMESTAMPTZ)
                               AND started_at < TRY_CAST(? AS TIMESTAMPTZ)
                         THEN COALESCE(TRY_CAST(gpu_count AS DOUBLE), 0)
                              * COALESCE(TRY_CAST(elapsed_seconds AS DOUBLE), 0)
                         ELSE 0 END
                ), 0) / 3600.0 AS gpu_hours
            FROM job_rows
            """,
            [start, end] * 5 + [start, end, start, end],
        )
        row = historical.to_dicts()[0] if not historical.is_empty() else {}
    except Exception as exc:
        return JsonResponse({"error": "Unable to calculate period summary: {}".format(exc)}, status=500)

    # Peak values are calculated over terminal jobs started in the selected range.
    # Null telemetry remains null so missing measurements are never shown as zero.
    try:
        from slurm_analytics.storage import connect
        schema_con = connect()
        try:
            available_job_columns = {r[0].lower() for r in schema_con.execute("DESCRIBE jobs").fetchall()}
        finally:
            schema_con.close()
        ntasks_expr = "TRY_CAST(tj.ntasks AS DOUBLE)" if "ntasks" in available_job_columns else "NULL::DOUBLE"
        reqmem_bytes_expr = """TRY_CAST(regexp_extract(upper(CAST(tj.reqmem AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)
            * CASE regexp_extract(upper(CAST(tj.reqmem AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)
                WHEN 'K' THEN 1024.0 WHEN 'M' THEN 1048576.0 WHEN 'G' THEN 1073741824.0
                WHEN 'T' THEN 1099511627776.0 WHEN 'P' THEN 1125899906842624.0
                WHEN 'E' THEN 1152921504606846976.0 ELSE 1.0 END
            * CASE lower(regexp_extract(upper(CAST(tj.reqmem AS VARCHAR)), '([CN])$', 1))
                WHEN 'C' THEN COALESCE(TRY_CAST(tj.ncpus AS DOUBLE), 0)
                ELSE COALESCE(TRY_CAST(tj.nnodes AS DOUBLE), 0) END"""
        if "allocated_memory_bytes" in available_job_columns:
            requested_memory_expr = "COALESCE(MAX(TRY_CAST(tj.allocated_memory_bytes AS DOUBLE)), MAX({})) / 1073741824.0 AS max_requested_memory_gib".format(reqmem_bytes_expr)
        else:
            requested_memory_expr = "MAX({}) / 1073741824.0 AS max_requested_memory_gib".format(reqmem_bytes_expr)
        peaks = query_df(
            """
            WITH terminal_jobs AS (
                SELECT * FROM jobs
                WHERE TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                  AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                  AND lower(COALESCE(state_category, '')) NOT IN ('running', 'pending')
            ), step_rss AS (
                SELECT js.canonical_job_id,
                       MAX(
                           TRY_CAST(regexp_extract(upper(CAST(js.maxrss AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)
                           * CASE regexp_extract(upper(CAST(js.maxrss AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)
                               WHEN 'K' THEN 1024.0 WHEN 'M' THEN 1048576.0 WHEN 'G' THEN 1073741824.0
                               WHEN 'T' THEN 1099511627776.0 WHEN 'P' THEN 1125899906842624.0
                               WHEN 'E' THEN 1152921504606846976.0 ELSE 1.0 END
                       ) AS max_rss_bytes
                FROM job_steps js JOIN terminal_jobs tj ON tj.canonical_job_id = js.canonical_job_id
                WHERE js.maxrss IS NOT NULL
                GROUP BY js.canonical_job_id
            )
            SELECT
                MAX(TRY_CAST(tj.ncpus AS DOUBLE)) AS max_cpus_per_job,
                MAX(TRY_CAST(tj.gpu_count AS DOUBLE)) AS max_gpus_per_job,
                {requested_memory_expr},
                MAX(TRY_CAST(tj.nnodes AS DOUBLE)) AS max_nodes_per_job,
                AVG(TRY_CAST(tj.nnodes AS DOUBLE)) AS avg_nodes_per_job,
                MAX({ntasks_expr}) AS max_tasks_per_job,
                AVG({ntasks_expr}) AS avg_tasks_per_job,
                MAX(COALESCE(sr.max_rss_bytes,
                    TRY_CAST(regexp_extract(upper(CAST(tj.maxrss AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)
                    * CASE regexp_extract(upper(CAST(tj.maxrss AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)
                        WHEN 'K' THEN 1024.0 WHEN 'M' THEN 1048576.0 WHEN 'G' THEN 1073741824.0
                        WHEN 'T' THEN 1099511627776.0 WHEN 'P' THEN 1125899906842624.0
                        WHEN 'E' THEN 1152921504606846976.0 ELSE 1.0 END
                )) / 1073741824.0 AS max_recorded_rss_gib,
                MAX(TRY_CAST(tj.time_limit_seconds AS DOUBLE)) / 3600.0 AS max_time_limit_hours,
                MAX(TRY_CAST(tj.elapsed_seconds AS DOUBLE)) / 3600.0 AS max_elapsed_runtime_hours,
                MAX(TRY_CAST(tj.gpu_mem_max_bytes AS DOUBLE)) / 1073741824.0 AS max_recorded_gpu_memory_gib
            FROM terminal_jobs tj LEFT JOIN step_rss sr ON sr.canonical_job_id = tj.canonical_job_id
            """.format(ntasks_expr=ntasks_expr, requested_memory_expr=requested_memory_expr),
            [start, end],
        )
        peak_row = peaks.to_dicts()[0] if not peaks.is_empty() else {}
        # Recompute peak MaxRSS from raw step values with a tolerant parser;
        # this also covers DuckDB builds whose regexp engine does not parse
        # decimal/unit suffixes consistently.
        import re
        rss_rows = query_df(
            """
            SELECT js.maxrss
            FROM job_steps js JOIN jobs j ON j.canonical_job_id = js.canonical_job_id
            WHERE TRY_CAST(j.start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
              AND TRY_CAST(j.start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
              AND lower(COALESCE(j.state_category, '')) NOT IN ('running', 'pending')
              AND js.maxrss IS NOT NULL
            """,
            [start, end],
        )
        rss_bytes = []
        for item in rss_rows.get_column("maxrss").to_list() if not rss_rows.is_empty() else []:
            match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*[cCnN]?\s*$", str(item), re.I)
            if match:
                powers = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
                rss_bytes.append(float(match.group(1)) * (1024.0 ** powers[match.group(2).upper()]))
        if rss_bytes:
            peak_row["max_recorded_rss_gib"] = max(rss_bytes) / (1024.0 ** 3)
    except Exception:
        peak_row = {}

    # Prefer sacct MaxDiskRead/MaxDiskWrite telemetry. Older databases may not
    # have these columns until a refresh ingests the expanded sacct field list.
    try:
        import re
        con = None
        from slurm_analytics.storage import connect
        con = connect()
        job_columns = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
        con.close()
        if {"maxdiskread", "maxdiskwrite"} & job_columns:
            read_col = "maxdiskread" if "maxdiskread" in job_columns else "NULL AS maxdiskread"
            write_col = "maxdiskwrite" if "maxdiskwrite" in job_columns else "NULL AS maxdiskwrite"
            io_rows = query_df(
                """
                SELECT {read_col}, {write_col} FROM jobs
                WHERE TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                  AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                  AND lower(COALESCE(state_category, '')) NOT IN ('running', 'pending')
                """.format(read_col=read_col, write_col=write_col),
                [start, end],
            )
            def disk_bytes(value):
                if value is None:
                    return None
                match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*$", str(value), re.I)
                if not match:
                    return None
                power = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}[match.group(2).upper()]
                return float(match.group(1)) * (1024.0 ** power)
            read_values, write_values, io_values = [], [], []
            for item in io_rows.to_dicts() if not io_rows.is_empty() else []:
                read_bytes = disk_bytes(item.get("maxdiskread"))
                write_bytes = disk_bytes(item.get("maxdiskwrite"))
                if read_bytes is not None:
                    read_values.append(read_bytes / (1024.0 ** 3))
                if write_bytes is not None:
                    write_values.append(write_bytes / (1024.0 ** 3))
                if read_bytes is not None or write_bytes is not None:
                    io_values.append(((read_bytes or 0.0) + (write_bytes or 0.0)) / (1024.0 ** 3))
            peak_row["max_disk_read_gib"] = max(read_values) if read_values else None
            peak_row["max_disk_write_gib"] = max(write_values) if write_values else None
            peak_row["max_disk_io_gib"] = max(io_values) if io_values else None
        else:
            peak_row["max_disk_read_gib"] = None
            peak_row["max_disk_write_gib"] = None
            peak_row["max_disk_io_gib"] = None
    except Exception:
        peak_row["max_disk_read_gib"] = None
        peak_row["max_disk_write_gib"] = None
        peak_row["max_disk_io_gib"] = None

    # Running and pending are current queue states, sourced live from squeue.
    try:
        queue = fetch_squeue()
        states = [str(value or "").strip().upper() for value in queue.get_column("state").to_list()]
        running_count = sum(value.startswith("RUNNING") for value in states)
        pending_count = sum(value.startswith("PENDING") for value in states)
    except Exception:
        # Never report a failed live query as zero jobs.
        running_count = None
        pending_count = None

    return JsonResponse({
        "job_count": int(row.get("job_count") or 0),
        "completed_count": int(row.get("completed_count") or 0),
        "cancelled_count": int(row.get("cancelled_count") or 0),
        "failed_count": int(row.get("failed_count") or 0),
        "other_count": int(row.get("other_count") or 0),
        "running_count": running_count,
        "pending_count": pending_count,
        "cpu_hours": float(row.get("cpu_hours") or 0),
        "gpu_hours": float(row.get("gpu_hours") or 0),
        "max_cpus_per_job": peak_row.get("max_cpus_per_job"),
        "max_gpus_per_job": peak_row.get("max_gpus_per_job"),
        "max_requested_memory_gib": peak_row.get("max_requested_memory_gib"),
        "max_nodes_per_job": peak_row.get("max_nodes_per_job"),
        "avg_nodes_per_job": peak_row.get("avg_nodes_per_job"),
        "max_tasks_per_job": peak_row.get("max_tasks_per_job"),
        "avg_tasks_per_job": peak_row.get("avg_tasks_per_job"),
        "max_recorded_rss_gib": peak_row.get("max_recorded_rss_gib"),
        "max_time_limit_hours": peak_row.get("max_time_limit_hours"),
        "max_elapsed_runtime_hours": peak_row.get("max_elapsed_runtime_hours"),
        "max_recorded_gpu_memory_gib": peak_row.get("max_recorded_gpu_memory_gib"),
        "max_disk_io_gib": peak_row.get("max_disk_io_gib"),
        "max_disk_read_gib": peak_row.get("max_disk_read_gib"),
        "max_disk_write_gib": peak_row.get("max_disk_write_gib"),
        "start": start,
        "end": end,
    })

@require_GET
def api_users(request):
    snapshot, response = _snapshot_or_response()
    if response:
        return response
    return JsonResponse(dataframe_to_records(snapshot.analytics["users"]), safe=False)


@require_GET
def api_partitions(request):
    snapshot, response = _snapshot_or_response()
    if response:
        return response
    return JsonResponse(dataframe_to_records(snapshot.analytics["partitions"]), safe=False)


@require_GET
def api_partition_usage(request):
    """Aggregate terminal-job CPU/GPU usage by partition over a date range."""
    snapshot, response = _snapshot_or_response()
    if response:
        return response
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    start = request.GET.get("start") or (now - timedelta(days=7)).isoformat()
    end = request.GET.get("end") or now.isoformat()
    try:
        con = None
        from slurm_analytics.storage import connect
        con = connect()
        cols = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
        con.close()
        ntasks_expr = "TRY_CAST(ntasks AS DOUBLE)" if "ntasks" in cols else "NULL::DOUBLE"
        allocated_memory_expr = "TRY_CAST(allocated_memory_bytes AS DOUBLE)" if "allocated_memory_bytes" in cols else "NULL::DOUBLE"
        def disk_gib_expr(column):
            if column not in cols:
                return "NULL::DOUBLE"
            value = "TRY_CAST(regexp_extract(upper(CAST({0} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)".format(column)
            unit = "regexp_extract(upper(CAST({0} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)".format(column)
            return "(({value}) * CASE {unit} WHEN 'K' THEN 1.0/1048576.0 WHEN 'M' THEN 1.0/1024.0 WHEN 'G' THEN 1.0 WHEN 'T' THEN 1024.0 WHEN 'P' THEN 1048576.0 WHEN 'E' THEN 1073741824.0 ELSE 1.0/1073741824.0 END)".format(value=value, unit=unit)
        disk_read_expr, disk_write_expr = disk_gib_expr("maxdiskread"), disk_gib_expr("maxdiskwrite")
        query = query_df(
            """
            WITH base_jobs AS (
                SELECT *,
                       COALESCE(NULLIF(TRIM(partition), ''), '(Unknown partition)') AS partition_list
                FROM jobs
                WHERE TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                  AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                  AND lower(COALESCE(state_category, '')) NOT IN ('running', 'pending')
            ), split_jobs AS (
                SELECT b.*,
                       TRIM(p.partition_name) AS partition_name,
                       GREATEST(array_length(string_split(b.partition_list, ',')), 1) AS partition_choices
                FROM base_jobs b
                CROSS JOIN UNNEST(string_split(b.partition_list, ',')) AS p(partition_name)
            )
            SELECT partition_name AS partition,
                   COUNT(DISTINCT canonical_job_id) AS jobs,
                   SUM((COALESCE(TRY_CAST(ncpus AS DOUBLE), 0) * COALESCE(TRY_CAST(elapsed_seconds AS DOUBLE), 0) / 3600.0) / partition_choices) AS cpu_hours,
                   SUM((COALESCE(TRY_CAST(gpu_count AS DOUBLE), 0) * COALESCE(TRY_CAST(elapsed_seconds AS DOUBLE), 0) / 3600.0) / partition_choices) AS gpu_hours,
                   SUM((COALESCE({allocated_memory_expr}, 0) / 1073741824.0 * COALESCE(TRY_CAST(elapsed_seconds AS DOUBLE), 0) / 3600.0) / partition_choices) AS memory_gb_hours,
                   SUM(COALESCE({ntasks}, 0) / partition_choices) AS total_tasks,
                   SUM(COALESCE({disk_read_expr}, 0) / partition_choices) AS disk_read_gib,
                   SUM(COALESCE({disk_write_expr}, 0) / partition_choices) AS disk_write_gib,
                   AVG(TRY_CAST(nnodes AS DOUBLE)) AS avg_nodes_per_job,
                   MAX(TRY_CAST(nnodes AS DOUBLE)) AS max_nodes_per_job,
                   AVG({ntasks}) AS avg_tasks_per_job,
                   MAX({ntasks}) AS max_tasks_per_job
            FROM split_jobs
            WHERE partition_name <> ''
            GROUP BY partition_name
            ORDER BY cpu_hours DESC
            """.format(ntasks=ntasks_expr, allocated_memory_expr=allocated_memory_expr, disk_read_expr=disk_read_expr, disk_write_expr=disk_write_expr), [start, end],
        )
        return JsonResponse(dataframe_to_records(query), safe=False)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@require_GET
def api_gpu(request):
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    result = {}
    for key in ("gpu", "gpu_efficiency"):
        df = snapshot.analytics.get(key)
        if df is not None and not df.is_empty():
            result.update(df.to_dicts()[0])
    return JsonResponse(result)


@require_GET
def api_runtime(request):
    snapshot, response = _snapshot_or_response()
    if response:
        return response
    result = snapshot.analytics["runtime"].to_dicts()
    return JsonResponse(result[0] if result else {})


@require_GET
def api_jobs(request):
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    limit = request.GET.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            return JsonResponse({"error": "limit must be an integer"}, status=400)
        if limit < 1:
            return JsonResponse({"error": "limit must be greater than zero"}, status=400)

    jobs = snapshot.ingestion.consolidated_jobs
    return JsonResponse({
        "count": jobs.height,
        "results": dataframe_to_records(jobs, limit=limit),
    })


@require_GET
def api_timeseries(request):
    """Return bucketed job/resource time-series data from the current snapshot."""
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    interval = request.GET.get("interval", "1h")
    start = request.GET.get("start") or None
    end = request.GET.get("end") or None

    try:
        # Historical charts must come from the persistent DuckDB store, not
        # the live snapshot. The snapshot intentionally contains only the
        # current refresh window, while DuckDB contains the accumulated
        # accounting history.
        if start or end:
            from datetime import datetime, timedelta, timezone

            now = datetime.now(timezone.utc)
            requested_end = end or now.isoformat()
            requested_start = start
            if requested_start is None:
                requested_start = (now - timedelta(days=30)).isoformat()

            historical_jobs = query_df(
                """
                SELECT
                    CAST(j.canonical_job_id AS VARCHAR) AS canonical_job_id,
                    CAST(j.submit_time AS VARCHAR) AS submit_time,
                    CAST(j.start_time AS VARCHAR) AS start_time,
                    CAST(j.end_time AS VARCHAR) AS end_time,
                    CAST(j.maxrss AS VARCHAR) AS job_maxrss,
                    j.state_category,
                    j.ncpus,
                    j.gpu_count,
                    j.allocated_memory_bytes,
                    NULL::DOUBLE AS max_rss_bytes,
                    j.elapsed_seconds,
                    j.time_limit_seconds
                FROM jobs j
                WHERE (
                    j.start_time IS NOT NULL
                    AND j.start_time < ?
                    AND (j.end_time IS NULL OR j.end_time > ?)
                ) OR (
                    j.submit_time >= TRY_CAST(? AS TIMESTAMPTZ)
                    AND j.submit_time < TRY_CAST(? AS TIMESTAMPTZ)
                )
                """,
                [requested_end, requested_start, requested_start, requested_end],
                schema={
                    "canonical_job_id": pl.String,
                    "submit_time": pl.String,
                    "start_time": pl.String,
                    "end_time": pl.String,
                    "job_maxrss": pl.String,
                    "state_category": pl.String,
                    "ncpus": pl.Float64,
                    "gpu_count": pl.Float64,
                    "allocated_memory_bytes": pl.Float64,
                    "max_rss_bytes": pl.Float64,
                    "elapsed_seconds": pl.Float64,
                    "time_limit_seconds": pl.Float64,
                },
            )
            # Parse MaxRSS values from job-step rows in Python instead of relying
            # on SQL regexp behavior, which varies across DuckDB versions and
            # can silently yield NULL for otherwise valid sacct values.
            import re
            step_rows = query_df(
                "SELECT canonical_job_id, maxrss FROM job_steps WHERE maxrss IS NOT NULL",
                [],
            )
            def parse_rss_bytes(value):
                if value is None:
                    return None
                match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*[cCnN]?\s*$", str(value), re.I)
                if not match:
                    return None
                powers = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
                return float(match.group(1)) * (1024.0 ** powers[match.group(2).upper()])
            rss_by_job = {}
            for step in step_rows.to_dicts() if not step_rows.is_empty() else []:
                rss = parse_rss_bytes(step.get("maxrss"))
                if rss is not None:
                    key = str(step.get("canonical_job_id") or "")
                    if key:
                        rss_by_job[key] = max(rss_by_job.get(key, 0.0), rss)
            if "canonical_job_id" in historical_jobs.columns:
                rss_values = [
                    rss_by_job.get(str(job_id or "")) or parse_rss_bytes(raw_rss)
                    for job_id, raw_rss in zip(
                        historical_jobs.get_column("canonical_job_id").to_list(),
                        historical_jobs.get_column("job_maxrss").to_list(),
                    )
                ]
                historical_jobs = historical_jobs.with_columns(
                    pl.Series("max_rss_bytes", rss_values, dtype=pl.Float64, strict=False)
                )
            result = time_series(
                historical_jobs,
                start=requested_start,
                end=requested_end,
                interval=interval,
            )
        else:
            result = time_series(
                snapshot.ingestion.consolidated_jobs,
                start=start,
                end=end,
                interval=interval,
            )
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)

    return JsonResponse({
        "interval": interval,
        "start": start,
        "end": end,
        "results": dataframe_to_records(result),
    })


@require_GET
def api_queue(request):
    """Return the live queue from squeue; this is not part of the cached snapshot."""
    try:
        df = fetch_squeue()
    except Exception as exc:
        return JsonResponse({"status": "error", "error": str(exc)}, status=502)
    return JsonResponse({"count": df.height, "results": dataframe_to_records(df)}, safe=False)


@require_POST
def api_refresh(request):
    """Build and atomically publish a new snapshot."""
    config = SacctConfig(
        clusters=request.POST.get("clusters", "all"),
        start=request.POST.get("start") or None,
        end=request.POST.get("end") or None,
    )

    try:
        snapshot = refresh_slurm(
            sacct_config=config,
            refresh_nodes=request.POST.get("nodes", "1") not in ("0", "false", "False"),
        )
    except Exception as exc:
        # refresh_slurm does not replace state on failure, so the previous
        # snapshot remains available to readers.
        return JsonResponse({"status": "error", "error": str(exc)}, status=502)

    return JsonResponse({
        "status": "ok",
        "ready": True,
        "job_count": snapshot.ingestion.consolidated_jobs.height,
        "step_count": snapshot.ingestion.steps.height,
        "refreshed_at": snapshot.refreshed_at.isoformat(),
    })

@require_GET
def api_db_metadata(request):
    from slurm_analytics.storage import connect, get_metadata

    con = connect()
    try:
        initialized_value = get_metadata(con, "initialized", "false")
        database = con.execute("PRAGMA database_list").fetchall()

        print(
            f"DEBUG api_db_metadata: "
            f"initialized={initialized_value!r}, database={database}",
            flush=True,
        )

        return JsonResponse({
            "ready": initialized_value == "true",
            "initialized_through": get_metadata(con, "initialized_through"),
            "last_refresh_start": get_metadata(con, "last_refresh_start"),
            "last_refresh_end": get_metadata(con, "last_refresh_end"),
        })
    finally:
        con.close()

@require_GET
def api_db_metadata_debug(request):
    from slurm_analytics.storage import connect, get_metadata
    con = connect()
    try:
        initialized = get_metadata(con, "initialized", "false") == "true"
        return JsonResponse({
            "ready": initialized,
            "initialized_through": get_metadata(con, "initialized_through"),
            "last_refresh_start": get_metadata(con, "last_refresh_start"),
            "last_refresh_end": get_metadata(con, "last_refresh_end"),
        })
    finally:
        con.close()


@require_GET
def api_account_usage(request):
    from slurm_analytics.accounts import account_usage
    try:
        limit = max(1, min(int(request.GET.get("limit", "100")), 10000))
        result = account_usage(
            request.GET.get("start"), request.GET.get("end"), request.GET.get("account"),
            limit, request.GET.get("sort", "cpu_hours"), request.GET.get("direction", "desc"),
        )
        return JsonResponse(dataframe_to_records(result), safe=False)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@require_GET
def api_account_tree(request):
    from slurm_analytics.accounts import account_tree
    try:
        return JsonResponse(
            account_tree(
                request.GET.get("start"), request.GET.get("end"),
                request.GET.get("sort", "name"), request.GET.get("direction", "asc"),
            ),
            safe=False,
        )
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@require_GET
def api_user_usage(request):
    from slurm_analytics.accounts import user_usage
    try:
        limit = max(1, min(int(request.GET.get("limit", "100")), 10000))
        result = user_usage(
            request.GET.get("start"), request.GET.get("end"), request.GET.get("user"), request.GET.get("account"),
            limit, request.GET.get("sort", "cpu_hours"), request.GET.get("direction", "desc"),
        )
        return JsonResponse(dataframe_to_records(result), safe=False)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)
