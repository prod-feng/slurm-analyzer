"""HTTP API for the current Slurm analytics snapshot."""

from __future__ import absolute_import

from django.http import JsonResponse
import polars as pl
from django.views.decorators.http import require_GET, require_POST

from slurm_analytics.sacct import SacctConfig
from slurm_analytics.squeue import fetch_squeue
from slurm_analytics.analytics import time_series
from slurm_analytics.storage import query_df, connect, get_metadata

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
    """Return lightweight database metadata without loading the full snapshot.

    The DuckDB file is the durable source of truth. Counting rows here avoids
    unpickling/rebuilding a multi-year Polars snapshot just to render Overview.
    """
    from datetime import datetime, timezone

    try:
        con = connect()
        try:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            if "ingestion_metadata" not in tables or get_metadata(con, "initialized", "false") != "true":
                return _not_ready()
            # Do not COUNT(*) across multi-year accounting tables just to paint
            # the Overview header. Those counts are not used by the UI, and on
            # multi-GB databases this adds avoidable work to every first load.
            job_count = get_metadata(con, "job_count")
            step_count = get_metadata(con, "step_count")
            node_count = get_metadata(con, "node_count")
            refreshed_at = get_metadata(con, "last_refresh_end") or get_metadata(con, "last_refresh_start")
        finally:
            con.close()
        return JsonResponse({
            "ready": True,
            "refreshed_at": refreshed_at or datetime.now(timezone.utc).isoformat(),
            "job_count": int(job_count) if job_count is not None else None,
            "step_count": int(step_count) if step_count is not None else None,
            "node_count": int(node_count) if node_count is not None else None,
        })
    except Exception as exc:
        return JsonResponse({"error": "Unable to read database metadata: {}".format(exc)}, status=500)



def _calculate_overview_additional_metrics(start, end):
    """Calculate expensive Overview metrics, including job_steps telemetry, on demand."""
    peak_row = {}
    # Peak values are calculated over terminal jobs started in the selected range.
    # Build expressions from each cluster's actual schema: imported databases can
    # predate normalized columns, and one absent field must not hide every metric.
    try:
        import logging
        import re
        from slurm_analytics.storage import connect

        logger = logging.getLogger(__name__)
        con = connect()
        try:
            available_job_columns = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
            try:
                available_step_columns = {r[0].lower() for r in con.execute("DESCRIBE job_steps").fetchall()}
            except Exception:
                available_step_columns = set()
        finally:
            con.close()

        def job_num(column):
            return "TRY_CAST(tj.{0} AS DOUBLE)".format(column) if column in available_job_columns else "NULL::DOUBLE"

        def step_num(column):
            return "TRY_CAST(js.{0} AS DOUBLE)".format(column) if column in available_step_columns else "NULL::DOUBLE"

        ncpus_expr = job_num("ncpus")
        gpu_count_expr = job_num("gpu_count")
        nnodes_expr = job_num("nnodes")
        ntasks_expr = job_num("ntasks")
        time_limit_expr = job_num("time_limit_seconds")
        elapsed_expr = job_num("elapsed_seconds")
        gpu_mem_expr = job_num("gpu_mem_max_bytes")
        maxrss_source = "tj.maxrss" if "maxrss" in available_job_columns else "NULL::VARCHAR"
        state_source = (
            "COALESCE(NULLIF(state_category, ''), state, '')"
            if {"state_category", "state"}.issubset(available_job_columns)
            else ("state_category" if "state_category" in available_job_columns else ("state" if "state" in available_job_columns else "''"))
        )
        job_key = "canonical_job_id" if "canonical_job_id" in available_job_columns else ("jobid" if "jobid" in available_job_columns else None)
        step_key = "canonical_job_id" if "canonical_job_id" in available_step_columns else ("jobid" if "jobid" in available_step_columns else None)

        # A step aggregate is useful if ANY telemetry column is present. In
        # particular, ntasks/GPU memory must not depend on MaxRSS being stored.
        step_selects = []
        if "maxrss" in available_step_columns:
            step_selects.append("MAX(TRY_CAST(regexp_extract(upper(CAST(js.maxrss AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE) * CASE regexp_extract(upper(CAST(js.maxrss AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2) WHEN 'K' THEN 1024.0 WHEN 'M' THEN 1048576.0 WHEN 'G' THEN 1073741824.0 WHEN 'T' THEN 1099511627776.0 WHEN 'P' THEN 1125899906842624.0 WHEN 'E' THEN 1152921504606846976.0 ELSE 1.0 END) AS max_rss_bytes")
        else:
            step_selects.append("NULL::DOUBLE AS max_rss_bytes")
        step_selects.append(("MAX({})".format(step_num("ntasks")) if "ntasks" in available_step_columns else "NULL::DOUBLE") + " AS max_ntasks")
        step_selects.append(("MAX({})".format(step_num("gpu_mem_bytes")) if "gpu_mem_bytes" in available_step_columns else "NULL::DOUBLE") + " AS max_gpu_mem_bytes")
        step_selects.append(("MAX(CAST(js.maxdiskread AS VARCHAR))" if "maxdiskread" in available_step_columns else "NULL::VARCHAR") + " AS maxdiskread")
        step_selects.append(("MAX(CAST(js.maxdiskwrite AS VARCHAR))" if "maxdiskwrite" in available_step_columns else "NULL::VARCHAR") + " AS maxdiskwrite")
        if job_key and step_key and available_step_columns and any(c in available_step_columns for c in ("maxrss", "ntasks", "gpu_mem_bytes", "maxdiskread", "maxdiskwrite")):
            step_sql = "SELECT CAST(js.{step_key} AS VARCHAR) AS job_join_key, {selects} FROM job_steps js JOIN terminal_jobs tj ON CAST(tj.{job_key} AS VARCHAR) = CAST(js.{step_key} AS VARCHAR) GROUP BY CAST(js.{step_key} AS VARCHAR)".format(
                step_key=step_key, job_key=job_key, selects=", ".join(step_selects))
        else:
            step_sql = "SELECT NULL::VARCHAR AS job_join_key, NULL::DOUBLE AS max_rss_bytes, NULL::DOUBLE AS max_ntasks, NULL::DOUBLE AS max_gpu_mem_bytes, NULL::VARCHAR AS maxdiskread, NULL::VARCHAR AS maxdiskwrite WHERE FALSE"

        reqmem_source = "tj.reqmem" if "reqmem" in available_job_columns else "NULL::VARCHAR"
        reqmem_bytes_expr = """TRY_CAST(regexp_extract(upper(CAST({reqmem_source} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)
            * CASE regexp_extract(upper(CAST({reqmem_source} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)
                WHEN 'K' THEN 1024.0 WHEN 'M' THEN 1048576.0 WHEN 'G' THEN 1073741824.0
                WHEN 'T' THEN 1099511627776.0 WHEN 'P' THEN 1125899906842624.0
                WHEN 'E' THEN 1152921504606846976.0 ELSE 1.0 END
            * CASE lower(regexp_extract(upper(CAST({reqmem_source} AS VARCHAR)), '([CN])$', 1))
                WHEN 'C' THEN COALESCE({ncpus_expr}, 0)
                ELSE COALESCE({nnodes_expr}, 0) END""".format(reqmem_source=reqmem_source, ncpus_expr=ncpus_expr, nnodes_expr=nnodes_expr)
        if "allocated_memory_bytes" in available_job_columns:
            requested_memory_expr = "COALESCE(MAX(TRY_CAST(tj.allocated_memory_bytes AS DOUBLE)), MAX({})) / 1073741824.0 AS max_requested_memory_gib".format(reqmem_bytes_expr)
        else:
            requested_memory_expr = "MAX({}) / 1073741824.0 AS max_requested_memory_gib".format(reqmem_bytes_expr)
        if job_key:
            step_join = "sr.job_join_key = CAST(tj.{0} AS VARCHAR)".format(job_key)
        else:
            step_join = "FALSE"

        peaks = query_df(
            """
            WITH terminal_jobs AS (
                SELECT * FROM jobs
                WHERE TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                  AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                  AND lower(COALESCE({state_source}, '')) NOT IN ('running', 'pending')
            ), step_metrics AS ({step_sql})
            SELECT
                MAX({ncpus_expr}) AS max_cpus_per_job,
                MAX({gpu_count_expr}) AS max_gpus_per_job,
                {requested_memory_expr},
                MAX({nnodes_expr}) AS max_nodes_per_job,
                AVG({nnodes_expr}) AS avg_nodes_per_job,
                MAX(COALESCE(NULLIF({ntasks_expr}, 0), sr.max_ntasks)) AS max_tasks_per_job,
                AVG(COALESCE(NULLIF({ntasks_expr}, 0), sr.max_ntasks)) AS avg_tasks_per_job,
                MAX(COALESCE(sr.max_rss_bytes,
                    TRY_CAST(regexp_extract(upper(CAST({maxrss_source} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)
                    * CASE regexp_extract(upper(CAST({maxrss_source} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)
                        WHEN 'K' THEN 1024.0 WHEN 'M' THEN 1048576.0 WHEN 'G' THEN 1073741824.0
                        WHEN 'T' THEN 1099511627776.0 WHEN 'P' THEN 1125899906842624.0
                        WHEN 'E' THEN 1152921504606846976.0 ELSE 1.0 END
                )) / 1073741824.0 AS max_recorded_rss_gib,
                MAX({time_limit_expr}) / 3600.0 AS max_time_limit_hours,
                MAX({elapsed_expr}) / 3600.0 AS max_elapsed_runtime_hours,
                MAX(COALESCE(NULLIF({gpu_mem_expr}, 0), sr.max_gpu_mem_bytes)) / 1073741824.0 AS max_recorded_gpu_memory_gib
            FROM terminal_jobs tj LEFT JOIN step_metrics sr ON {step_join}
            """.format(
                state_source=state_source, step_sql=step_sql, ncpus_expr=ncpus_expr,
                gpu_count_expr=gpu_count_expr, requested_memory_expr=requested_memory_expr,
                nnodes_expr=nnodes_expr, ntasks_expr=ntasks_expr, maxrss_source=maxrss_source,
                time_limit_expr=time_limit_expr, elapsed_expr=elapsed_expr,
                gpu_mem_expr=gpu_mem_expr, step_join=step_join),
            [start, end],
        )
        peak_row = peaks.to_dicts()[0] if not peaks.is_empty() else {}

        # Python-side MaxRSS parsing is retained as a fallback for DuckDB regex
        # differences, and only runs when both job and step IDs/MaxRSS exist.
        if job_key and step_key and "maxrss" in available_step_columns:
            joined_state_source = (
                "COALESCE(NULLIF(j.state_category, ''), j.state, '')"
                if {"state_category", "state"}.issubset(available_job_columns)
                else ("j.state_category" if "state_category" in available_job_columns else ("j.state" if "state" in available_job_columns else "''"))
            )
            rss_rows = query_df(
                """SELECT js.maxrss FROM job_steps js JOIN jobs j
                   ON CAST(j.{job_key} AS VARCHAR) = CAST(js.{step_key} AS VARCHAR)
                   WHERE TRY_CAST(j.start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                     AND TRY_CAST(j.start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                     AND lower(COALESCE({state_source}, '')) NOT IN ('running', 'pending')
                     AND js.maxrss IS NOT NULL""".format(job_key=job_key, step_key=step_key, state_source=joined_state_source),
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
        import logging
        logging.getLogger(__name__).exception("Failed to calculate Overview peak metrics")
        peak_row = {}

    # Prefer sacct MaxDiskRead/MaxDiskWrite telemetry. Some imported databases
    # have NULL allocation-level values while job_steps contains the readings.
    # Use every shared job identifier for the step join; older imports can have
    # canonical_job_id values that do not line up even when jobid does.
    try:
        import logging
        import re
        from slurm_analytics.storage import connect

        disk_logger = logging.getLogger(__name__)
        con = connect()
        try:
            job_columns = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
            try:
                step_columns = {r[0].lower() for r in con.execute("DESCRIBE job_steps").fetchall()}
            except Exception:
                step_columns = set()
        finally:
            con.close()

        def disk_bytes(value):
            if value is None:
                return None
            # sacct values may be strings such as 11265.10M or plain numbers.
            raw = str(value).strip().replace(",", "")
            match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*$", raw, re.I)
            if not match:
                return None
            power = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}[match.group(2).upper()]
            return float(match.group(1)) * (1024.0 ** power)

        read_values, write_values, io_values = [], [], []
        if ("maxdiskread" in job_columns) or ("maxdiskwrite" in job_columns):
            read_col = "maxdiskread" if "maxdiskread" in job_columns else "NULL AS maxdiskread"
            write_col = "maxdiskwrite" if "maxdiskwrite" in job_columns else "NULL AS maxdiskwrite"
            state_expr = (
                "COALESCE(NULLIF(state_category, ''), state, '')"
                if {"state_category", "state"}.issubset(job_columns)
                else ("state_category" if "state_category" in job_columns else ("state" if "state" in job_columns else "''"))
            )
            io_rows = query_df(
                """SELECT {read_col}, {write_col} FROM jobs
                   WHERE TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                     AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                     AND lower(COALESCE({state_expr}, '')) NOT IN ('running', 'pending')""".format(
                         read_col=read_col, write_col=write_col, state_expr=state_expr),
                [start, end],
            )
            for item in io_rows.to_dicts() if not io_rows.is_empty() else []:
                read_bytes = disk_bytes(item.get("maxdiskread"))
                write_bytes = disk_bytes(item.get("maxdiskwrite"))
                if read_bytes is not None:
                    read_values.append(read_bytes / (1024.0 ** 3))
                if write_bytes is not None:
                    write_values.append(write_bytes / (1024.0 ** 3))
                if read_bytes is not None or write_bytes is not None:
                    io_values.append(((read_bytes or 0.0) + (write_bytes or 0.0)) / (1024.0 ** 3))

        # Only use step data for a metric when job-level telemetry is missing
        # or all zero. Aggregate step readings by maximum, not by summing steps.
        need_step_read = not any(value > 0 for value in read_values)
        need_step_write = not any(value > 0 for value in write_values)
        if (need_step_read or need_step_write) and step_columns and ("maxdiskread" in step_columns or "maxdiskwrite" in step_columns):
            common_keys = [key for key in ("canonical_job_id", "jobid") if key in job_columns and key in step_columns]
            if common_keys:
                # Avoid an OR across join keys. DuckDB can produce a very expensive
                # join plan for that predicate on clusters whose disk telemetry is
                # stored on hundreds of thousands of job_steps. Use independent
                # equality joins instead; duplicates across keys do not affect the
                # peak values calculated below.
                step_read_select = "js.maxdiskread AS maxdiskread" if "maxdiskread" in step_columns else "NULL AS maxdiskread"
                step_write_select = "js.maxdiskwrite AS maxdiskwrite" if "maxdiskwrite" in step_columns else "NULL AS maxdiskwrite"
                step_state_expr = (
                    "COALESCE(NULLIF(j.state_category, ''), j.state, '')"
                    if {"state_category", "state"}.issubset(job_columns)
                    else ("j.state_category" if "state_category" in job_columns else ("j.state" if "state" in job_columns else "''"))
                )
                step_queries = []
                step_params = []
                for key in common_keys:
                    step_queries.append(
                        """SELECT {step_read_select}, {step_write_select}
                           FROM job_steps js
                           JOIN jobs j ON CAST(j.{key} AS VARCHAR) = CAST(js.{key} AS VARCHAR)
                           WHERE TRY_CAST(j.start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                             AND TRY_CAST(j.start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                             AND lower(COALESCE({state_expr}, '')) NOT IN ('running', 'pending')""".format(
                            step_read_select=step_read_select,
                            step_write_select=step_write_select,
                            key=key,
                            state_expr=step_state_expr,
                        )
                    )
                    step_params.extend([start, end])
                step_rows = query_df(" UNION ALL ".join(step_queries), step_params)
                for item in step_rows.to_dicts() if not step_rows.is_empty() else []:
                    read_bytes = disk_bytes(item.get("maxdiskread"))
                    write_bytes = disk_bytes(item.get("maxdiskwrite"))
                    if need_step_read and read_bytes is not None:
                        read_values.append(read_bytes / (1024.0 ** 3))
                    if need_step_write and write_bytes is not None:
                        write_values.append(write_bytes / (1024.0 ** 3))
                    if (need_step_read and read_bytes is not None) or (need_step_write and write_bytes is not None):
                        io_values.append(((read_bytes or 0.0) + (write_bytes or 0.0)) / (1024.0 ** 3))
            else:
                disk_logger.warning("Overview disk I/O fallback unavailable: no shared job identifier between jobs and job_steps")

        peak_row["max_disk_read_gib"] = max(read_values) if read_values else None
        peak_row["max_disk_write_gib"] = max(write_values) if write_values else None
        peak_row["max_disk_io_gib"] = max(io_values) if io_values else None
    except Exception:
        import logging
        logging.getLogger(__name__).exception("Failed to calculate Overview disk I/O metrics")
        peak_row["max_disk_read_gib"] = None
        peak_row["max_disk_write_gib"] = None
        peak_row["max_disk_io_gib"] = None

    return peak_row


@require_GET
def api_summary(request):
    """Return date-range job outcomes/resource totals and live queue counts."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    start = request.GET.get("start")
    end = request.GET.get("end") or now.isoformat()
    if not start:
        start = (now - timedelta(days=30)).isoformat()

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
                        WHEN upper(COALESCE(state, '')) LIKE 'TIMEOUT%' THEN 'timeout'
                        WHEN upper(COALESCE(state, '')) LIKE 'FAILED%'
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
                WHERE (
                    TRY_CAST(submit_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                    AND TRY_CAST(submit_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                ) OR (
                    TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                    AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                ) OR (
                    TRY_CAST(end_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                    AND TRY_CAST(end_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                )
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
                      AND summary_category = 'timeout'
                ) AS timeout_count,
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
            [start, end] * 3 + [start, end] * 6 + [start, end, start, end],
        )
        row = historical.to_dicts()[0] if not historical.is_empty() else {}
    except Exception as exc:
        return JsonResponse({"error": "Unable to calculate period summary: {}".format(exc)}, status=500)

    # Running and pending are live queue states, not historical database
    # metrics. Only query squeue for clusters configured as live; otherwise
    # every imported cluster would incorrectly show the same local queue.
    cluster = getattr(request, "slurm_cluster", {}) or {}
    if cluster.get("mode", "live") != "live":
        running_count = None
        pending_count = None
    else:
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
        "timeout_count": int(row.get("timeout_count") or 0),
        "other_count": int(row.get("other_count") or 0),
        "running_count": running_count,
        "pending_count": pending_count,
        "cpu_hours": float(row.get("cpu_hours") or 0),
        "gpu_hours": float(row.get("gpu_hours") or 0),
        # Keep the existing summary response shape for API clients. These
        # expensive values are populated by api_additional_job_metrics on demand.
        "max_cpus_per_job": None,
        "max_gpus_per_job": None,
        "max_requested_memory_gib": None,
        "max_nodes_per_job": None,
        "avg_nodes_per_job": None,
        "max_tasks_per_job": None,
        "avg_tasks_per_job": None,
        "max_recorded_rss_gib": None,
        "max_time_limit_hours": None,
        "max_elapsed_runtime_hours": None,
        "max_recorded_gpu_memory_gib": None,
        "max_disk_io_gib": None,
        "max_disk_read_gib": None,
        "max_disk_write_gib": None,
        "start": start,
        "end": end,
    })

@require_GET
def api_additional_job_metrics(request):
    """Return expensive per-job/step metrics only when the Overview section is opened."""
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    start = request.GET.get("start")
    end = request.GET.get("end") or now.isoformat()
    if not start:
        start = (now - timedelta(days=7)).isoformat()

    try:
        metrics = _calculate_overview_additional_metrics(start, end)
        return JsonResponse({
            "max_cpus_per_job": metrics.get("max_cpus_per_job"),
            "max_gpus_per_job": metrics.get("max_gpus_per_job"),
            "max_requested_memory_gib": metrics.get("max_requested_memory_gib"),
            "max_nodes_per_job": metrics.get("max_nodes_per_job"),
            "avg_nodes_per_job": metrics.get("avg_nodes_per_job"),
            "max_tasks_per_job": metrics.get("max_tasks_per_job"),
            "avg_tasks_per_job": metrics.get("avg_tasks_per_job"),
            "max_recorded_rss_gib": metrics.get("max_recorded_rss_gib"),
            "max_time_limit_hours": metrics.get("max_time_limit_hours"),
            "max_elapsed_runtime_hours": metrics.get("max_elapsed_runtime_hours"),
            "max_recorded_gpu_memory_gib": metrics.get("max_recorded_gpu_memory_gib"),
            "max_disk_io_gib": metrics.get("max_disk_io_gib"),
            "max_disk_read_gib": metrics.get("max_disk_read_gib"),
            "max_disk_write_gib": metrics.get("max_disk_write_gib"),
            "start": start,
            "end": end,
        })
    except Exception as exc:
        import logging
        logging.getLogger(__name__).exception("Failed to calculate Overview additional job metrics")
        return JsonResponse({"error": "Unable to calculate additional job metrics: {}".format(exc)}, status=500)


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
    """Aggregate terminal-job resource usage by partition over a date range.

    Use allocation-row MaxDiskRead/MaxDiskWrite when positive. Some clusters
    populate these fields only on job steps, so fall back to the maximum step
    value for that job. Task count also falls back to the maximum step NTasks
    when the allocation row has no usable value.
    """
    snapshot, response = _snapshot_or_response()
    if response:
        return response
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    start = request.GET.get("start") or (now - timedelta(days=7)).isoformat()
    end = request.GET.get("end") or now.isoformat()
    try:
        from slurm_analytics.storage import connect
        con = connect()
        try:
            cols = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
            try:
                step_cols = {r[0].lower() for r in con.execute("DESCRIBE job_steps").fetchall()}
            except Exception:
                step_cols = set()
        finally:
            con.close()

        def disk_gib_expr(column, alias, available_columns):
            if column not in available_columns:
                return "NULL::DOUBLE"
            value = "TRY_CAST(regexp_extract(upper(CAST({0}.{1} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)".format(alias, column)
            unit = "regexp_extract(upper(CAST({0}.{1} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)".format(alias, column)
            return "(({value}) * CASE {unit} WHEN 'K' THEN 1.0/1048576.0 WHEN 'M' THEN 1.0/1024.0 WHEN 'G' THEN 1.0 WHEN 'T' THEN 1024.0 WHEN 'P' THEN 1048576.0 WHEN 'E' THEN 1073741824.0 ELSE 1.0/1073741824.0 END)".format(value=value, unit=unit)

        job_read = disk_gib_expr("maxdiskread", "j", cols)
        job_write = disk_gib_expr("maxdiskwrite", "j", cols)
        step_read = disk_gib_expr("maxdiskread", "js", step_cols)
        step_write = disk_gib_expr("maxdiskwrite", "js", step_cols)
        step_tasks = "TRY_CAST(js.ntasks AS DOUBLE)" if "ntasks" in step_cols else "NULL::DOUBLE"
        if "canonical_job_id" in step_cols and "canonical_job_id" in cols:
            step_io_sql = """SELECT js.canonical_job_id,
                       MAX({step_read}) AS disk_read_gib,
                       MAX({step_write}) AS disk_write_gib,
                       MAX({step_tasks}) AS max_ntasks
                FROM job_steps js
                JOIN filtered_jobs fj ON fj.canonical_job_id = js.canonical_job_id
                GROUP BY js.canonical_job_id""".format(step_read=step_read, step_write=step_write, step_tasks=step_tasks)
            step_join_condition = "si.canonical_job_id = j.canonical_job_id"
        else:
            step_io_sql = "SELECT NULL::VARCHAR AS canonical_job_id, NULL::DOUBLE AS disk_read_gib, NULL::DOUBLE AS disk_write_gib, NULL::DOUBLE AS max_ntasks WHERE FALSE"
            step_join_condition = "FALSE"

        job_tasks = "TRY_CAST(j.ntasks AS DOUBLE)" if "ntasks" in cols else "NULL::DOUBLE"
        ntasks_expr = "COALESCE(NULLIF({job_tasks}, 0), si.max_ntasks, 0.0)".format(job_tasks=job_tasks)
        disk_read_expr = "CASE WHEN COALESCE({job_read}, 0) > 0 THEN {job_read} ELSE COALESCE(si.disk_read_gib, {job_read}, 0.0) END".format(job_read=job_read)
        disk_write_expr = "CASE WHEN COALESCE({job_write}, 0) > 0 THEN {job_write} ELSE COALESCE(si.disk_write_gib, {job_write}, 0.0) END".format(job_write=job_write)
        allocated_memory_expr = "TRY_CAST(allocated_memory_bytes AS DOUBLE)" if "allocated_memory_bytes" in cols else "NULL::DOUBLE"
        query = query_df(
            """
            WITH filtered_jobs AS (
                SELECT j.* FROM jobs j
                WHERE TRY_CAST(j.start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ)
                  AND TRY_CAST(j.start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ)
                  AND lower(COALESCE(j.state_category, '')) NOT IN ('running', 'pending')
            ), step_io AS ({step_io_sql}), base_jobs AS (
                SELECT j.*,
                       COALESCE(NULLIF(TRIM(j.partition), ''), '(Unknown partition)') AS partition_list,
                       {ntasks_expr} AS resolved_ntasks,
                       {disk_read_expr} AS resolved_disk_read_gib,
                       {disk_write_expr} AS resolved_disk_write_gib
                FROM filtered_jobs j
                LEFT JOIN step_io si ON {step_join_condition}
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
                   SUM(COALESCE(resolved_ntasks, 0) / partition_choices) AS total_tasks,
                   SUM(COALESCE(resolved_disk_read_gib, 0) / partition_choices) AS disk_read_gib,
                   SUM(COALESCE(resolved_disk_write_gib, 0) / partition_choices) AS disk_write_gib,
                   AVG(resolved_ntasks) AS avg_tasks_per_job,
                   MAX(resolved_ntasks) AS max_tasks_per_job,
                   AVG(TRY_CAST(nnodes AS DOUBLE)) AS avg_nodes_per_job,
                   MAX(TRY_CAST(nnodes AS DOUBLE)) AS max_nodes_per_job
            FROM split_jobs
            WHERE partition_name <> ''
            GROUP BY partition_name
            ORDER BY cpu_hours DESC
            """.format(
                step_io_sql=step_io_sql,
                step_join_condition=step_join_condition,
                ntasks_expr=ntasks_expr,
                disk_read_expr=disk_read_expr,
                disk_write_expr=disk_write_expr,
                allocated_memory_expr=allocated_memory_expr,
            ),
            [start, end],
        )
        return JsonResponse(dataframe_to_records(query), safe=False)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@require_GET
def api_partition_gpu_types(request):
    """Summarize GPU models, falling back to node inventory for generic GPU TRES."""
    snapshot, response = _snapshot_or_response()
    if response:
        return response
    from datetime import datetime, timedelta, timezone
    import re
    import subprocess
    from collections import defaultdict
    from slurm_analytics.storage import connect

    now = datetime.now(timezone.utc)
    start = request.GET.get("start") or (now - timedelta(days=7)).isoformat()
    end = request.GET.get("end") or now.isoformat()
    try:
        con = connect()
        try:
            cols = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
        finally:
            con.close()
        if "alloctres" not in cols:
            return JsonResponse({"types": [], "partition_types": [], "note": "AllocTRES is not stored; GPU models cannot be identified."})
        partition_expr = "COALESCE(NULLIF(TRIM(partition), ''), '(Unknown partition)')" if "partition" in cols else "'(Unknown partition)'"
        elapsed_expr = "TRY_CAST(elapsed_seconds AS DOUBLE)" if "elapsed_seconds" in cols else "NULL::DOUBLE"
        state_expr = "lower(COALESCE(state_category, ''))" if "state_category" in cols else "''"
        start_expr = "TRY_CAST(start_time AS TIMESTAMPTZ)" if "start_time" in cols else "NULL::TIMESTAMPTZ"
        id_expr = "COALESCE(CAST(canonical_job_id AS VARCHAR), CAST(jobid AS VARCHAR))" if "canonical_job_id" in cols and "jobid" in cols else ("CAST(canonical_job_id AS VARCHAR)" if "canonical_job_id" in cols else "CAST(jobid AS VARCHAR)")
        nodelist_expr = "COALESCE(CAST(nodelist AS VARCHAR), '')" if "nodelist" in cols else "''"
        frame = query_df(
            "SELECT {id_expr} AS job_id, {partition_expr} AS partition_list, alloctres, {elapsed_expr} AS elapsed_seconds, {nodelist_expr} AS nodelist FROM jobs WHERE {start_expr} >= TRY_CAST(? AS TIMESTAMPTZ) AND {start_expr} < TRY_CAST(? AS TIMESTAMPTZ) AND {state_expr} NOT IN ('running','pending')".format(
                id_expr=id_expr, partition_expr=partition_expr, elapsed_expr=elapsed_expr,
                nodelist_expr=nodelist_expr, start_expr=start_expr, state_expr=state_expr), [start, end])

        typed_pattern = re.compile(r"(?:gres/)?gpu:([^=,:]+)(?::[^=,]+)?=([0-9]+(?:\.[0-9]+)?)", re.I)
        generic_pattern = re.compile(r"(?:gres/)?gpu=([0-9]+(?:\.[0-9]+)?)", re.I)
        node_typed_pattern = re.compile(r"(?:^|,)gpu:([^:,()]+):([0-9]+)(?=\(|,|$)", re.I)
        node_generic_pattern = re.compile(r"(?:^|,)gpu:([0-9]+)(?=\(|,|$)", re.I)

        # Current node inventory provides model information when old/generic
        # AllocTRES records contain only gres/gpu=N. Build a node -> model/capacity map.
        node_models = defaultdict(lambda: defaultdict(float))
        partition_models = defaultdict(lambda: defaultdict(float))
        nodes_df = getattr(snapshot, "nodes", None)
        if nodes_df is None or nodes_df.is_empty():
            # Snapshots rebuilt from DuckDB after a web-worker restart do not
            # include node inventory. Fetch it only on demand (this endpoint is
            # itself lazy-loaded by the UI), then retain it on the snapshot.
            try:
                from slurm_analytics.node_inventory import refresh_node_inventory
                nodes_df = refresh_node_inventory()
                snapshot.nodes = nodes_df
            except Exception:
                nodes_df = None
        if nodes_df is not None and not nodes_df.is_empty():
            for node in nodes_df.to_dicts():
                node_name = str(node.get("node_name") or "").strip()
                gres = str(node.get("gres") or "")
                if not node_name:
                    continue
                for match in node_typed_pattern.finditer(gres):
                    node_models[node_name][match.group(1).strip().upper()] += float(match.group(2))
                # A model-specific GresUsed value may be present even when Gres is generic.
                if not node_models[node_name]:
                    gres_used = str(node.get("gres_used") or "")
                    for match in node_typed_pattern.finditer(gres_used):
                        node_models[node_name][match.group(1).strip().upper()] += float(match.group(2))

        hostname_cache = {}
        def expand_nodelist(expression):
            expression = str(expression or "").strip()
            if not expression or expression.lower() in ("(null)", "none", "unknown"):
                return []
            if expression in hostname_cache:
                return hostname_cache[expression]
            names = []
            if "[" in expression and "]" in expression:
                try:
                    result = subprocess.run(["scontrol", "show", "hostnames", expression], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=5)
                    if result.returncode == 0:
                        names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
                except (OSError, subprocess.SubprocessError):
                    names = []
            if not names:
                names = [part.strip() for part in expression.split(",") if part.strip()]
            hostname_cache[expression] = names
            return names

        def infer_from_nodes(nodelist, gpu_count):
            per_type = defaultdict(float)
            capacities = defaultdict(float)
            for node_name in expand_nodelist(nodelist):
                for model, capacity in node_models.get(node_name, {}).items():
                    capacities[model] += capacity
            if not capacities:
                return {}
            # If all allocated nodes expose one model, attribute all allocated GPUs
            # to it. For mixed-model node lists, distribute by known GPU capacity.
            if len(capacities) == 1:
                return {next(iter(capacities)): gpu_count}
            total_capacity = sum(capacities.values())
            if total_capacity <= 0:
                return {}
            for model, capacity in capacities.items():
                per_type[model] = gpu_count * capacity / total_capacity
            return dict(per_type)

        # Treat the current node inventory as the source of truth for model
        # labels. Historical AllocTRES can contain stale or erroneous model
        # names (for example, a GPU type not installed on this cluster); keep
        # the allocation hours but do not present an unsupported model label.
        known_gpu_models = {
            model.upper()
            for models in node_models.values()
            for model in models
        }

        type_jobs = defaultdict(set)
        type_gpus = defaultdict(float)
        type_gpu_hours = defaultdict(float)
        type_max = defaultdict(float)
        partition_jobs = defaultdict(set)
        partition_gpus = defaultdict(float)
        partition_gpu_hours = defaultdict(float)
        for row in frame.to_dicts():
            tres = str(row.get("alloctres") or "")
            matches = []
            unresolved_typed = 0.0
            for match in typed_pattern.finditer(tres):
                gpu_type = match.group(1).strip().upper()
                count = float(match.group(2))
                if gpu_type not in known_gpu_models:
                    unresolved_typed += count
                else:
                    matches.append((gpu_type, count))
            if unresolved_typed:
                inferred = infer_from_nodes(row.get("nodelist"), unresolved_typed)
                matches.extend(inferred.items() if inferred else [("Unspecified GPU", unresolved_typed)])
            if not matches:
                generic_matches = [float(m.group(1)) for m in generic_pattern.finditer(tres)]
                if generic_matches:
                    total_generic = sum(generic_matches)
                    inferred = infer_from_nodes(row.get("nodelist"), total_generic)
                    matches = list(inferred.items()) if inferred else [("Unspecified GPU", total_generic)]
            if not matches:
                continue
            per_type = defaultdict(float)
            for gpu_type, count in matches:
                per_type[gpu_type] += count
            elapsed_hours = max(float(row.get("elapsed_seconds") or 0), 0.0) / 3600.0
            job_id = str(row.get("job_id") or "")
            partitions = [p.strip() for p in str(row.get("partition_list") or "(Unknown partition)").split(",") if p.strip()]
            if not partitions:
                partitions = ["(Unknown partition)"]
            divisor = float(len(partitions))
            for gpu_type, count in per_type.items():
                type_jobs[gpu_type].add(job_id)
                type_gpus[gpu_type] += count
                type_gpu_hours[gpu_type] += count * elapsed_hours
                type_max[gpu_type] = max(type_max[gpu_type], count)
                for partition in partitions:
                    key = (partition, gpu_type)
                    partition_jobs[key].add(job_id)
                    partition_gpus[key] += count / divisor
                    partition_gpu_hours[key] += count * elapsed_hours / divisor

        types = []
        for gpu_type in sorted(type_jobs):
            n = len(type_jobs[gpu_type])
            types.append({"gpu_type": gpu_type, "jobs": n, "total_gpus": round(type_gpus[gpu_type], 3), "avg_gpus_per_job": round(type_gpus[gpu_type] / n, 3) if n else None, "max_gpus_per_job": type_max[gpu_type], "gpu_hours": round(type_gpu_hours[gpu_type], 3)})
        partition_types = []
        for (partition, gpu_type), job_ids in partition_jobs.items():
            n = len(job_ids)
            partition_types.append({"partition": partition, "gpu_type": gpu_type, "jobs": n, "total_gpus": round(partition_gpus[(partition, gpu_type)], 3), "avg_gpus_per_job": round(partition_gpus[(partition, gpu_type)] / n, 3) if n else None, "gpu_hours": round(partition_gpu_hours[(partition, gpu_type)], 3)})
        partition_types.sort(key=lambda r: (-r["gpu_hours"], r["partition"], r["gpu_type"]))
        return JsonResponse({"types": sorted(types, key=lambda r: (-r["gpu_hours"], r["gpu_type"])), "partition_types": partition_types}, safe=False)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@require_GET
def api_gpu_type_usage(request):
    """Return GPU-hours by model, loading the persisted inventory if needed.

    Explicit AllocTRES model names take precedence. Generic GPU counts are
    attributed using this cluster's stored node inventory; the endpoint does
    not invoke scontrol during a page request.
    """
    snapshot, response = _snapshot_or_response()
    if response:
        return response

    from datetime import datetime, timedelta, timezone
    from collections import defaultdict
    import re
    from slurm_analytics.storage import connect, load_node_inventory

    now = datetime.now(timezone.utc)
    start = request.GET.get("start") or (now - timedelta(days=7)).isoformat()
    end = request.GET.get("end") or now.isoformat()
    try:
        con = connect()
        try:
            cols = {r[0].lower() for r in con.execute("DESCRIBE jobs").fetchall()}
            tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
            account_rows = con.execute("SELECT account_name, parent_name, level, path FROM accounts").fetchall() if "accounts" in tables else []
        finally:
            con.close()

        required = {"alloctres", "start_time", "elapsed_seconds"}
        if not required.issubset(cols):
            return JsonResponse({"types": [], "users": [], "accounts": [], "note": "Required GPU accounting fields are not stored."})

        user_expr = "COALESCE(NULLIF(TRIM(CAST(\"user\" AS VARCHAR)), ''), '(Unknown user)')" if "user" in cols else "'(Unknown user)'"
        account_expr = "COALESCE(NULLIF(TRIM(CAST(account AS VARCHAR)), ''), 'Unknown')" if "account" in cols else "'Unknown'"
        nodelist_col = next((name for name in ("nodelist", "node_list", "nodes") if name in cols), None)
        nodelist_expr = "COALESCE(CAST(\"{}\" AS VARCHAR), '')".format(nodelist_col) if nodelist_col else "''"
        partition_expr = "COALESCE(CAST(partition AS VARCHAR), '')" if "partition" in cols else "''"
        state_expr = "lower(COALESCE(state_category, ''))" if "state_category" in cols else "''"
        rows = query_df(
            "SELECT {user} AS user_name, {account} AS account_name, alloctres, {elapsed} AS elapsed_seconds, {nodes} AS nodelist, {partition} AS partition_name "
            "FROM jobs WHERE TRY_CAST(start_time AS TIMESTAMPTZ) >= TRY_CAST(? AS TIMESTAMPTZ) "
            "AND TRY_CAST(start_time AS TIMESTAMPTZ) < TRY_CAST(? AS TIMESTAMPTZ) "
            "AND {state} NOT IN ('running','pending')".format(
                user=user_expr, account=account_expr, elapsed="TRY_CAST(elapsed_seconds AS DOUBLE)", nodes=nodelist_expr, partition=partition_expr, state=state_expr
            ), [start, end]
        )

        typed_pattern = re.compile(r"(?:gres/)?gpu:([^=,:]+)(?::[^=,]+)?=([0-9]+(?:\.[0-9]+)?)", re.I)
        generic_pattern = re.compile(r"(?:gres/)?gpu=([0-9]+(?:\.[0-9]+)?)", re.I)
        node_typed_pattern = re.compile(r"gpu:([^:,()\s]+):([0-9]+)(?=\(|,|\s|$)", re.I)

        def normalize_model(value):
            model = str(value or "").strip().upper().replace("_", " ")
            return re.sub(r"^RTX[ -]*(\d)", r"RTX \1", model)

        node_models = defaultdict(lambda: defaultdict(float))
        partition_models = defaultdict(lambda: defaultdict(float))
        cluster_types = set()
        nodes_df = getattr(snapshot, "nodes", None)
        if nodes_df is None or nodes_df.is_empty():
            # Node inventory is already persisted in this cluster's DuckDB.
            # Load that snapshot instead of classifying valid generic GPU jobs
            # as "Unspecified GPU" merely because a worker restarted.
            try:
                nodes_df = load_node_inventory()
                snapshot.nodes = nodes_df
            except Exception:
                nodes_df = None
        if nodes_df is not None and not nodes_df.is_empty():
            for node in nodes_df.to_dicts():
                lowered = {str(k).lower(): v for k, v in node.items()}
                node_name = str(lowered.get("node_name") or lowered.get("nodename") or "").strip()
                if not node_name:
                    continue
                gres = str(lowered.get("gres") or "")
                gres_used = str(lowered.get("gres_used") or "")
                partition_names = str(lowered.get("partition") or "").strip()
                matches = list(node_typed_pattern.finditer(gres))
                if not matches:
                    matches = list(node_typed_pattern.finditer(gres_used))
                for match in matches:
                    model = normalize_model(match.group(1))
                    capacity = float(match.group(2))
                    node_models[node_name.lower()][model] += capacity
                    for partition_name in partition_names.split(","):
                        partition_name = partition_name.strip()
                        if partition_name and partition_name not in ("(null)", "(none)"):
                            partition_models[partition_name][model] += capacity
                    cluster_types.add(model)

        # Expand Slurm hostlists locally only when generic GPU allocations need
        # model inference. Most typed AllocTRES rows do not need node processing.
        hostname_cache = {}
        def _expand_hostlist_piece(piece):
            match = re.search(r"\[([^\[\]]+)\]", piece)
            if not match:
                return [piece]
            prefix, body, suffix = piece[:match.start()], match.group(1), piece[match.end():]
            values = []
            for item in body.split(","):
                item = item.strip()
                if not item:
                    continue
                range_match = re.fullmatch(r"(\d+)-(\d+)(?::(\d+))?", item)
                if range_match:
                    first, last = int(range_match.group(1)), int(range_match.group(2))
                    step = int(range_match.group(3) or 1)
                    if step < 1 or last < first or last - first > 100000:
                        continue
                    width = max(len(range_match.group(1)), len(range_match.group(2)))
                    values.extend("{}{}{}".format(prefix, str(number).zfill(width), suffix) for number in range(first, last + 1, step))
                else:
                    values.append("{}{}{}".format(prefix, item, suffix))
            expanded = []
            for value in values:
                if "[" in value and "]" in value:
                    expanded.extend(_expand_hostlist_piece(value))
                else:
                    expanded.append(value)
            return expanded

        def expand_nodelist(expression):
            expression = str(expression or "").strip()
            if not expression or expression.lower() in ("(null)", "none", "unknown"):
                return []
            if expression in hostname_cache:
                return hostname_cache[expression]
            parts, current, depth = [], [], 0
            for char in expression:
                if char == "[":
                    depth += 1
                elif char == "]":
                    depth = max(0, depth - 1)
                if char == "," and depth == 0:
                    parts.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            if current:
                parts.append("".join(current).strip())
            names = []
            for part in parts:
                if part:
                    names.extend(_expand_hostlist_piece(part))
            hostname_cache[expression] = names
            return names

        def infer_models(nodelist, gpu_count, partition_name=""):
            capacities = defaultdict(float)
            for node_name in expand_nodelist(nodelist):
                for model, capacity in node_models.get(node_name.lower(), {}).items():
                    capacities[model] += capacity
            if not capacities and partition_name:
                for part in str(partition_name).split(","):
                    for model, capacity in partition_models.get(part.strip(), {}).items():
                        capacities[model] += capacity
            if not capacities and len(cluster_types) == 1:
                return {next(iter(cluster_types)): gpu_count}
            if not capacities:
                return {}
            if len(capacities) == 1:
                return {next(iter(capacities)): gpu_count}
            total = sum(capacities.values())
            return {model: gpu_count * cap / total for model, cap in capacities.items()} if total > 0 else {}

        # Do not expose a historical AllocTRES model as an installed model
        # unless it is present in the current node inventory. Keep its hours
        # under a neutral label so totals are retained without inventing a GPU.
        known_gpu_models = {model.upper() for model in cluster_types}

        user_hours = defaultdict(float)
        account_direct_hours = defaultdict(float)
        # Convert the Polars result once. Parse typed allocations first and only
        # expand a hostlist for a row that needs generic-GPU model inference.
        for row in rows.to_dicts():
            tres = str(row.get("alloctres") or "")
            allocations = defaultdict(float)
            unresolved_typed = 0.0
            for match in typed_pattern.finditer(tres):
                model = normalize_model(match.group(1))
                count = float(match.group(2))
                if model.upper() not in known_gpu_models:
                    # Old/stale AllocTRES model names should be resolved from
                    # the actual node allocation before using a generic label.
                    unresolved_typed += count
                else:
                    allocations[model] += count
            if unresolved_typed:
                inferred = infer_models(row.get("nodelist"), unresolved_typed, row.get("partition_name"))
                if inferred:
                    for model, count in inferred.items():
                        allocations[model] += count
                else:
                    allocations["Unspecified GPU"] += unresolved_typed
            if not allocations:
                generic = sum(float(m.group(1)) for m in generic_pattern.finditer(tres))
                if generic > 0:
                    inferred = infer_models(row.get("nodelist"), generic, row.get("partition_name"))
                    if inferred:
                        allocations.update(inferred)
                    else:
                        allocations["Unspecified GPU"] += generic
            if not allocations:
                continue
            hours = max(float(row.get("elapsed_seconds") or 0), 0.0) / 3600.0
            user_name = str(row.get("user_name") or "(Unknown user)")
            account_name = str(row.get("account_name") or "Unknown")
            for model, count in allocations.items():
                cluster_types.add(model)
                value = count * hours
                user_hours[(user_name, model)] += value
                account_direct_hours[(account_name, model)] += value

        parents = {str(name): (str(parent) if parent else None) for name, parent, level, path in account_rows if name}
        levels = {str(name): int(level or 0) for name, parent, level, path in account_rows if name}
        rolled = defaultdict(float)
        for (account_name, model), value in account_direct_hours.items():
            rolled[(account_name, model)] += value
            parent = parents.get(account_name)
            seen = {account_name}
            while parent and parent not in seen:
                seen.add(parent)
                rolled[(parent, model)] += value
                parent = parents.get(parent)

        users = [{"user": name, "gpu_type": model, "gpu_hours": round(value, 4)}
                 for (name, model), value in sorted(user_hours.items())]
        accounts = [{"account": name, "parent": parents.get(name), "level": levels.get(name, 0), "gpu_type": model, "gpu_hours": round(value, 4)}
                    for (name, model), value in sorted(rolled.items())]
        return JsonResponse({"types": sorted(cluster_types), "users": users, "accounts": accounts}, safe=False)
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
    """Return bucketed time-series data from DuckDB for the selected time range."""
    from datetime import datetime, timedelta, timezone

    interval = request.GET.get("interval", "1h")
    include_details = request.GET.get("details", "0").lower() in ("1", "true", "yes")
    end = request.GET.get("end") or datetime.now(timezone.utc).isoformat()
    start = request.GET.get("start") or (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

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
                    CAST(j.state AS VARCHAR) AS state,
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
                    "state": pl.String,
                    "ncpus": pl.Float64,
                    "gpu_count": pl.Float64,
                    "allocated_memory_bytes": pl.Float64,
                    "max_rss_bytes": pl.Float64,
                    "elapsed_seconds": pl.Float64,
                    "time_limit_seconds": pl.Float64,
                },
            )
            # job_steps can contain millions of records. It is only needed for
            # recorded-MaxRSS enrichment on the optional CPU/GPU/RAM/runtime
            # charts, so do not scan it for the default Overview request.
            if include_details:
                # Parse MaxRSS values from job-step rows in Python instead of
                # relying on SQL regexp behavior across DuckDB versions.
                import re
                step_rows = query_df(
                    """
                    SELECT s.canonical_job_id, s.maxrss
                    FROM job_steps s
                    INNER JOIN jobs j
                      ON CAST(s.canonical_job_id AS VARCHAR) = CAST(j.canonical_job_id AS VARCHAR)
                    WHERE s.maxrss IS NOT NULL
                      AND ((j.start_time IS NOT NULL
                            AND j.start_time < TRY_CAST(? AS TIMESTAMPTZ)
                            AND (j.end_time IS NULL OR j.end_time > TRY_CAST(? AS TIMESTAMPTZ)))
                        OR (j.submit_time >= TRY_CAST(? AS TIMESTAMPTZ)
                            AND j.submit_time < TRY_CAST(? AS TIMESTAMPTZ)))
                    """,
                    [requested_end, requested_start, requested_start, requested_end],
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
            from slurm_analytics.timeseries import job_wait_time_summary, job_wait_time_series
            waiting_summary = job_wait_time_summary(
                historical_jobs, start=requested_start, end=requested_end
            )
            waiting_series = job_wait_time_series(
                historical_jobs, start=requested_start, end=requested_end, interval=interval
            )
            result = time_series(
                historical_jobs,
                start=requested_start,
                end=requested_end,
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
        "waiting_summary": waiting_summary,
        "waiting_series": waiting_series,
        "details_included": include_details,
    })


@require_GET
def api_queue(request):
    """Return the live queue from squeue; remote file-import clusters have no local queue."""
    cluster = getattr(request, "slurm_cluster", {})
    if cluster.get("mode", "live") != "live":
        return JsonResponse({"status": "unavailable", "error": "Live queue is disabled for file-import clusters."}, status=503)
    try:
        df = fetch_squeue()
    except Exception as exc:
        return JsonResponse({"status": "error", "error": str(exc)}, status=502)
    return JsonResponse({"count": df.height, "results": dataframe_to_records(df)}, safe=False)


@require_POST
def api_refresh(request):
    """Build and atomically publish a new snapshot for live-connected clusters."""
    cluster = getattr(request, "slurm_cluster", {})
    if cluster.get("mode", "live") != "live":
        return JsonResponse({"status": "unavailable", "error": "Live refresh is disabled for file-import clusters; import updated history files instead."}, status=403)
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
def api_resource_efficiency(request):
    """Return per-job CPU/RAM/GPU efficiency summaries for users and account levels."""
    from slurm_analytics.efficiency import resource_efficiency
    try:
        include_jobs = str(request.GET.get("include_jobs", "0")).lower() in ("1", "true", "yes")
        waiting_only = str(request.GET.get("waiting_only", "0")).lower() in ("1", "true", "yes")
        limit = max(1, min(int(request.GET.get("limit", "10000")), 50000))
        result = resource_efficiency(
            request.GET.get("start"), request.GET.get("end"),
            request.GET.get("user", ""), request.GET.get("account", ""),
            include_jobs=include_jobs, limit=limit, waiting_only=waiting_only,
        )
        return JsonResponse(result, safe=True)
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
