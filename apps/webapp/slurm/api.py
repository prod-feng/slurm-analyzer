"""HTTP API for the current Slurm analytics snapshot."""

from __future__ import absolute_import

from django.http import JsonResponse
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
                    CAST(start_time AS VARCHAR) AS start_time,
                    CAST(end_time AS VARCHAR) AS end_time,
                    state_category,
                    ncpus,
                    gpu_count
                FROM jobs
                WHERE start_time IS NOT NULL
                  AND start_time < ?
                  AND (end_time IS NULL OR end_time > ?)
                """,
                [requested_end, requested_start],
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
