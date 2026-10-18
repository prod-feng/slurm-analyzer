"""Time-series analytics over normalized Slurm job allocations.

The time series is event-based rather than a sampled snapshot.  Each row
represents the allocation state at the beginning of a bucket, plus jobs that
started/finished during that bucket.  This keeps the calculation efficient
for large job histories while preserving the important resource trend.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

UTC = timezone.utc


_INTERVALS = {
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    # Calendar-based buckets; these are floored/advanced by helper functions.
    "1w": None,
    "1mo": None,
}


def _bucket_floor(value, interval):
    """Floor an aware datetime to a fixed or calendar UTC interval."""
    if interval == "1w":
        day = value.astimezone(UTC).date()
        monday = day - timedelta(days=day.weekday())
        return datetime(monday.year, monday.month, monday.day, tzinfo=UTC)
    if interval == "1mo":
        value = value.astimezone(UTC)
        return datetime(value.year, value.month, 1, tzinfo=UTC)
    delta = _INTERVALS[interval]
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    seconds = int((value - epoch).total_seconds())
    width = int(delta.total_seconds())
    return epoch + timedelta(seconds=(seconds // width) * width)


def _bucket_next(value, interval):
    """Return the next bucket boundary (calendar-aware for week/month)."""
    if interval == "1w":
        return value + timedelta(days=7)
    if interval == "1mo":
        if value.month == 12:
            return datetime(value.year + 1, 1, 1, tzinfo=UTC)
        return datetime(value.year, value.month + 1, 1, tzinfo=UTC)
    return value + _INTERVALS[interval]


def _parse_datetime(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _numeric(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _empty_frame():
    return pl.DataFrame(
        schema={
            "timestamp": pl.Datetime(time_zone="UTC"),
            "active_job_count": pl.Int64,
            "running_job_count": pl.Int64,
            "allocated_cpus": pl.Float64,
            "allocated_gpus": pl.Float64,
            "jobs_started": pl.Int64,
            "jobs_completed": pl.Int64,
            "jobs_failed": pl.Int64,
            "jobs_timeout": pl.Int64,
            "jobs_cancelled": pl.Int64,
            "jobs_other_terminal": pl.Int64,
            "jobs_submitted": pl.Int64,
            "avg_allocated_cpus_per_job": pl.Float64,
            "avg_allocated_gpus_per_job": pl.Float64,
            "avg_requested_memory_gib_per_job": pl.Float64,
            "avg_recorded_memory_gib_per_job": pl.Float64,
            "avg_elapsed_runtime_seconds": pl.Float64,
            "avg_time_limit_seconds": pl.Float64,
        }
    )




def _mean(values):
    return (sum(values) / len(values)) if values else None


def _parse_memory_bytes(value):
    """Parse a Slurm MaxRSS value (e.g. 1200K, 3G); return None if absent."""
    if value is None:
        return None
    import re
    match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*[cCnN]?\s*$", str(value), re.I)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).upper()
    powers = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
    return number * (1024.0 ** powers[unit])


def job_time_series(df, start=None, end=None, interval="1h"):
    """Build an allocation time series from one-row-per-job records.

    ``timestamp`` is the state at the start of each bucket.  ``jobs_started``
    and terminal-state counters describe events occurring in that bucket.
    Jobs with no usable start time are excluded.
    """
    if interval not in _INTERVALS:
        raise ValueError(
            "Unsupported interval {!r}; choose one of {}".format(
                interval, ", ".join(_INTERVALS)
            )
        )

    if df is None or df.is_empty():
        return _empty_frame()

    if "start_time" not in df.columns:
        return _empty_frame()

    now = datetime.now(UTC)
    requested_start = _parse_datetime(start) if start else None
    requested_end = _parse_datetime(end) if end else None
    requested_end = requested_end or now

    if requested_start is None:
        # Default to the earliest usable job start, but cap an implicit
        # all-history request to 30 days. Explicit start/end ranges are
        # allowed to span the full persistent history.
        values = df.get_column("start_time").drop_nulls().to_list()
        if not values:
            return _empty_frame()
        requested_start = max(
            min(_parse_datetime(v) for v in values if _parse_datetime(v)),
            requested_end - timedelta(days=30),
        )

    if requested_end <= requested_start:
        raise ValueError("end must be later than start")

    first_bucket = _bucket_floor(requested_start, interval)
    last_bucket = _bucket_floor(requested_end - timedelta(microseconds=1), interval)

    # Event deltas: resources/jobs become active at start and inactive at end.
    events = {}
    starts = {}
    submissions = {}
    terminals = {}
    allocation_samples = {}
    runtime_samples = {}

    columns = set(df.columns)
    rows = df.select(
        [
            c for c in [
                "submit_time",
                "start_time",
                "end_time",
                "state_category",
                "state",
                "ncpus",
                "gpu_count",
                "allocated_memory_bytes",
                # The historical API enriches each job with parsed MaxRSS from
                # job-step accounting. Include that field here; otherwise the
                # value is silently dropped and the Average recorded MaxRSS
                # series stays empty even when step telemetry exists.
                "max_rss_bytes",
                "maxrss",
                "elapsed_seconds",
                "time_limit_seconds",
            ] if c in columns
        ]
    ).to_dicts()

    for row in rows:
        submitted_at = _parse_datetime(row.get("submit_time"))
        if submitted_at is not None and requested_start <= submitted_at < requested_end:
            submit_bucket = _bucket_floor(submitted_at, interval)
            submissions[submit_bucket] = submissions.get(submit_bucket, 0) + 1

        # Count terminal events from end_time independently of start_time.
        # Slurm jobs can be cancelled while pending and therefore have no
        # start_time; skipping those rows here used to silently omit them
        # from the event-count chart. Only a real end_time denotes a terminal
        # event; the synthetic `now` below is for active-allocation tracking.
        state = str(row.get("state_category") or "other").strip().lower()
        raw_state = str(row.get("state") or "").strip().upper()
        # Databases imported before timeout became its own normalized category
        # still have the original Slurm state. Use it to split TIMEOUT records
        # without requiring users to re-import their history.
        if raw_state.startswith("TIMEOUT"):
            state = "timeout"
        recorded_end = _parse_datetime(row.get("end_time"))
        if recorded_end is not None and requested_start <= recorded_end < requested_end:
            terminal_bucket = _bucket_floor(recorded_end, interval)
            terminal = terminals.setdefault(terminal_bucket, [0, 0, 0, 0, 0])
            if state == "completed":
                terminal[0] += 1
            elif state == "timeout":
                terminal[2] += 1
            elif state == "failed":
                terminal[1] += 1
            elif state == "cancelled":
                terminal[3] += 1
            else:
                terminal[4] += 1

        job_start = _parse_datetime(row.get("start_time"))
        if job_start is None:
            continue

        job_end = recorded_end or now
        if job_end <= requested_start or job_start >= requested_end:
            continue

        clipped_start = max(job_start, requested_start)
        clipped_end = min(job_end, requested_end)
        if clipped_end <= clipped_start:
            continue

        start_bucket = _bucket_floor(clipped_start, interval)
        end_bucket = _bucket_floor(clipped_end, interval)

        cpu = _numeric(row.get("ncpus"))
        gpu = _numeric(row.get("gpu_count"))
        start_event = events.setdefault(start_bucket, [0, 0, 0.0, 0.0])
        start_event[0] += 1
        # A started allocation is running regardless of its final Slurm state.
        # The final state describes how the allocation ended, not how it was
        # running during its lifetime.
        start_event[1] += 1
        start_event[2] += cpu
        start_event[3] += gpu

        if clipped_end < requested_end:
            end_event = events.setdefault(end_bucket, [0, 0, 0.0, 0.0])
            end_event[0] -= 1
            end_event[1] -= 1
            end_event[2] -= cpu
            end_event[3] -= gpu

        if job_start >= requested_start:
            starts[start_bucket] = starts.get(start_bucket, 0) + 1

        if job_start >= requested_start and job_start < requested_end:
            sample = allocation_samples.setdefault(start_bucket, {
                "cpus": [], "gpus": [], "requested_memory": [], "recorded_memory": []
            })
            sample["cpus"].append(cpu)
            sample["gpus"].append(gpu)
            memory_bytes = row.get("allocated_memory_bytes")
            if memory_bytes is not None:
                try:
                    sample["requested_memory"].append(float(memory_bytes) / (1024.0 ** 3))
                except (TypeError, ValueError):
                    pass
            rss_bytes = _parse_memory_bytes(row.get("max_rss_bytes", row.get("maxrss")))
            if rss_bytes is not None:
                sample["recorded_memory"].append(rss_bytes / (1024.0 ** 3))

        if job_end <= requested_end and job_end > requested_start:
            if state in ("completed", "failed", "cancelled", "other"):
                elapsed = row.get("elapsed_seconds")
                limit = row.get("time_limit_seconds")
                sample = runtime_samples.setdefault(end_bucket, {"elapsed": [], "limit": []})
                if elapsed is not None:
                    try: sample["elapsed"].append(float(elapsed))
                    except (TypeError, ValueError): pass
                if limit is not None:
                    try: sample["limit"].append(float(limit))
                    except (TypeError, ValueError): pass

    result = []
    active_jobs = 0
    running_jobs = 0
    allocated_cpus = 0.0
    allocated_gpus = 0.0
    timestamp = first_bucket

    while timestamp <= last_bucket:
        delta = events.get(timestamp)
        if delta:
            active_jobs += delta[0]
            running_jobs += delta[1]
            allocated_cpus += delta[2]
            allocated_gpus += delta[3]

        terminal = terminals.get(timestamp, [0, 0, 0, 0, 0])
        result.append(
            {
                "timestamp": timestamp,
                "active_job_count": max(0, active_jobs),
                "running_job_count": max(0, running_jobs),
                "allocated_cpus": max(0.0, allocated_cpus),
                "allocated_gpus": max(0.0, allocated_gpus),
                "jobs_started": starts.get(timestamp, 0),
                "jobs_completed": terminal[0],
                "jobs_failed": terminal[1],
                "jobs_timeout": terminal[2],
                "jobs_cancelled": terminal[3],
                "jobs_other_terminal": terminal[4],
                "jobs_submitted": submissions.get(timestamp, 0),
                "avg_allocated_cpus_per_job": _mean(allocation_samples.get(timestamp, {}).get("cpus", [])),
                "avg_allocated_gpus_per_job": _mean(allocation_samples.get(timestamp, {}).get("gpus", [])),
                "avg_requested_memory_gib_per_job": _mean(allocation_samples.get(timestamp, {}).get("requested_memory", [])),
                "avg_recorded_memory_gib_per_job": _mean(allocation_samples.get(timestamp, {}).get("recorded_memory", [])),
                "avg_elapsed_runtime_seconds": _mean(runtime_samples.get(timestamp, {}).get("elapsed", [])),
                "avg_time_limit_seconds": _mean(runtime_samples.get(timestamp, {}).get("limit", [])),
            }
        )
        timestamp = _bucket_next(timestamp, interval)

    # Do not rely on Polars' default schema inference here.  The first
    # 100 buckets can contain only nulls (or whole-number-looking values) for
    # a metric, while a later bucket may contain a float such as 96.0.  That
    # causes a runtime error when Polars tries to append the later value to
    # a column inferred as Int64.  Reuse the canonical schema from the empty
    # frame so every bucket has stable dtypes, including all-null series.
    # Build columns independently with explicit dtypes.  Some supported
    # Polars versions still attempt row-wise inference for dict records even
    # when a schema is supplied, which can fail when an early value is null
    # or integral and a later value is a float (for example 96.0).
    schema = _empty_frame().schema
    return pl.DataFrame({
        name: pl.Series(
            name,
            [row.get(name) for row in result],
            dtype=dtype,
            strict=False,
        )
        for name, dtype in schema.items()
    })


def job_wait_time_summary(df, start=None, end=None):
    """Summarize queue wait for jobs submitted in the selected time range.

    Wait is ``start_time - submit_time``. Jobs that have not started, have
    invalid timestamps, or have negative waits are excluded because their
    completed queue wait cannot be measured reliably yet.
    """
    if df is None or df.is_empty() or not {"submit_time", "start_time"}.issubset(df.columns):
        return {"count": 0, "mean_seconds": None, "max_seconds": None}

    now = datetime.now(UTC)
    requested_end = _parse_datetime(end) if end else now
    requested_start = _parse_datetime(start) if start else requested_end - timedelta(days=30)
    if requested_start is None or requested_end is None or requested_end <= requested_start:
        return {"count": 0, "mean_seconds": None, "max_seconds": None}

    waits = []
    for row in df.select(["submit_time", "start_time"]).to_dicts():
        submitted_at = _parse_datetime(row.get("submit_time"))
        started_at = _parse_datetime(row.get("start_time"))
        if submitted_at is None or started_at is None:
            continue
        if not (requested_start <= submitted_at < requested_end):
            continue
        if started_at >= requested_end:
            continue
        wait_seconds = (started_at - submitted_at).total_seconds()
        if wait_seconds >= 0:
            waits.append(wait_seconds)

    return {
        "count": len(waits),
        "mean_seconds": _mean(waits),
        "max_seconds": max(waits) if waits else None,
    }

def job_wait_time_series(df, start=None, end=None, interval="1d"):
    """Return bucketed mean and maximum queue wait for jobs submitted in range.

    Each job is assigned to a bucket by its submission timestamp. Wait time is
    ``start_time - submit_time``. Jobs that have not started, have invalid
    timestamps, start outside the requested window, or have negative waits are
    excluded. Empty buckets are included with null statistics to keep the time
    axis continuous.
    """
    if interval not in _INTERVALS:
        raise ValueError(
            "Unsupported interval {!r}; choose one of {}".format(
                interval, ", ".join(_INTERVALS)
            )
        )

    requested_end = _parse_datetime(end) if end else datetime.now(UTC)
    requested_start = _parse_datetime(start) if start else requested_end - timedelta(days=30)
    if requested_start is None or requested_end is None or requested_end <= requested_start:
        return []

    buckets = {}
    if df is not None and not df.is_empty() and {"submit_time", "start_time"}.issubset(df.columns):
        for row in df.select(["submit_time", "start_time"]).to_dicts():
            submitted_at = _parse_datetime(row.get("submit_time"))
            started_at = _parse_datetime(row.get("start_time"))
            if submitted_at is None or started_at is None:
                continue
            if not (requested_start <= submitted_at < requested_end):
                continue
            if started_at >= requested_end:
                continue
            wait_seconds = (started_at - submitted_at).total_seconds()
            if wait_seconds < 0:
                continue
            bucket = _bucket_floor(submitted_at, interval)
            buckets.setdefault(bucket, []).append(wait_seconds)

    result = []
    timestamp = _bucket_floor(requested_start, interval)
    while timestamp < requested_end:
        values = buckets.get(timestamp, [])
        result.append({
            "timestamp": timestamp.isoformat(),
            "count": len(values),
            "mean_seconds": _mean(values),
            "max_seconds": max(values) if values else None,
        })
        timestamp = _bucket_next(timestamp, interval)
    return result

