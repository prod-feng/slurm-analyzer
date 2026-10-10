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
}


def _bucket_floor(value, interval):
    """Floor an aware datetime to a fixed UTC interval."""
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    seconds = int((value - epoch).total_seconds())
    width = int(interval.total_seconds())
    return epoch + timedelta(seconds=(seconds // width) * width)


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

    bucket_delta = _INTERVALS[interval]
    first_bucket = _bucket_floor(requested_start, bucket_delta)
    last_bucket = _bucket_floor(requested_end - timedelta(microseconds=1), bucket_delta)

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
            submit_bucket = _bucket_floor(submitted_at, bucket_delta)
            submissions[submit_bucket] = submissions.get(submit_bucket, 0) + 1

        job_start = _parse_datetime(row.get("start_time"))
        if job_start is None:
            continue

        job_end = _parse_datetime(row.get("end_time")) or now
        if job_end <= requested_start or job_start >= requested_end:
            continue

        clipped_start = max(job_start, requested_start)
        clipped_end = min(job_end, requested_end)
        if clipped_end <= clipped_start:
            continue

        start_bucket = _bucket_floor(clipped_start, bucket_delta)
        end_bucket = _bucket_floor(clipped_end, bucket_delta)

        cpu = _numeric(row.get("ncpus"))
        gpu = _numeric(row.get("gpu_count"))
        state = str(row.get("state_category") or "other")

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
            if state == "completed":
                terminals[end_bucket] = terminals.get(end_bucket, [0, 0, 0])
                terminals[end_bucket][0] += 1
            elif state == "failed":
                terminals[end_bucket] = terminals.get(end_bucket, [0, 0, 0])
                terminals[end_bucket][1] += 1

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

        terminal = terminals.get(timestamp, [0, 0, 0])
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
                "jobs_submitted": submissions.get(timestamp, 0),
                "avg_allocated_cpus_per_job": _mean(allocation_samples.get(timestamp, {}).get("cpus", [])),
                "avg_allocated_gpus_per_job": _mean(allocation_samples.get(timestamp, {}).get("gpus", [])),
                "avg_requested_memory_gib_per_job": _mean(allocation_samples.get(timestamp, {}).get("requested_memory", [])),
                "avg_recorded_memory_gib_per_job": _mean(allocation_samples.get(timestamp, {}).get("recorded_memory", [])),
                "avg_elapsed_runtime_seconds": _mean(runtime_samples.get(timestamp, {}).get("elapsed", [])),
                "avg_time_limit_seconds": _mean(runtime_samples.get(timestamp, {}).get("limit", [])),
            }
        )
        timestamp += bucket_delta

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
