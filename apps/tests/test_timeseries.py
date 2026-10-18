from datetime import datetime, timezone

import polars as pl

from slurm_analytics.analytics import time_series

UTC = timezone.utc


def make_jobs():
    return pl.DataFrame(
        {
            "start_time": [
                datetime(2026, 10, 1, 0, 15, tzinfo=UTC),
                datetime(2026, 10, 1, 1, 10, tzinfo=UTC),
                datetime(2026, 10, 1, 1, 40, tzinfo=UTC),
            ],
            "end_time": [
                datetime(2026, 10, 1, 1, 20, tzinfo=UTC),
                datetime(2026, 10, 1, 2, 10, tzinfo=UTC),
                datetime(2026, 10, 1, 2, 0, tzinfo=UTC),
            ],
            "state_category": ["completed", "completed", "failed"],
            "ncpus": [4, 8, 2],
            "gpu_count": [0, 1, 2],
        }
    )


def test_time_series_has_expected_buckets():
    result = time_series(
        make_jobs(),
        start="2026-10-01T00:00:00+00:00",
        end="2026-10-01T03:00:00+00:00",
        interval="1h",
    )

    assert result.height == 3
    rows = result.to_dicts()
    assert rows[0]["jobs_started"] == 1
    assert rows[1]["jobs_started"] == 2
    assert rows[1]["active_job_count"] == 2
    assert rows[1]["allocated_cpus"] == 10
    assert rows[1]["allocated_gpus"] == 3
    assert rows[2]["active_job_count"] == 0
    assert rows[2]["jobs_completed"] == 1
    assert rows[2]["jobs_failed"] == 1


def test_time_series_rejects_unknown_interval():
    try:
        time_series(make_jobs(), interval="5m")
    except ValueError as exc:
        assert "Unsupported interval" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_week_bucket_uses_monday_calendar_boundaries():
    from slurm_analytics.timeseries import _bucket_floor, _bucket_next

    value = datetime(2026, 10, 15, 12, tzinfo=UTC)  # Thursday
    start = _bucket_floor(value, "1w")
    assert start == datetime(2026, 10, 12, tzinfo=UTC)
    assert _bucket_next(start, "1w") == datetime(2026, 10, 19, tzinfo=UTC)


def test_month_bucket_uses_calendar_month_boundaries():
    from slurm_analytics.timeseries import _bucket_floor, _bucket_next

    value = datetime(2026, 12, 31, 23, tzinfo=UTC)
    start = _bucket_floor(value, "1mo")
    assert start == datetime(2026, 12, 1, tzinfo=UTC)
    assert _bucket_next(start, "1mo") == datetime(2027, 1, 1, tzinfo=UTC)


def test_terminal_events_include_cancelled_jobs_without_start_time():
    from slurm_analytics.analytics import time_series

    jobs = pl.DataFrame(
        {
            "submit_time": [
                datetime(2026, 10, 1, 0, 10, tzinfo=UTC),
                datetime(2026, 10, 1, 1, 10, tzinfo=UTC),
                datetime(2026, 10, 1, 1, 20, tzinfo=UTC),
            ],
            "start_time": [None, None, datetime(2026, 10, 1, 1, 25, tzinfo=UTC)],
            "end_time": [
                datetime(2026, 10, 1, 1, 15, tzinfo=UTC),
                datetime(2026, 10, 1, 2, 15, tzinfo=UTC),
                datetime(2026, 10, 1, 2, 30, tzinfo=UTC),
            ],
            "state_category": ["cancelled", "other", "completed"],
            "ncpus": [0, 0, 4],
            "gpu_count": [0, 0, 0],
        }
    )

    result = time_series(
        jobs,
        start="2026-10-01T00:00:00+00:00",
        end="2026-10-01T03:00:00+00:00",
        interval="1h",
    ).to_dicts()

    # Pending jobs can be cancelled or otherwise terminated without ever
    # acquiring a start_time; their end events must still be counted.
    assert result[1]["jobs_cancelled"] == 1
    assert result[2]["jobs_other_terminal"] == 1
    assert result[2]["jobs_completed"] == 1


def test_active_jobs_without_end_time_are_not_counted_as_terminal():
    from slurm_analytics.analytics import time_series

    jobs = pl.DataFrame(
        {
            "submit_time": [datetime(2026, 10, 1, 0, 10, tzinfo=UTC)],
            "start_time": [datetime(2026, 10, 1, 0, 20, tzinfo=UTC)],
            "end_time": [None],
            "state_category": ["running"],
            "ncpus": [4],
            "gpu_count": [0],
        }
    )

    result = time_series(
        jobs,
        start="2026-10-01T00:00:00+00:00",
        end="2026-10-01T02:00:00+00:00",
        interval="1h",
    ).to_dicts()

    assert sum(row["jobs_completed"] for row in result) == 0
    assert sum(row["jobs_failed"] for row in result) == 0
    assert sum(row["jobs_cancelled"] for row in result) == 0
    assert sum(row["jobs_other_terminal"] for row in result) == 0


def test_timeout_events_are_separate_from_failed_events():
    from slurm_analytics.analytics import time_series

    jobs = pl.DataFrame(
        {
            "submit_time": [
                datetime(2026, 10, 1, 0, 5, tzinfo=UTC),
                datetime(2026, 10, 1, 0, 10, tzinfo=UTC),
            ],
            "start_time": [
                datetime(2026, 10, 1, 0, 15, tzinfo=UTC),
                datetime(2026, 10, 1, 0, 20, tzinfo=UTC),
            ],
            "end_time": [
                datetime(2026, 10, 1, 1, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 1, 30, tzinfo=UTC),
            ],
            # Simulate a legacy DB whose normalized category still labels
            # TIMEOUT as failed; raw Slurm state must take precedence.
            "state_category": ["failed", "failed"],
            "state": ["TIMEOUT", "FAILED"],
            "ncpus": [4, 4],
            "gpu_count": [0, 0],
        }
    )

    rows = time_series(
        jobs,
        start="2026-10-01T00:00:00+00:00",
        end="2026-10-01T02:00:00+00:00",
        interval="1h",
    ).to_dicts()
    assert rows[1]["jobs_timeout"] == 1
    assert rows[1]["jobs_failed"] == 1


def test_wait_time_summary_uses_jobs_submitted_in_range():
    from slurm_analytics.timeseries import job_wait_time_summary

    jobs = pl.DataFrame(
        {
            "submit_time": [
                datetime(2026, 10, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 0, 30, tzinfo=UTC),
                datetime(2026, 9, 30, 23, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 0, 40, tzinfo=UTC),
            ],
            "start_time": [
                datetime(2026, 10, 1, 1, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 2, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 0, 0, tzinfo=UTC),
                None,
            ],
        }
    )

    summary = job_wait_time_summary(
        jobs,
        start="2026-10-01T00:00:00+00:00",
        end="2026-10-01T03:00:00+00:00",
    )
    assert summary["count"] == 2
    assert summary["mean_seconds"] == 4500
    assert summary["max_seconds"] == 5400


def test_wait_time_series_groups_by_submission_bucket():
    from slurm_analytics.timeseries import job_wait_time_series

    jobs = pl.DataFrame(
        {
            "submit_time": [
                datetime(2026, 10, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 2, 0, tzinfo=UTC),
                datetime(2026, 10, 2, 0, 0, tzinfo=UTC),
                datetime(2026, 10, 3, 0, 0, tzinfo=UTC),
            ],
            "start_time": [
                datetime(2026, 10, 1, 1, 0, tzinfo=UTC),
                datetime(2026, 10, 1, 4, 0, tzinfo=UTC),
                datetime(2026, 10, 2, 3, 0, tzinfo=UTC),
                None,
            ],
        }
    )
    rows = job_wait_time_series(
        jobs,
        start="2026-10-01T00:00:00+00:00",
        end="2026-10-04T00:00:00+00:00",
        interval="1d",
    )
    assert len(rows) == 3
    assert rows[0]["count"] == 2
    assert rows[0]["mean_seconds"] == 5400
    assert rows[0]["max_seconds"] == 7200
    assert rows[1]["count"] == 1
    assert rows[1]["mean_seconds"] == 10800
    assert rows[2]["count"] == 0
    assert rows[2]["mean_seconds"] is None


def test_wait_time_series_supports_week_and_month_buckets():
    from slurm_analytics.timeseries import job_wait_time_series

    jobs = pl.DataFrame(
        {
            "submit_time": [datetime(2026, 10, 1, tzinfo=UTC)],
            "start_time": [datetime(2026, 10, 1, 1, tzinfo=UTC)],
        }
    )
    common = {
        "start": "2026-09-01T00:00:00+00:00",
        "end": "2026-11-01T00:00:00+00:00",
    }
    weekly = job_wait_time_series(jobs, interval="1w", **common)
    monthly = job_wait_time_series(jobs, interval="1mo", **common)
    assert len(weekly) >= 8
    assert len(monthly) == 2
    assert sum(row["count"] for row in weekly) == 1
    assert sum(row["count"] for row in monthly) == 1
