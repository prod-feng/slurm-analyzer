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
