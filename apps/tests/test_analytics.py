# tests/test_analytics.py

import polars as pl

from slurm_analytics.analytics import (
    analyze_jobs,
    summarize_jobs,
    summarize_partitions,
    summarize_users,
)


def make_jobs():

    return pl.DataFrame(
        {
            "canonical_job_id": [
                "1",
                "2",
                "3",
                "4",
            ],
            "user": [
                "alice",
                "alice",
                "bob",
                "bob",
            ],
            "partition": [
                "gpu",
                "gpu",
                "cpu",
                "cpu",
            ],
            "state": [
                "COMPLETED",
                "FAILED",
                "RUNNING",
                "PENDING",
            ],
            "state_category": [
                "completed",
                "failed",
                "running",
                "pending",
            ],
            "elapsed_seconds": [
                100,
                200,
                300,
                0,
            ],
            "ncpus": [
                8,
                8,
                4,
                4,
            ],
            "gpu_count": [
                2.0,
                1.0,
                None,
                None,
            ],
            "gpu_util_min": [
                20.0,
                None,
                None,
                None,
            ],
            "gpu_util_max": [
                80.0,
                None,
                None,
                None,
            ],
            "gpu_mem_max_bytes": [
                1024,
                None,
                None,
                None,
            ],
        }
    )


def test_summary():

    result = summarize_jobs(
        make_jobs()
    ).row(
        0,
        named=True,
    )

    assert result["job_count"] == 4
    assert result["running_count"] == 1
    assert result["pending_count"] == 1
    assert result["completed_count"] == 1
    assert result["failed_count"] == 1


def test_users():

    result = summarize_users(
        make_jobs()
    )

    alice = (
        result
        .filter(
            pl.col("user") == "alice"
        )
        .row(
            0,
            named=True,
        )
    )

    assert alice["job_count"] == 2
    assert alice["total_elapsed_seconds"] == 300
    assert alice["total_gpu"] == 3


def test_partitions():

    result = summarize_partitions(
        make_jobs()
    )

    gpu = (
        result
        .filter(
            pl.col("partition") == "gpu"
        )
        .row(
            0,
            named=True,
        )
    )

    assert gpu["job_count"] == 2
    assert gpu["gpu_count"] == 3
    assert gpu["runtime_seconds"] == 300


def test_analyze_jobs():

    result = analyze_jobs(
        make_jobs()
    )

    assert "summary" in result
    assert "gpu" in result
    assert "gpu_efficiency" in result
    assert "runtime" in result
    assert "partitions" in result
    assert "users" in result

