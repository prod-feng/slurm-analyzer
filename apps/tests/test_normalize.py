# tests/test_normalize.py

import polars as pl

from slurm_analytics.normalize import (
    classify_and_normalize,
    consolidate_jobs,
    split_jobs_and_steps,
)


def make_input():

    return pl.DataFrame(
        {
            "jobid": [
                "100",
                "100.batch",
                "100.extern",
                "100.0",
                "101",
            ],
            "jobidraw": [
                "100",
                "100.batch",
                "100.extern",
                "100.0",
                "101",
            ],
            "cluster": [
                "test",
                "test",
                "test",
                "test",
                "test",
            ],
            "partition": [
                "gpu",
                "gpu",
                "gpu",
                "gpu",
                "cpu",
            ],
            "qos": [
                "normal",
                "normal",
                "normal",
                "normal",
                "normal",
            ],
            "account": [
                "research",
                "research",
                "research",
                "research",
                "research",
            ],
            "user": [
                "alice",
                "alice",
                "alice",
                "alice",
                "bob",
            ],
            "submit": [
                "2026-10-06T10:00:00",
                "2026-10-06T10:00:00",
                "2026-10-06T10:00:00",
                "2026-10-06T10:00:00",
                "2026-10-06T11:00:00",
            ],
            "eligible": [
                "2026-10-06T10:00:00",
                "2026-10-06T10:00:00",
                "2026-10-06T10:00:00",
                "2026-10-06T10:00:00",
                "2026-10-06T11:00:00",
            ],
            "start": [
                "2026-10-06T10:01:00",
                "2026-10-06T10:01:00",
                "2026-10-06T10:01:00",
                "2026-10-06T10:01:00",
                "2026-10-06T11:01:00",
            ],
            "end": [
                "2026-10-06T10:11:00",
                "2026-10-06T10:11:00",
                "2026-10-06T10:11:00",
                "2026-10-06T10:11:00",
                "2026-10-06T11:31:00",
            ],
            "elapsed": [
                "00:10:00",
                "00:01:00",
                "00:00:01",
                "00:08:00",
                "00:30:00",
            ],
            "exitcode": [
                "0:0",
                "0:0",
                "0:0",
                "0:0",
                "1:0",
            ],
            "state": [
                "COMPLETED",
                "COMPLETED",
                "COMPLETED",
                "COMPLETED",
                "FAILED",
            ],
            "nnodes": [
                "1",
                "1",
                "1",
                "1",
                "1",
            ],
            "ncpus": [
                "8",
                "8",
                "8",
                "8",
                "4",
            ],
            "reqcpus": [
                "8",
                "8",
                "8",
                "8",
                "4",
            ],
            "reqmem": [
                "64G",
                "64G",
                "64G",
                "64G",
                "16G",
            ],
            "reqtres": [
                "cpu=8,gres/gpu=1",
                "cpu=8,gres/gpu=1",
                "cpu=8,gres/gpu=1",
                "cpu=8,gres/gpu=1",
                "cpu=4",
            ],
            "alloctres": [
                "cpu=8,gres/gpu=1",
                "cpu=8,gres/gpu=1",
                "cpu=8,gres/gpu=1",
                "cpu=8,gres/gpu=1",
                "cpu=4",
            ],
            "timelimit": [
                "01:00:00",
                "01:00:00",
                "01:00:00",
                "01:00:00",
                "01:00:00",
            ],
            "nodelist": [
                "gpu01",
                "gpu01",
                "gpu01",
                "gpu01",
                "cpu01",
            ],
            "jobname": [
                "test",
                "test",
                "test",
                "test",
                "failed",
            ],
            "tresusageinave": [
                None,
                None,
                None,
                "gres/gpuutil=75,gres/gpumem=4G",
                None,
            ],
        }
    )


def test_normalize_and_split():

    df = make_input()

    normalized = classify_and_normalize(
        df
    )

    assert "canonical_job_id" in normalized.columns
    assert "step_id" in normalized.columns
    assert "state_category" in normalized.columns

    jobs, steps = split_jobs_and_steps(
        normalized
    )

    assert jobs.height == 2
    assert steps.height == 3


def test_job_id_normalization():

    df = make_input()

    normalized = classify_and_normalize(
        df
    )

    row = (
        normalized
        .filter(
            pl.col("jobid")
            == "100.0"
        )
        .row(0, named=True)
    )

    assert row["canonical_job_id"] == "100"
    assert row["step_id"] == "0"
    assert row["step_type"] == "application"


def test_state_classification():

    df = make_input()

    normalized = classify_and_normalize(
        df
    )

    failed = (
        normalized
        .filter(
            pl.col("jobid") == "101"
        )
        .select("state_category")
        .item()
    )

    assert failed == "failed"


def test_gpu_allocation():

    df = make_input()

    normalized = classify_and_normalize(
        df
    )

    row = (
        normalized
        .filter(
            pl.col("jobid") == "100"
        )
        .row(0, named=True)
    )

    assert row["gpu_count"] == 1


def test_gpu_telemetry():

    df = make_input()

    normalized = classify_and_normalize(
        df
    )

    row = (
        normalized
        .filter(
            pl.col("jobid") == "100.0"
        )
        .row(0, named=True)
    )

    assert row["gpu_util"] == 75
    assert row["gpu_mem_bytes"] == 4 * 1024 ** 3


def test_consolidation():

    df = make_input()

    normalized = classify_and_normalize(
        df
    )

    jobs, steps = split_jobs_and_steps(
        normalized
    )

    from slurm_analytics.normalize import prepare_steps

    steps = prepare_steps(
        steps
    )

    consolidated = consolidate_jobs(
        jobs,
        steps,
    )

    assert consolidated.height == 2

    row = (
        consolidated
        .filter(
            pl.col("canonical_job_id")
            == "100"
        )
        .row(0, named=True)
    )

    assert row["gpu_count"] == 1
    assert row["step_count"] == 3
    assert row["workload_step_count"] == 1
    assert row["gpu_util_max"] == 75

def test_empty_sacct_result_has_normalized_step_schema():
    empty = pl.DataFrame()

    normalized = classify_and_normalize(empty)

    assert normalized.is_empty()
    assert "step_type" in normalized.columns
    assert "canonical_job_id" in normalized.columns
    assert "step_id" in normalized.columns

    jobs, steps = split_jobs_and_steps(normalized)

    assert jobs.is_empty()
    assert steps.is_empty()

    prepared = prepare_steps(steps)
    assert prepared.is_empty()
    assert "is_batch" in prepared.columns
    assert "is_extern" in prepared.columns
    assert "is_application_step" in prepared.columns
    assert "is_workload_step" in prepared.columns

