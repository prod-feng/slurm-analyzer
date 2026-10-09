# slurm_analytics/normalize.py

from __future__ import print_function

import polars as pl

from .parser import (
    parse_gpu_count,
    parse_gpu_memory,
    parse_gpu_utilization,
    parse_job_id,
    parse_memory_allocation,
    parse_slurm_duration,
)


# ---------------------------------------------------------------------------
# Job classification
# ---------------------------------------------------------------------------

def classify_steps(df):
    """
    Classify records using the canonical JobID parser.
    """

    if df.is_empty():
        return df.with_columns(
            pl.lit("unknown").alias("step_type")
        )

    return df.with_columns(
        pl.col("jobid")
        .map_elements(
            lambda value: parse_job_id(value).step_type,
            return_dtype=pl.String,
        )
        .alias("step_type")
    )


# ---------------------------------------------------------------------------
# Job ID normalization
# ---------------------------------------------------------------------------

def normalize_job_ids(df):
    """
    Add:

        canonical_job_id
        step_id
    """

    return df.with_columns(
        [
            pl.col("jobid")
            .map_elements(
                lambda value: (
                    parse_job_id(value).canonical_job_id
                ),
                return_dtype=pl.String,
            )
            .alias("canonical_job_id"),

            pl.col("jobid")
            .map_elements(
                lambda value: (
                    parse_job_id(value).step_id
                ),
                return_dtype=pl.String,
            )
            .alias("step_id"),
        ]
    )


# ---------------------------------------------------------------------------
# Time normalization
# ---------------------------------------------------------------------------

def normalize_times(df):

    expressions = []

    for source, target in [
        ("submit", "submit_time"),
        ("eligible", "eligible_time"),
        ("start", "start_time"),
        ("end", "end_time"),
    ]:

        if source not in df.columns:
            continue

        expressions.append(
            pl.col(source)
            .str.to_datetime(
                strict=False,
                time_zone="UTC",
            )
            .alias(target)
        )

    if expressions:
        df = df.with_columns(
            expressions
        )

    if "elapsed" in df.columns:

        df = df.with_columns(
            pl.col("elapsed")
            .map_elements(
                parse_slurm_duration,
                return_dtype=pl.Float64,
            )
            .alias("elapsed_seconds")
        )

        df = df.with_columns(
            pl.col("elapsed_seconds")
            .round(0)
            .cast(pl.Int64)
        )

    if "timelimit" in df.columns:

        df = df.with_columns(
            pl.col("timelimit")
            .map_elements(
                parse_slurm_duration,
                return_dtype=pl.Float64,
            )
            .alias("time_limit_seconds")
        )

        df = df.with_columns(
            pl.col("time_limit_seconds")
            .round(0)
            .cast(pl.Int64)
        )

    if "reqmem" in df.columns:
        nnodes = df.get_column("nnodes") if "nnodes" in df.columns else pl.Series("nnodes", [None] * df.height)
        ncpus = df.get_column("ncpus") if "ncpus" in df.columns else pl.Series("ncpus", [None] * df.height)
        df = df.with_columns(
            pl.struct([pl.col("reqmem"), nnodes.alias("_nnodes"), ncpus.alias("_ncpus")])
            .map_elements(
                lambda row: parse_memory_allocation(row.get("reqmem"), row.get("_nnodes"), row.get("_ncpus")),
                return_dtype=pl.Int64,
            )
            .alias("allocated_memory_bytes")
        )

    return df


# ---------------------------------------------------------------------------
# GPU normalization
# ---------------------------------------------------------------------------

def normalize_gpu_fields(df):

    expressions = []

    if "alloctres" in df.columns:

        expressions.append(
            pl.col("alloctres")
            .map_elements(
                parse_gpu_count,
                return_dtype=pl.Float64,
            )
            .alias("gpu_count")
        )

    if "tresusageinave" in df.columns:

        expressions.extend(
            [
                pl.col("tresusageinave")
                .map_elements(
                    parse_gpu_utilization,
                    return_dtype=pl.Float64,
                )
                .alias("gpu_util"),

                pl.col("tresusageinave")
                .map_elements(
                    parse_gpu_memory,
                    return_dtype=pl.Int64,
                )
                .alias("gpu_mem_bytes"),
            ]
        )

    if expressions:
        df = df.with_columns(
            expressions
        )

    return df


# ---------------------------------------------------------------------------
# State normalization
# ---------------------------------------------------------------------------

def classify_state(value):
    """
    Convert Slurm state to a dashboard-friendly category.

    We preserve the original state and add a separate category.
    """

    if value is None:
        return "unknown"

    state = str(value).strip().upper()

    if not state:
        return "unknown"

    if state.startswith("RUNNING"):
        return "running"

    if state.startswith("PENDING"):
        return "pending"

    if state.startswith("COMPLETED"):
        return "completed"

    if state.startswith("CANCELLED"):
        return "cancelled"

    failure_prefixes = (
        "FAILED",
        "TIMEOUT",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
        "REVOKED",
        "SPECIAL_EXIT",
    )

    if state.startswith(
        failure_prefixes
    ):
        return "failed"

    return "other"


def normalize_states(df):

    if "state" not in df.columns:
        return df

    return df.with_columns(
        pl.col("state")
        .map_elements(
            classify_state,
            return_dtype=pl.String,
        )
        .alias("state_category")
    )


# ---------------------------------------------------------------------------
# Main normalization
# ---------------------------------------------------------------------------

def classify_and_normalize(df):
    """
    Convert raw sacct data into the application's
    normalized DataFrame.
    """

    if df.is_empty():
        # ``read_sacct_output`` returns a zero-column DataFrame when sacct
        # produced no rows.  Keep the normalized schema explicit so the
        # downstream job/step split does not try to resolve a missing
        # ``step_type`` column.  We intentionally do not run the normal
        # expressions here because there is no ``jobid`` column to derive
        # them from.
        return df.with_columns(
            [
                pl.Series("step_type", [], dtype=pl.String),
                pl.Series("canonical_job_id", [], dtype=pl.String),
                pl.Series("step_id", [], dtype=pl.String),
            ]
        )

    df = classify_steps(df)

    df = normalize_job_ids(df)

    df = normalize_times(df)

    df = normalize_gpu_fields(df)

    df = normalize_states(df)

    return df


# ---------------------------------------------------------------------------
# Split jobs and steps
# ---------------------------------------------------------------------------

def split_jobs_and_steps(df):

    jobs = df.filter(
        pl.col("step_type") == "job"
    )

    steps = df.filter(
        pl.col("step_type") != "job"
    )

    return jobs, steps


# ---------------------------------------------------------------------------
# Step flags
# ---------------------------------------------------------------------------

def add_step_flags(df):

    if df.is_empty():
        return df.with_columns(
            [
                pl.lit(False).alias("is_batch"),
                pl.lit(False).alias("is_extern"),
                pl.lit(False).alias("is_application_step"),
                pl.lit(False).alias("has_gpu_activity"),
                pl.lit(False).alias("is_workload_step"),
            ]
        )

    df = df.with_columns(
        [
            (
                pl.col("step_id") == "batch"
            ).alias("is_batch"),

            (
                pl.col("step_id") == "extern"
            ).alias("is_extern"),

            (
                ~pl.col("step_id")
                .is_in(["batch", "extern"])
            ).alias("is_application_step"),
        ]
    )

    df = df.with_columns(
        (
            (
                pl.col("gpu_util")
                .fill_null(0.0)
                > 0.0
            )
            |
            (
                pl.col("gpu_mem_bytes")
                .fill_null(0)
                > 0
            )
        )
        .alias("has_gpu_activity")
    )

    df = df.with_columns(
        (
            pl.col("has_gpu_activity")
            |
            (
                pl.col("is_application_step")
                &
                (
                    pl.col("elapsed_seconds")
                    .fill_null(0)
                    > 0
                )
            )
        )
        .alias("is_workload_step")
    )

    return df


# ---------------------------------------------------------------------------
# Prepare steps
# ---------------------------------------------------------------------------

def prepare_steps(steps):

    if steps.is_empty():
        return add_step_flags(
            steps
        )

    steps = add_step_flags(
        steps
    )

    columns = [
        "canonical_job_id",
        "jobid",
        "jobidraw",
        "step_id",
        "step_type",
        "cluster",
        "partition",
        "qos",
        "account",
        "user",
        "submit_time",
        "eligible_time",
        "start_time",
        "end_time",
        "elapsed_seconds",
        "state",
        "state_category",
        "nnodes",
        "ncpus",
        "reqcpus",
        "alloctres",
        "nodelist",
        "jobname",
        "tresusageintot",
        "tresusageinave",
        "tresusageinmax",
        "avecpu",
        "totalcpu",
        "maxrss",
        "maxvmsize",
        "gpu_count",
        "gpu_util",
        "gpu_mem_bytes",
        "allocated_memory_bytes",
        "is_workload_step",
        "has_gpu_activity",
    ]

    columns = [
        column
        for column in columns
        if column in steps.columns
    ]

    return steps.select(
        columns
    )


# ---------------------------------------------------------------------------
# Consolidate jobs
# ---------------------------------------------------------------------------

def consolidate_jobs(jobs, steps):
    """
    Produce one row per allocation/job.

    GPU allocation comes from the job allocation row.

    GPU utilization and memory come from step telemetry.
    """

    if jobs.is_empty():
        return jobs

    if "gpu_count" not in jobs.columns:
        jobs = jobs.with_columns(
            pl.lit(None, dtype=pl.Float64)
            .alias("gpu_count")
        )

    if steps.is_empty():

        return jobs.with_columns(
            [
                pl.lit(None, dtype=pl.Float64)
                .alias("gpu_util_min"),

                pl.lit(None, dtype=pl.Float64)
                .alias("gpu_util_max"),

                pl.lit(None, dtype=pl.Int64)
                .alias("gpu_mem_max_bytes"),

                pl.lit(0, dtype=pl.Int64)
                .alias("workload_step_count"),

                pl.lit(0, dtype=pl.Int64)
                .alias("step_count"),
            ]
        )

    if "is_workload_step" not in steps.columns:
        steps = add_step_flags(
            steps
        )

    step_summary = (
        steps
        .group_by("canonical_job_id")
        .agg(
            [
                pl.col("gpu_util")
                .min()
                .alias("gpu_util_min"),

                pl.col("gpu_util")
                .max()
                .alias("gpu_util_max"),

                pl.col("gpu_mem_bytes")
                .max()
                .alias("gpu_mem_max_bytes"),

                pl.col("is_workload_step")
                .sum()
                .cast(pl.Int64)
                .alias("workload_step_count"),

                pl.len()
                .cast(pl.Int64)
                .alias("step_count"),
            ]
        )
    )

    result = jobs.join(
        step_summary,
        on="canonical_job_id",
        how="left",
    )

    return result.with_columns(
        [
            pl.col("workload_step_count")
            .fill_null(0)
            .cast(pl.Int64),

            pl.col("step_count")
            .fill_null(0)
            .cast(pl.Int64),
        ]
    )

