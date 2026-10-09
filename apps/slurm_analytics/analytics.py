# slurm_analytics/analytics.py

from __future__ import annotations

import polars as pl


def _empty_summary():
    return pl.DataFrame(
        {
            "job_count": [0],
            "running_count": [0],
            "pending_count": [0],
            "completed_count": [0],
            "failed_count": [0],
            "other_count": [0],
            "total_runtime_seconds": [0],
            "avg_runtime_seconds": [None],
        }
    )


def summarize_jobs(df):

    if df.is_empty():
        return _empty_summary()

    state = (
        pl.col("state_category")
        if "state_category" in df.columns
        else pl.lit("unknown")
    )

    return df.select(
        [
            pl.len().alias("job_count"),

            (
                state
                == "running"
            )
            .sum()
            .alias("running_count"),

            (
                state
                == "pending"
            )
            .sum()
            .alias("pending_count"),

            (
                state
                == "completed"
            )
            .sum()
            .alias("completed_count"),

            (
                state
                == "failed"
            )
            .sum()
            .alias("failed_count"),

            (
                state
                == "other"
            )
            .sum()
            .alias("other_count"),

            pl.col("elapsed_seconds")
            .fill_null(0)
            .sum()
            .alias("total_runtime_seconds"),

            pl.col("elapsed_seconds")
            .mean()
            .alias("avg_runtime_seconds"),
        ]
    )


def summarize_gpu_jobs(df):

    if df.is_empty():
        return pl.DataFrame(
            {
                "gpu_job_count": [0],
                "allocated_gpu_count": [0.0],
                "gpu_util_min": [None],
                "gpu_util_max": [None],
                "gpu_mem_max_bytes": [None],
            }
        )

    gpu = df.filter(
        pl.col("gpu_count")
        .fill_null(0)
        > 0
    )

    if gpu.is_empty():
        return pl.DataFrame(
            {
                "gpu_job_count": [0],
                "allocated_gpu_count": [0.0],
                "gpu_util_min": [None],
                "gpu_util_max": [None],
                "gpu_mem_max_bytes": [None],
            }
        )

    return gpu.select(
        [
            pl.len().alias(
                "gpu_job_count"
            ),

            pl.col("gpu_count")
            .sum()
            .alias(
                "allocated_gpu_count"
            ),

            pl.col("gpu_util_min")
            .min()
            .alias(
                "gpu_util_min"
            ),

            pl.col("gpu_util_max")
            .max()
            .alias(
                "gpu_util_max"
            ),

            pl.col("gpu_mem_max_bytes")
            .max()
            .alias(
                "gpu_mem_max_bytes"
            ),
        ]
    )


def summarize_partitions(df):

    if (
        df.is_empty()
        or "partition" not in df.columns
    ):
        return pl.DataFrame()

    return (
        df.group_by("partition")
        .agg(
            [
                pl.len().alias(
                    "job_count"
                ),

                pl.col("gpu_count")
                .fill_null(0)
                .sum()
                .alias(
                    "gpu_count"
                ),

                pl.col("elapsed_seconds")
                .fill_null(0)
                .sum()
                .alias(
                    "runtime_seconds"
                ),
            ]
        )
        .sort(
            "job_count",
            descending=True,
        )
    )


def summarize_users(df):

    if (
        df.is_empty()
        or "user" not in df.columns
    ):
        return pl.DataFrame()

    aggregations = [
        pl.len().alias(
            "job_count"
        )
    ]

    if "elapsed_seconds" in df.columns:
        aggregations.append(
            pl.col("elapsed_seconds")
            .cast(
                pl.Int64,
                strict=False,
            )
            .fill_null(0)
            .sum()
            .alias(
                "total_elapsed_seconds"
            )
        )

    if "ncpus" in df.columns:
        aggregations.append(
            pl.col("ncpus")
            .cast(
                pl.Int64,
                strict=False,
            )
            .fill_null(0)
            .sum()
            .alias(
                "total_cpu"
            )
        )

    if "gpu_count" in df.columns:
        aggregations.append(
            pl.col("gpu_count")
            .cast(
                pl.Float64,
                strict=False,
            )
            .fill_null(0)
            .sum()
            .alias(
                "total_gpu"
            )
        )

    return (
        df.group_by("user")
        .agg(aggregations)
        .sort(
            "job_count",
            descending=True,
        )
    )


def runtime_statistics(df):

    if df.is_empty():
        return pl.DataFrame()

    return df.select(
        [
            pl.col("elapsed_seconds")
            .min()
            .alias("min_seconds"),

            pl.col("elapsed_seconds")
            .mean()
            .alias("mean_seconds"),

            pl.col("elapsed_seconds")
            .median()
            .alias("median_seconds"),

            pl.col("elapsed_seconds")
            .quantile(0.95)
            .alias("p95_seconds"),

            pl.col("elapsed_seconds")
            .quantile(0.99)
            .alias("p99_seconds"),

            pl.col("elapsed_seconds")
            .max()
            .alias("max_seconds"),
        ]
    )


def gpu_efficiency(df):

    if df.is_empty():
        return pl.DataFrame()

    gpu = df.filter(
        pl.col("gpu_count")
        .fill_null(0)
        > 0
    )

    if gpu.is_empty():
        return pl.DataFrame()

    return gpu.select(
        [
            pl.len().alias(
                "gpu_job_count"
            ),

            pl.col("gpu_util_max")
            .is_not_null()
            .sum()
            .alias(
                "jobs_with_gpu_telemetry"
            ),

            pl.col("gpu_util_max")
            .is_null()
            .sum()
            .alias(
                "jobs_without_gpu_telemetry"
            ),

            pl.col("gpu_util_max")
            .mean()
            .alias(
                "average_gpu_utilization"
            ),

            pl.col("gpu_util_max")
            .max()
            .alias(
                "maximum_gpu_utilization"
            ),

            pl.col("gpu_mem_max_bytes")
            .max()
            .alias(
                "maximum_gpu_memory_bytes"
            ),
        ]
    )


def analyze_jobs(df):

    return {
        "summary": summarize_jobs(df),
        "gpu": summarize_gpu_jobs(df),
        "gpu_efficiency": gpu_efficiency(df),
        "runtime": runtime_statistics(df),
        "partitions": summarize_partitions(df),
        "users": summarize_users(df),
    }



def time_series(df, start=None, end=None, interval="1h"):
    """Public analytics entry point for allocation time series."""
    from .timeseries import job_time_series

    return job_time_series(
        df,
        start=start,
        end=end,
        interval=interval,
    )
