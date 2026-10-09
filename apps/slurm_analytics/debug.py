from __future__ import print_function

import polars as pl


def _filter_job(df, job_id):
    if df is None or df.height == 0:
        return df

    if "canonical_job_id" in df.columns:
        return df.filter(
            pl.col("canonical_job_id") == str(job_id)
        )

    if "job_id" in df.columns:
        return df.filter(
            pl.col("job_id") == str(job_id)
        )

    if "jobid" in df.columns:
        return df.filter(
            pl.col("jobid") == str(job_id)
        )

    return df.head(0)


def _raw_job_rows(raw, job_id):
    if raw is None or raw.height == 0:
        return raw

    job_id = str(job_id)

    # Match both normal job IDs and step IDs.
    return raw.filter(
        pl.col("canonical_job_id") == job_id
    )


def _show(title, df, columns=None):
    print("")
    print("=" * 70)
    print(title)
    print("=" * 70)

    if df is None:
        print("(None)")
        return

    if df.height == 0:
        print("(no rows)")
        return

    if columns:
        columns = [
            c for c in columns
            if c in df.columns
        ]
        if columns:
            df = df.select(columns)

    print(df)


def _first_value(df, column):
    if df is None or df.height == 0:
        return None

    if column not in df.columns:
        return None

    value = df.select(column).to_series()[0]

    return value


def _check(job_id, raw, steps, consolidated):
    warnings = []
    checks = []

    raw_job = _raw_job_rows(raw, job_id)

    job_rows = _filter_job(consolidated, job_id)

    step_rows = _filter_job(steps, job_id)

    # ------------------------------------------------------------
    # Basic existence
    # ------------------------------------------------------------

    if raw_job.height > 0:
        checks.append("raw records found")
    else:
        warnings.append("no raw records found")

    if job_rows.height == 1:
        checks.append("one consolidated job row")
    elif job_rows.height == 0:
        warnings.append("no consolidated job row")
    else:
        warnings.append(
            "multiple consolidated job rows: %d"
            % job_rows.height
        )

    # ------------------------------------------------------------
    # Step count
    # ------------------------------------------------------------

    raw_step_count = 0

    if raw_job.height > 0 and "step_type" in raw_job.columns:
        raw_step_count = raw_job.filter(
            pl.col("step_type") != "job"
        ).height

    normalized_step_count = step_rows.height

    if raw_step_count == normalized_step_count:
        checks.append(
            "step count matches: %d"
            % raw_step_count
        )
    else:
        warnings.append(
            "step count mismatch: raw=%d normalized=%d"
            % (
                raw_step_count,
                normalized_step_count,
            )
        )

    # ------------------------------------------------------------
    # GPU allocation
    # ------------------------------------------------------------

    allocated_gpu = None

    if job_rows.height > 0 and "gpu_count" in job_rows.columns:
        allocated_gpu = _first_value(
            job_rows,
            "gpu_count",
        )

    step_gpu_count = None

    if (
        step_rows.height > 0
        and "gpu_count" in step_rows.columns
    ):
        values = (
            step_rows
            .select("gpu_count")
            .drop_nulls()
            .to_series()
            .to_list()
        )

        if values:
            step_gpu_count = max(values)

    if allocated_gpu is not None and step_gpu_count is not None:
        if float(allocated_gpu) == float(step_gpu_count):
            checks.append(
                "GPU count consistent: %s"
                % allocated_gpu
            )
        else:
            warnings.append(
                "GPU count mismatch: job=%s steps=%s"
                % (
                    allocated_gpu,
                    step_gpu_count,
                )
            )

    # ------------------------------------------------------------
    # GPU activity
    # ------------------------------------------------------------

    gpu_activity = False

    if step_rows.height > 0:
        if "gpu_util" in step_rows.columns:
            gpu_values = (
                step_rows
                .select("gpu_util")
                .drop_nulls()
                .to_series()
                .to_list()
            )

            for value in gpu_values:
                if float(value) > 0:
                    gpu_activity = True
                    break

        if (
            not gpu_activity
            and "gpu_mem_bytes" in step_rows.columns
        ):
            mem_values = (
                step_rows
                .select("gpu_mem_bytes")
                .drop_nulls()
                .to_series()
                .to_list()
            )

            for value in mem_values:
                if int(value) > 0:
                    gpu_activity = True
                    break

    if gpu_activity:
        checks.append("GPU telemetry found in steps")

        if allocated_gpu is not None:
            if float(allocated_gpu) <= 0:
                warnings.append(
                    "GPU telemetry exists but job GPU allocation is zero"
                )
    else:
        if allocated_gpu is not None:
            if float(allocated_gpu) > 0:
                warnings.append(
                    "GPU allocated but no GPU telemetry found"
                )
            else:
                checks.append(
                    "no GPU allocated and no GPU telemetry"
                )

    # ------------------------------------------------------------
    # Consolidated GPU values
    # ------------------------------------------------------------

    if job_rows.height > 0:
        for column in [
            "gpu_util_min",
            "gpu_util_max",
            "gpu_mem_max_bytes",
        ]:
            if column not in job_rows.columns:
                continue

            value = _first_value(
                job_rows,
                column,
            )

            if value is not None:
                checks.append(
                    "%s populated" % column
                )

    return checks, warnings


def check_job(
    result,
    job_id,
    show_raw=True,
    show_steps=True,
    show_consolidated=True,
):
    """
    Inspect one job across the complete ingestion pipeline.

    Example:

        check_job(result, "49594")
    """

    job_id = str(job_id)

    raw = result.raw
    steps = result.steps
    consolidated = result.consolidated_jobs

    print("")
    print("#" * 70)
    print("JOB %s" % job_id)
    print("#" * 70)

    raw_job = _raw_job_rows(
        raw,
        job_id,
    )

    step_rows = _filter_job(
        steps,
        job_id,
    )

    job_rows = _filter_job(
        consolidated,
        job_id,
    )

    if show_raw:
        _show(
            "RAW / NORMALIZED RECORDS",
            raw_job,
            [
                "jobid",
                "jobidraw",
                "canonical_job_id",
                "step_id",
                "step_type",
                "cluster",
                "partition",
                "state",
                "start_time",
                "end_time",
                "elapsed_seconds",
                "nnodes",
                "ncpus",
                "alloctres",
                "nodelist",
                "tresusageinave",
            ],
        )

    if show_steps:
        _show(
            "NORMALIZED STEPS",
            step_rows,
            [
                "jobid",
                "jobidraw",
                "step_id",
                "step_type",
                "start_time",
                "end_time",
                "elapsed_seconds",
                "state",
                "ncpus",
                "alloctres",
                "nodelist",
                "tresusageinave",
                "gpu_count",
                "gpu_util",
                "gpu_mem_bytes",
                "is_workload_step",
            ],
        )

    if show_consolidated:
        _show(
            "CONSOLIDATED JOB",
            job_rows,
        )

    checks, warnings = _check(
        job_id,
        raw,
        steps,
        consolidated,
    )

    print("")
    print("-" * 70)
    print("CHECKS")
    print("-" * 70)

    for item in checks:
        print("[OK]   %s" % item)

    if not checks:
        print("(none)")

    print("")
    print("-" * 70)
    print("WARNINGS")
    print("-" * 70)

    for item in warnings:
        print("[WARN] %s" % item)

    if not warnings:
        print("(none)")

    return {
        "job_id": job_id,
        "checks": checks,
        "warnings": warnings,
        "raw": raw_job,
        "steps": step_rows,
        "consolidated": job_rows,
    }


def check_jobs(
    result,
    job_ids,
    show_raw=True,
    show_steps=True,
    show_consolidated=True,
):
    """
    Inspect multiple jobs.

    Example:

        check_jobs(
            result,
            [
                "45179",
                "48327",
                "49594",
                "48910_3",
            ],
        )
    """

    results = []

    for job_id in job_ids:
        results.append(
            check_job(
                result,
                job_id,
                show_raw=show_raw,
                show_steps=show_steps,
                show_consolidated=show_consolidated,
            )
        )

    return results


def gpu_jobs(result):
    """
    Return consolidated jobs that appear to have GPU allocation.
    """

    jobs = result.consolidated_jobs

    if "gpu_count" not in jobs.columns:
        return jobs.head(0)

    return jobs.filter(
        pl.col("gpu_count")
        .fill_null(0)
        > 0
    )


def gpu_steps(result):
    """
    Return steps containing actual GPU activity.
    """

    steps = result.steps

    if "gpu_util" not in steps.columns:
        return steps.head(0)

    return steps.filter(
        (
            pl.col("gpu_util")
            .fill_null(0)
            > 0
        )
        |
        (
            pl.col("gpu_mem_bytes")
            .fill_null(0)
            > 0
        )
    )

