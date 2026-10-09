# slurm_analytics/pipeline.py

from __future__ import print_function

from .normalize import (
    classify_and_normalize,
    consolidate_jobs,
    prepare_steps,
    split_jobs_and_steps,
)

from .sacct import fetch_sacct, read_sacct_file


class IngestionResult(object):

    def __init__(
        self,
        raw,
        jobs,
        steps,
        consolidated_jobs,
    ):
        self.raw = raw
        self.jobs = jobs
        self.steps = steps
        self.consolidated_jobs = consolidated_jobs


def ingest_sacct(config=None, input_file=None, timing=None):

    if input_file:
        raw = read_sacct_file(input_file)
    else:
        raw = fetch_sacct(config)

    normalized = classify_and_normalize(
        raw
    )

    jobs, steps = split_jobs_and_steps(
        normalized
    )

    steps = prepare_steps(
        steps
    )

    consolidated = consolidate_jobs(
        jobs,
        steps,
    )

    return IngestionResult(
        raw=raw,
        jobs=jobs,
        steps=steps,
        consolidated_jobs=consolidated,
    )

