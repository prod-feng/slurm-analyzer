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


def ingest_sacct(config=None, input_file=None, timing=None, verbose=False):

    if input_file:
        if isinstance(input_file, (list, tuple)):
            frames = [read_sacct_file(path, verbose=verbose) for path in input_file]
            frames = [frame for frame in frames if not frame.is_empty()]
            if not frames:
                raw = read_sacct_file(input_file[0], verbose=verbose)
            else:
                raw = frames[0] if len(frames) == 1 else __import__("polars").concat(frames, how="diagonal_relaxed")
        else:
            raw = read_sacct_file(input_file, verbose=verbose)
    else:
        raw = fetch_sacct(config, verbose=verbose)

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

