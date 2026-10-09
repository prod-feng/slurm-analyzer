from __future__ import print_function

import os
import subprocess

import polars as pl


SQUEUE_FIELDS = [
    "jobid",
    "user",
    "partition",
    "jobname",
    "state",
    "time",
    "nodes",
    "cpus",
    "reason",
]


class SqueueConfig(object):

    def __init__(
        self,
        clusters="all",
        extra_args=None,
    ):
        self.clusters = clusters
        self.extra_args = tuple(
            extra_args or ()
        )


def build_squeue_command(config=None):
    """
    Build the squeue command.

    Uses the short -o format supported by Slurm 23.02.x.
    """

    if config is None:
        config = SqueueConfig()

    command = [
        "squeue",
        "--clusters",
        config.clusters,
        "-a",
        "-h",
        "-o",
        "%i|%u|%P|%j|%T|%M|%D|%C|%R",
    ]

    command.extend(
        config.extra_args
    )

    return command

def build_squeue_command_old(config=None):
    """
    Build the squeue command.

    Output is pipe-delimited and has no header.
    """

    if config is None:
        config = SqueueConfig()

    command = [
        "squeue",
        #"--clusters",
        #config.clusters,
        "-a",
        "--noheader",
        "--Format="
        "JobIDRaw|User|Partition|Name|State|"
        "Time|NumNodes|NumCPUs|Reason",
    ]

    command.extend(
        config.extra_args
    )

    return command


def run_squeue(config=None):
    """
    Execute squeue and return stdout.
    """

    command = build_squeue_command(
        config
    )

    env = os.environ.copy()
    env["TZ"] = "UTC"

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        env=env,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "squeue failed\n"
            "return code: {0}\n"
            "command: {1}\n"
            "stderr:\n{2}".format(
                result.returncode,
                " ".join(command),
                result.stderr,
            )
        )

    return result.stdout


def read_squeue_output(output):
    """
    Convert squeue output into a Polars DataFrame.
    """

    schema = {
        "jobid": pl.String,
        "user": pl.String,
        "partition": pl.String,
        "jobname": pl.String,
        "state": pl.String,
        "time": pl.String,
        "nodes": pl.String,
        "cpus": pl.String,
        "reason": pl.String,
    }

    if not output or not output.strip():
        return pl.DataFrame(
            schema=schema
        )

    return pl.read_csv(
        output.encode(),
        separator="|",
        has_header=False,
        new_columns=list(schema.keys()),
        schema_overrides=schema,
        infer_schema=False,
    )


def read_squeue_output_old(output):
    """
    Convert squeue output into a Polars DataFrame.
    """

    if not output or not output.strip():
        return pl.DataFrame(
            schema={
                "jobid": pl.String,
                "user": pl.String,
                "partition": pl.String,
                "jobname": pl.String,
                "state": pl.String,
                "time": pl.String,
                "nodes": pl.String,
                "cpus": pl.String,
                "reason": pl.String,
            }
        )

    return pl.read_csv(
        output.encode(),
        separator="|",
        has_header=False,
        new_columns=SQUEUE_FIELDS,
        infer_schema_length=0,
        null_values=[
            "",
            "Unknown",
            "unknown",
            "N/A",
            "n/a",
            "None",
        ],
    )


def fetch_squeue(config=None):
    """
    Execute squeue and parse the result.
    """

    output = run_squeue(
        config
    )

    return read_squeue_output(
        output
    )

