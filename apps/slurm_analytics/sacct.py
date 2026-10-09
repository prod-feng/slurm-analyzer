# slurm_analytics/sacct.py

from __future__ import print_function

import io
import os
import subprocess
from datetime import datetime, timezone

import polars as pl


SACCT_FIELDS = [
    "jobid",
    "jobidraw",
    "cluster",
    "partition",
    "qos",
    "account",
    "group",
    "gid",
    "user",
    "uid",
    "submit",
    "eligible",
    "start",
    "end",
    "elapsed",
    "exitcode",
    "state",
    "nnodes",
    "ncpus",
    "reqcpus",
    "reqmem",
    "reqtres",
    "alloctres",
    "timelimit",
    "nodelist",
    "jobname",
    "tresusageintot",
    "tresusageinave",
    "tresusageinmax",
    "avecpu",
    "totalcpu",
    "maxrss",
    "maxvmsize",
]


class SacctConfig(object):

    def __init__(
        self,
        clusters="all",
        start=None,
        end=None,
        extra_args=None,
        timeout=300,
    ):
        self.clusters = clusters
        self.start = start
        self.end = end
        self.extra_args = tuple(
            extra_args or ()
        )
        self.timeout = timeout


def format_sacct_time(value):
    """Format an ISO/datetime value in the syntax accepted by sacct.

    sacct does not accept Python's offset-aware ISO-8601 form such as
    ``2025-01-01T00:00:00+00:00`` for ``-S``/``-E`` on some Slurm versions.
    Always pass UTC as ``YYYY-MM-DDTHH:MM:SS`` without the timezone suffix.
    Date-only values are also accepted and remain midnight UTC.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            # Preserve non-ISO sacct-native strings rather than guessing.
            return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")



def build_sacct_command(config=None):

    if config is None:
        config = SacctConfig()

    command = [
        "sacct",
        "--clusters",
        config.clusters,
        "-a",
        "--parsable2",
        "--noheader",
        "--duplicates",
        "--format=" + ",".join(
            SACCT_FIELDS
        ),
    ]

    if config.start is not None:
        command.extend([
            "-S",
            format_sacct_time(config.start),
        ])

    if config.end is not None:
        command.extend([
            "-E",
            format_sacct_time(config.end),
        ])

    command.extend(
        config.extra_args
    )

    return command


def run_sacct(config=None):

    if config is None:
        config = SacctConfig()

    command = build_sacct_command(
        config
    )

    env = os.environ.copy()
    env["TZ"] = "UTC"

    try:

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env,
            timeout=config.timeout,
        )

    except subprocess.TimeoutExpired:

        raise RuntimeError(
            "sacct timed out after {} seconds".format(
                config.timeout
            )
        )

    if result.returncode != 0:

        raise RuntimeError(
            "sacct failed\n"
            "return code: {0}\n"
            "command: {1}\n"
            "stderr:\n{2}".format(
                result.returncode,
                " ".join(command),
                result.stderr,
            )
        )

    return result.stdout


def read_sacct_output(output):

    if not output or not output.strip():
        return pl.DataFrame()

    return pl.read_csv(
        io.StringIO(output),
        separator="|",
        has_header=False,
        new_columns=SACCT_FIELDS,
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


def fetch_sacct(config=None):

    output = run_sacct(
        config
    )

    return read_sacct_output(
        output
    )

