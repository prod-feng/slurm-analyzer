# slurm_analytics/sacct.py

from __future__ import print_function

import io
import json
import os
import subprocess
import sys
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
    "ntasks",
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
    "maxdiskread",
    "maxdiskwrite",
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


def read_sacct_output(output, fields=None, verbose=False):

    if not output or not output.strip():
        return pl.DataFrame()

    source_fields = list(fields or SACCT_FIELDS)
    expected = len(source_fields)
    # Parse Slurm's pipe-delimited --parsable2 output ourselves instead of
    # asking Polars' CSV tokenizer to infer CSV quoting. sacct fields can
    # contain literal quote characters, and on large historical exports those
    # can cause Polars to report a malformed CSV chunk even though the record
    # delimiters are still valid. Slurm does not use CSV quoting for this
    # output format, so each physical line is one record.
    rows = []
    null_tokens = {"", "Unknown", "unknown", "N/A", "n/a", "None"}
    malformed_count = 0
    debug_limit = 50
    diagnostic_examples = []
    for line_number, line in enumerate(output.splitlines(), start=1):
        if not line:
            continue
        values = line.split("|")
        # --parsable2 normally has exactly the expected number of fields,
        # sometimes followed by one final delimiter (empty trailing field).
        # Log suspicious physical rows BEFORE padding/truncating so the raw
        # source data is available for diagnosing historical export issues.
        field_count = len(values) - (1 if line.endswith("|") else 0)
        if field_count != expected:
            malformed_count += 1
            if len(diagnostic_examples) < 5:
                diagnostic_examples.append((line_number, field_count, line))
            if verbose and malformed_count <= debug_limit:
                print(
                    "[sacct debug] suspicious input row: line={0}, "
                    "expected_fields={1}, actual_fields={2}, "
                    "trailing_pipe={3}; raw={4}".format(
                        line_number,
                        expected,
                        field_count,
                        line.endswith("|"),
                        repr(line[:4000]) + ("...<truncated>" if len(line) > 4000 else ""),
                    ),
                    file=sys.stderr,
                )
        # sacct --parsable2 commonly emits a trailing pipe; extra fields from
        # an older/different export are truncated, while missing fields are
        # padded with nulls so columns never shift position.
        values = values[:expected]
        if len(values) < expected:
            values.extend([""] * (expected - len(values)))
        rows.append([None if value in null_tokens else value for value in values])

    if malformed_count:
        print(
            "[sacct warning] found {0} row(s) with unexpected field counts "
            "(expected {1} fields).".format(malformed_count, expected),
            file=sys.stderr,
        )
        if verbose:
            print(
                "[sacct debug] displayed {0} row(s); line numbers refer to "
                "physical lines in the input file.".format(min(malformed_count, debug_limit)),
                file=sys.stderr,
            )
        else:
            examples = ", ".join(
                "line {0}: {1} fields".format(line_no, count)
                for line_no, count, _ in diagnostic_examples
            )
            if examples:
                print("[sacct warning] sample locations: " + examples + ". Re-run with --verbose to print raw rows.", file=sys.stderr)

    if not rows:
        return pl.DataFrame()

    # sacct fields are text values, even when a particular column happens
    # to contain only numbers in the first rows. Explicitly declare every
    # source column as String so Polars does not infer an integer/float type
    # and then fail when a later row contains a different value (for example
    # "1" in a column inferred as another type).
    frame = pl.DataFrame(
        rows,
        schema={name: pl.String for name in source_fields},
        orient="row",
    )
    # Historical files can have been collected with an older/different
    # --format list. Align by field name, filling fields absent from that
    # collection with null rather than silently shifting every later column.
    for name in SACCT_FIELDS:
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias(name))
    return frame.select(SACCT_FIELDS)

def read_sacct_file(path, verbose=False):
    fields = None
    metadata_path = str(path) + ".json"
    if os.path.isfile(metadata_path):
        try:
            with open(metadata_path, "r") as metadata_handle:
                metadata = json.load(metadata_handle)
            candidate = metadata.get("fields")
            if isinstance(candidate, list) and candidate:
                fields = [str(name).strip().lower() for name in candidate]
        except (IOError, ValueError, TypeError):
            fields = None
    with open(path, "r") as handle:
        return read_sacct_output(handle.read(), fields=fields, verbose=verbose)

def fetch_sacct(config=None, verbose=False):

    output = run_sacct(
        config
    )

    return read_sacct_output(
        output, verbose=verbose
    )

