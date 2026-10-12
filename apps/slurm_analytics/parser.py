# slurm_analytics/parser.py

from __future__ import print_function

import re


STEP_RE = re.compile(
    r"^(?P<parent>.+)\.(?P<step>[^.]+)$"
)


UNKNOWN_VALUES = {
    "",
    "unknown",
    "n/a",
    "none",
    "null",
    "undefined",
}


class ParsedJobID(object):
    def __init__(
        self,
        original,
        canonical_job_id,
        step_id,
        step_type,
    ):
        self.original = original
        self.canonical_job_id = canonical_job_id
        self.step_id = step_id
        self.step_type = step_type

    def __repr__(self):
        return (
            "ParsedJobID("
            "original={!r}, "
            "canonical_job_id={!r}, "
            "step_id={!r}, "
            "step_type={!r}"
            ")"
        ).format(
            self.original,
            self.canonical_job_id,
            self.step_id,
            self.step_type,
        )


def parse_job_id(job_id):
    """
    Parse a Slurm JobID.

    Examples:

        48327
            job

        48327.batch
            batch

        48327.extern
            extern

        48327.99
            application

        49898_3
            array job

        49898_3.99
            application step of array job
    """

    if job_id is None:
        return ParsedJobID(
            "",
            "",
            None,
            "unknown",
        )

    value = str(job_id).strip()

    if not value:
        return ParsedJobID(
            value,
            value,
            None,
            "unknown",
        )

    match = STEP_RE.match(value)

    if not match:
        return ParsedJobID(
            value,
            value,
            None,
            "job",
        )

    parent = match.group("parent")
    step = match.group("step")

    if step == "batch":
        step_type = "batch"

    elif step == "extern":
        step_type = "extern"

    elif step.isdigit():
        step_type = "application"

    else:
        step_type = "unknown_step"

    return ParsedJobID(
        value,
        parent,
        step,
        step_type,
    )


def parse_slurm_duration(value):
    """
    Convert Slurm duration to seconds.

    Examples:

        00:01:30
        02:09:19
        10-00:00:05
        1-02:03:04
        30
        UNLIMITED
    """

    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    if value.lower() in UNKNOWN_VALUES:
        return None

    if value.lower() in {
        "unlimited",
        "inf",
        "infinite",
    }:
        return None

    try:
        days = 0

        if "-" in value:
            day_part, value = value.split("-", 1)
            days = int(day_part)

        parts = value.split(":")

        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])

        elif len(parts) == 2:
            hours = 0
            minutes = int(parts[0])
            seconds = float(parts[1])

        elif len(parts) == 1:
            return (
                days * 86400
                + float(parts[0])
            )

        else:
            return None

        return (
            days * 86400
            + hours * 3600
            + minutes * 60
            + seconds
        )

    except (
        ValueError,
        TypeError,
    ):
        return None


def parse_slurm_memory(value):
    """
    Convert Slurm memory notation to bytes.

    Examples:

        4K
        150G
        128G
        500M
        1T
        100G/c
        100G/n

    Scope suffixes c and n are accepted.
    """

    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    if value.lower() in UNKNOWN_VALUES:
        return None

    match = re.match(
        r"^\s*"
        r"(?P<number>[0-9]+(?:\.[0-9]+)?)"
        r"(?P<unit>[KMGTPE]?)"
        r"(?P<scope>[cn]?)"
        r"\s*$",
        value,
        re.IGNORECASE,
    )

    if not match:
        return None

    number = float(
        match.group("number")
    )

    unit = match.group(
        "unit"
    ).upper()

    multipliers = {
        "": 1,
        "K": 1024,
        "M": 1024 ** 2,
        "G": 1024 ** 3,
        "T": 1024 ** 4,
        "P": 1024 ** 5,
        "E": 1024 ** 6,
    }

    return int(
        number * multipliers[unit]
    )


def parse_memory_allocation(value, nnodes=None, ncpus=None):
    """Return allocated memory bytes for a job allocation.

    Slurm ReqMem commonly carries a scope suffix: ``c`` means per CPU and
    ``n`` means per node.  Unsuffixed values are treated as per-node, which
    is the conservative interpretation for accounting summaries.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.match(r"^(.*?)/?([cn])$", text, re.IGNORECASE)
    scope = match.group(2).lower() if match else "n"
    memory_text = match.group(1) if match else text
    base = parse_slurm_memory(memory_text)
    if base is None:
        return None
    try:
        multiplier = float(ncpus if scope == "c" else nnodes)
    except (TypeError, ValueError):
        return None
    if multiplier <= 0:
        return None
    return int(base * multiplier)



def parse_tres(value):
    """
    Parse a Slurm TRES string.

    Example:

        cpu=4,gres/gpu=1,mem=128G,node=1

    Returns:

        {
            "cpu": "4",
            "gres/gpu": "1",
            "mem": "128G",
            "node": "1",
        }
    """

    if value is None:
        return {}

    value = str(value).strip()

    if not value:
        return {}

    result = {}

    for item in value.split(","):
        if "=" not in item:
            continue

        key, val = item.split("=", 1)

        key = key.strip()
        val = val.strip()

        if key:
            result[key] = val

    return result


def parse_gpu_count(value):
    """
    Extract GPU allocation from a TRES string.

    Handles:

        gres/gpu=1
        gres/gpu:a100=8
        gres/gpu:a100=8,gres/gpu:h100=2

    Returns float or None.
    """

    parsed = parse_tres(value)

    total = 0.0
    found = False

    for key, val in parsed.items():

        if not (
            key == "gres/gpu"
            or key.startswith("gres/gpu:")
        ):
            continue

        try:
            total += float(val)
            found = True

        except (
            ValueError,
            TypeError,
        ):
            continue

    if not found:
        return None

    return total


def parse_gpu_utilization(value):
    """
    Extract gres/gpuutil from TRES usage.
    """

    parsed = parse_tres(value)

    raw = parsed.get(
        "gres/gpuutil"
    )

    if raw is None:
        return None

    try:
        return float(raw)

    except (
        ValueError,
        TypeError,
    ):
        return None


def parse_gpu_memory(value):
    """
    Extract gres/gpumem from TRES usage
    and convert it to bytes.
    """

    parsed = parse_tres(value)

    return parse_slurm_memory(
        parsed.get("gres/gpumem")
    )

