from __future__ import print_function

import os
import subprocess

from datetime import datetime
from datetime import timezone

import polars as pl


UTC = timezone.utc


def run_scontrol_show_node(
    nodes=None,
):
    command = [
        "scontrol",
        "show",
        "node",
        "-a",
    ]

    if nodes:
        command.extend(nodes)

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
            "scontrol show node failed\n"
            "return code: {0}\n"
            "stderr:\n{1}".format(
                result.returncode,
                result.stderr,
            )
        )

    return result.stdout


def parse_key_value_tokens(
    block,
):
    result = {}

    for token in block.replace(
        "\n",
        " ",
    ).split():

        if "=" not in token:
            continue

        key, value = token.split(
            "=",
            1,
        )

        result[key] = value

    return result


def parse_scontrol_nodes(
    output,
    collected_at=None,
):
    """
    Parse scontrol show node output.

    We retain the raw GRES field because
    GPU naming is cluster-specific.
    """

    if collected_at is None:
        collected_at = (
            datetime.now(UTC)
        )

    blocks = [
        block.strip()
        for block in output.split(
            "\n\n"
        )
        if block.strip()
    ]

    rows = []

    for block in blocks:

        values = (
            parse_key_value_tokens(
                block
            )
        )

        node_name = values.get(
            "NodeName"
        )

        if not node_name:
            continue

        rows.append(
            {
                "node_name": node_name,

                "state": values.get(
                    "State"
                ),

                "partition": values.get(
                    "Partitions"
                ),

                "cpu_alloc": values.get(
                    "CPUAlloc"
                ),

                "cpu_total": values.get(
                    "CPUTot"
                ),

                "real_memory": values.get(
                    "RealMemory"
                ),

                "alloc_memory": values.get(
                    "AllocMem"
                ),

                "free_memory": values.get(
                    "FreeMem"
                ),

                "gres": values.get(
                    "Gres"
                ),

                "gres_used": values.get(
                    "GresUsed"
                ),

                "feature": values.get(
                    "AvailableFeatures"
                ),

                "active_features": values.get(
                    "ActiveFeatures"
                ),

                "collected_at": (
                    collected_at
                ),
            }
        )

    return pl.DataFrame(
        rows
    )


def refresh_node_inventory(
    nodes=None,
):
    output = (
        run_scontrol_show_node(
            nodes
        )
    )

    return parse_scontrol_nodes(
        output
    )

