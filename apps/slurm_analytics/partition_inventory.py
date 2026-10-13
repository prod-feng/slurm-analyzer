from __future__ import print_function

import os
import re
import subprocess
from datetime import datetime, timezone

import polars as pl

UTC = timezone.utc


def run_scontrol_show_partition():
    """Return the current partition configuration from this Slurm cluster."""
    env = os.environ.copy()
    env["TZ"] = "UTC"
    result = subprocess.run(
        ["scontrol", "show", "partition", "-o"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "scontrol show partition failed\nreturn code: {}\nstderr:\n{}".format(
                result.returncode, result.stderr
            )
        )
    return result.stdout


def _tokens(line):
    values = {}
    for token in line.strip().split():
        if "=" in token:
            key, value = token.split("=", 1)
            values[key] = value
    return values


def parse_scontrol_partitions(output, collected_at=None):
    """Parse `scontrol show partition -o`; retain raw output for future fields."""
    collected_at = collected_at or datetime.now(UTC)
    rows = []
    for line in (output or "").splitlines():
        if not line.strip():
            continue
        values = _tokens(line)
        name = values.get("PartitionName")
        if not name:
            continue
        rows.append({
            "partition_name": name,
            "state": values.get("State"),
            "nodes": values.get("Nodes"),
            "default_flag": values.get("Default"),
            "max_time": values.get("MaxTime"),
            "total_cpus": values.get("TotalCPUs"),
            "total_nodes": values.get("TotalNodes"),
            "allow_accounts": values.get("AllowAccounts"),
            "allow_groups": values.get("AllowGroups"),
            "allow_qos": values.get("AllowQos"),
            "deny_accounts": values.get("DenyAccounts"),
            "deny_qos": values.get("DenyQos"),
            "default_mem_per_cpu": values.get("DefMemPerCPU"),
            "default_mem_per_node": values.get("DefMemPerNode"),
            "max_mem_per_cpu": values.get("MaxMemPerCPU"),
            "max_mem_per_node": values.get("MaxMemPerNode"),
            "priority_tier": values.get("PriorityTier"),
            "tres": values.get("TRES"),
            "raw_output": line.strip(),
            "collected_at": collected_at,
        })
    return pl.DataFrame(rows)


def refresh_partition_inventory():
    """Refresh local partition metadata, or load saved metadata for import-mode clusters."""
    from slurm_analytics.storage import (
        current_cluster_mode, load_partition_inventory, replace_partition_inventory,
    )
    if current_cluster_mode() == "import":
        return load_partition_inventory()
    output = run_scontrol_show_partition()
    frame = parse_scontrol_partitions(output)
    if frame.is_empty():
        raise RuntimeError("No partitions could be parsed from scontrol output")
    replace_partition_inventory(frame)
    return frame
