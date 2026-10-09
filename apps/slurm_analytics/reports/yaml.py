"""YAML reports using the standalone analytics layer."""
from __future__ import annotations

import yaml

from ..accounts import account_usage, user_usage, account_tree


def _round_report(data):
    metrics = ("allocated_nodes", "cpu_hours", "gpu_hours", "memory_gb_hours", "gpu_memory_gb", "elapsed_hours")
    for row in data:
        for key in metrics:
            if key in row and row[key] is not None:
                row[key] = round(float(row[key]), 2)
    return data


def build_accounts_report(start=None, end=None, limit=100000):
    return {"period": {"start": start, "end": end}, "accounts": _round_report(account_tree(start, end)[:limit])}


def build_users_report(start=None, end=None, pattern=None, limit=100000):
    return {"period": {"start": start, "end": end}, "users": _round_report(user_usage(start, end, pattern, None, limit, round_metrics=False).to_dicts())}


def write_yaml(data, path):
    with open(path, "w") as handle:
        yaml.safe_dump(data, handle, sort_keys=False, default_flow_style=False)
