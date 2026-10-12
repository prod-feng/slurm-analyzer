"""Configuration for multiple Slurm clusters served by one Django instance.

Set SLURM_CLUSTERS_CONFIG to a JSON file containing a list of cluster objects:
[{"slug":"cluster1","name":"Cluster 1","db_path":"/data/cluster1.duckdb","mode":"live"}]
mode is informational: use "live" for clusters this host can query with sacct,
or "import" for remote clusters populated from archived sacct files.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

from slurm_analytics.storage import DEFAULT_DB_PATH


def _default_clusters():
    return [{"slug": "default", "name": "Default cluster",
             "db_path": str(os.environ.get("SLURM_DUCKDB_PATH") or DEFAULT_DB_PATH),
             "mode": "live"}]


@lru_cache(maxsize=4)
def _load_config(config_path, modified_ns):
    if not config_path:
        return _default_clusters()
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if isinstance(raw, dict):
        raw = raw.get("clusters", [])
    if not isinstance(raw, list) or not raw:
        raise ValueError("Cluster configuration must be a non-empty list or an object with a clusters list")
    clusters = []
    seen = set()
    for item in raw:
        slug = str(item.get("slug", "")).strip().lower()
        name = str(item.get("name") or slug).strip()
        db_path = item.get("db_path") or item.get("database")
        if not slug or not all(c.isalnum() or c in "-_" for c in slug):
            raise ValueError("Every cluster requires a URL-safe slug (letters, digits, hyphens, underscores)")
        if slug in seen:
            raise ValueError("Duplicate cluster slug: {}".format(slug))
        if not db_path:
            raise ValueError("Cluster {} requires db_path".format(slug))
        seen.add(slug)
        db_file = Path(db_path).expanduser()
        if not db_file.is_absolute() and config_path:
            db_file = Path(config_path).expanduser().resolve().parent / db_file
        clusters.append({"slug": slug, "name": name, "db_path": str(db_file),
                         "mode": str(item.get("mode", "import")).lower()})
    return clusters


def get_clusters():
    # By default, read clusters.json from the application root (normally
    # /workspace inside the Apptainer container). SLURM_CLUSTERS_CONFIG can
    # still override this with an absolute or relative path.
    config_path = os.environ.get("SLURM_CLUSTERS_CONFIG")
    if not config_path:
        default_config = Path(__file__).resolve().parents[2] / "clusters.json"
        if default_config.is_file():
            config_path = str(default_config)
    modified_ns = 0
    if config_path and os.path.exists(config_path):
        modified_ns = os.stat(config_path).st_mtime_ns
    return _load_config(config_path, modified_ns)


def get_default_cluster():
    clusters = get_clusters()
    desired = os.environ.get("SLURM_DEFAULT_CLUSTER")
    if desired:
        for cluster in clusters:
            if cluster["slug"] == desired:
                return cluster
        raise ValueError("SLURM_DEFAULT_CLUSTER {!r} is not in cluster configuration".format(desired))
    return clusters[0]


def get_cluster(slug):
    for cluster in get_clusters():
        if cluster["slug"] == slug:
            return cluster
    return None
