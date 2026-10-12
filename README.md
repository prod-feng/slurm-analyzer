# Slurm Analytics

A Django-based dashboard and CLI for exploring Slurm accounting history, resource usage, accounts, users, partitions, GPU models, and time-series metrics. Historical job and job-step records are normalized and stored in DuckDB.

## Multi-cluster mode

One webapp can serve multiple Slurm clusters. Each cluster has its own DuckDB database, while all clusters use the same dashboard code and tabs. The top navigation switches between cluster pages.

1. Copy `clusters.example.json` to a protected configuration path, for example `/etc/slurm-analytics/clusters.json`.
2. Edit each cluster's `slug`, display `name`, absolute `db_path`, and `mode`.
3. Set `SLURM_CLUSTERS_CONFIG=/etc/slurm-analytics/clusters.json` in the Django service environment and restart the service.
4. Open `/clusters/cluster1/`, `/clusters/cluster2/`, etc.

Example configuration:

```json
{
  "clusters": [
    {"slug":"cluster1", "name":"Cluster 1", "db_path":"/var/lib/slurm-analytics/cluster1.duckdb", "mode":"live"},
    {"slug":"cluster2", "name":"Cluster 2", "db_path":"/var/lib/slurm-analytics/cluster2.duckdb", "mode":"import"}
  ]
}
```

- `mode: "live"`: this host can query the cluster using `sacct` and `sacctmgr`. The live queue and refresh endpoints are enabled.
- `mode: "import"`: the cluster is remote or otherwise not queryable from this host. Use historical files to initialize/update its database. Live queue and refresh endpoints are disabled.

The `db_path` must be writable by the Django process. Back up each DuckDB database separately. Do not use the same database file for more than one cluster. Keep the cluster configuration file outside the public web root and restrict its permissions.

### Import historical data for each cluster

For an imported cluster, run the management command with `SLURM_DUCKDB_PATH` set to that cluster's configured database. Run it inside the same container/environment that contains the project code and dependencies.

```bash
# Cluster 2: import previously collected sacct files
SLURM_DUCKDB_PATH=/var/lib/slurm-analytics/cluster2.duckdb \
  python manage.py initialize_slurm \
  --history-file "/data/cluster2-history/*.out" \
  --no-accounts

# Cluster 3: a separate database and separate history files
SLURM_DUCKDB_PATH=/var/lib/slurm-analytics/cluster3.duckdb \
  python manage.py initialize_slurm \
  --history-file "/data/cluster3-history/*.out" \
  --no-accounts
```

Use `--no-accounts` only if account/association information is unavailable or you intentionally want to skip querying `sacctmgr`. Job-based account usage is still available from imported records, but the parent-child account tree may be incomplete without that cluster’s account hierarchy. Do not query the local cluster’s `sacctmgr` and assume it describes a remote cluster. If the hierarchy is needed, export or collect that cluster’s account/association metadata separately and add an import path for it before relying on hierarchical rollups. For a remote cluster, historical files can be collected on a host that has Slurm access and transferred to the webapp host. Keep every `.out` file with its matching `.out.json` metadata sidecar. To preserve that remote cluster's account tree too, run `python /workspace/manage.py export_slurm_accounts --output /workspace/cluster2-accounts.json` on a host configured for that cluster, transfer the JSON file, and import it alongside history with `python /workspace/manage.py initialize_slurm --history-file '/workspace/history/*.out' --accounts-file /workspace/cluster2-accounts.json`. The imported accounts and associations are stored in that cluster's own DuckDB (`accounts`, `associations`, and their history tables).

### Update a file-import cluster later

When newer accounting files are available, import them into the same cluster database:

```bash
SLURM_DUCKDB_PATH=/var/lib/slurm-analytics/cluster2.duckdb \
  python manage.py initialize_slurm \
  --history-file "/data/cluster2-history/*.out" \
  --no-accounts
```

The importer upserts records using their record keys, so overlapping files can be re-imported. Do not run multiple writers against the same DuckDB database concurrently. A file-import cluster does not automatically refresh itself; transfer new files and rerun the import when you want to update it.

### Default routes

The existing `/` and `/api/` routes remain aliases for the configured default cluster. Set `SLURM_DEFAULT_CLUSTER` to choose the default cluster slug. The top navigation links to the explicit `/clusters/<slug>/` route for each cluster.

## Single-cluster setup

Without `SLURM_CLUSTERS_CONFIG`, the app keeps the original single-cluster behavior and uses `SLURM_DUCKDB_PATH` if set, otherwise `data/slurm.duckdb`.

## Historical ingestion: two methods

### Method 1 — Query Slurm directly

Use this when the webapp host can run `sacct` and contact the accounting service:

```bash
python manage.py initialize_slurm --start 2021-01-01 --chunk-days 7
```

The default start date is `1970-01-01`; if `--end` is omitted, the current UTC time is used. For a specific range:

```bash
python manage.py initialize_slurm \
  --start 2021-01-01 \
  --end 2026-01-01 \
  --chunk-days 3
```

Options include `--clusters` (default `all`), `--timeout` (default `1800` seconds), `--no-accounts`, and `--verbose`. The `--chunk-days` option controls the size of each live `sacct` request. If SlurmDBD reports `slurm_pack_list: size limit exceeded`, reduce the chunk size. If errors persist, use the file-based method.

### Method 2 — Collect `sacct` files, then import them

This is useful for remote clusters, large histories, repeatable imports, and archiving raw accounting output.

Collect history on a machine with access to the target Slurm cluster:

```bash
python -m slurm_analytics.collect_sacct \
  --start 2021-01-01 \
  --end 2026-01-01 \
  --output-dir /data/cluster-history \
  --chunk-days 3
```

The `--end` value is exclusive. The collector creates `.out` files and `.out.json` metadata sidecars. Verify the `SACCT` executable path in `slurm_analytics/collect_sacct.py` for your environment. Transfer both the output files and sidecars to the webapp host, then import:

```bash
python manage.py initialize_slurm \
  --history-file "/data/cluster-history/*.out"
```

Wildcards are supported even when quoted. You can also list individual files. In file-import mode, `--start` and `--end` set initialization metadata; they do not filter the records inside the files.

## Installation

Requirements include Python 3.8+, Django, Django REST Framework, Polars, DuckDB, and PyYAML. The environment also needs `sacct` and, when collecting account hierarchy, `sacctmgr`.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e .
```

For Apptainer/Singularity deployments, run commands inside the application container and ensure Slurm binaries, configuration, and required library bindings are available there.

## Run the dashboard

```bash
python manage.py check
python manage.py runserver 127.0.0.1:8666
```

Open `http://127.0.0.1:8666/`. For multiple clusters, open `/clusters/<slug>/`. The checked-in Django settings are development-oriented; do not expose Django's development server directly to untrusted networks. Configure `DEBUG`, `ALLOWED_HOSTS`, authentication, TLS, and a production WSGI server before production use.

## Incremental refresh for live clusters

For the default/live database, update recent accounting data with:

```bash
python manage.py refresh_slurm_db
```

This uses the saved initialization cursor and an overlap window. You can supply `--start`, `--end`, `--chunk-days`, `--overlap-minutes`, `--clusters`, and `--no-accounts`. For a non-default database, set `SLURM_DUCKDB_PATH` to that database when running the command.

`refresh_slurm` is a separate command that refreshes the live in-memory snapshot/cache and local node inventory. Use `refresh_slurm_db` for persistent historical accounting updates.

## YAML reports

After initializing a database:

```bash
slurm-analytics report accounts --output accounts.yaml
slurm-analytics report users --output users.yaml
```

Or use `python -m slurm_analytics.cli report accounts --output accounts.yaml`. For a non-default cluster, set `SLURM_DUCKDB_PATH` to its DuckDB file before running the report. Report filters may include `--start`, `--end`, and `--user` where supported.

## Data storage and safety

- Persistent accounting records live in DuckDB; Django's `db.sqlite3` is a separate database.
- `SLURM_DUCKDB_PATH` selects the database for management commands and standalone reports.
- Each configured cluster must have a unique database path.
- Do not commit production databases, raw accounting files, secrets, or cluster config containing sensitive paths/access details to Git.
- Back up DuckDB before reinitializing or repairing historical data.

## Troubleshooting

### Dashboard says data is not initialized

Confirm the right database was initialized and that its `db_path` matches the cluster configuration. The dashboard and import command must point to the same DuckDB file.

### `slurm_pack_list: size limit exceeded`

Reduce `--chunk-days` for live ingestion, or collect history in smaller windows and use file-based import.

### Missing account hierarchy

The initializer queries `sacctmgr` unless `--no-accounts` is supplied. Confirm access to `sacctmgr`, or intentionally skip the hierarchy for a remote cluster.

### GPU totals look doubled

In `AllocTRES`, generic `gres/gpu` and typed fields such as `gres/gpu:h200` can describe the same allocation. The parser prefers the generic total if present and otherwise sums typed fields. If a database was created with an older parser, back it up and rerun initialization with the corrected code.


## Remote cluster export without installing this application

A remote cluster only needs its existing Slurm commands (`sacct`, `sacctmgr`,
`scontrol`) and Python 3.6 or newer. Copy `tools/export_slurm_cluster.py` to that
cluster (or use the same script from a shared filesystem), then run:

```bash
python3 export_slurm_cluster.py \
  --start 2020-01-01 --end 2026-10-11 \
  --output-dir /tmp/cluster2-export --chunk-days 3
```

This creates raw `sacct_*.out` files and `.out.json` field sidecars, plus
`accounts.json` (account hierarchy and associations) and `nodes.json` (the
cluster's `scontrol show node -a` output). It does not install Django, DuckDB,
Polars, or any dashboard dependencies on the remote cluster. Use a short
`--chunk-days` (for example 1-3) for large histories or a busy slurmdbd.

The exporter deliberately uses the same `FIELDS` list and ordering as the last
working single-cluster collector. It omits `group`, `gid`, `uid`,
`tresusageintot`, and `maxvmsize`; `jobname` is intentionally the final field.
The `.out.json` sidecars record this exact field list so the importer can map
columns correctly. Do not reorder or add fields without updating the metadata
and ensuring the importer understands them.

Transfer the export directory to the dashboard host, then initialize that
cluster's configured DuckDB file. For example:

```bash
export SLURM_DUCKDB_PATH=/workspace/data/cluster2.duckdb
python /workspace/manage.py initialize_slurm \
  --history-file '/workspace/import/cluster2/sacct_*.out' \
  --accounts-file /workspace/import/cluster2/accounts.json \
  --nodes-file /workspace/import/cluster2/nodes.json \
  --start 1970-01-01 --end 2026-10-11
```

The quoted history wildcard is expanded by the management command. Keep each
`.out.json` sidecar beside its `.out` file. Account hierarchy and node inventory
are stored in the selected cluster's DuckDB. Mark that cluster as `"mode":
"import"` in `clusters.json` so dashboard GPU-model inference reads its imported
node inventory instead of querying the web host's local Slurm controller.


