# Slurm Analytics

A Django-based dashboard and CLI for exploring Slurm accounting history, resource usage, accounts, users, partitions, GPU models, and time-series metrics. Historical job and job-step records are normalized and stored in DuckDB.

<img width="" height="268" alt="Screenshot 2026-10-11 at 2 16 33 PM" src="https://github.com/user-attachments/assets/edc3654e-9659-43f6-90c7-a326a2751b67" />

<img width="283" height="268" alt="Screenshot 2026-10-11 at 2 19 23 PM" src="https://github.com/user-attachments/assets/612a887b-13fd-4281-8261-86667a43dc00" />


<img width="" height="268" alt="Screenshot 2026-10-11 at 2 15 34 PM" src="https://github.com/user-attachments/assets/de8dc520-17b1-4cbc-a39c-7b2b93b00ce5" />

## Quick Start
For a quick start, I will use the Singularity container to deploy, test, and work in production.
1. go to ```appraiser/``` folder,

```
singularity build --fakeroot   slurm-manager-ldap-django.sif  slurm-manager.def
```
2. Add your local IP address in to ```ALLOWED_HOSTS ``` of file ```apps/slurm_web/settings.py```.

```
ALLOWED_HOSTS = [
    "127.0.0.1",
    "localhost",
    "x.x.x.x",
]
```
3. Export Slurm data, please see ```"Data flow at a glance"``` below.
4. Initialize import Slurm data into DuckDB.

```
./run_me_initialize.sh
```
5. Start the app in development mode

```
./run_me.sh start
```
Finally, 

6. Run in production using ```Gunicorn```

```
./run_production.sh
```
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

#### Data flow at a glance

For a remote cluster, the workflow has three stages:

1. **Export on the source cluster:** run `tools/export_slurm_cluster.py` on a host that can access that cluster's Slurm commands. It collects historical `sacct` records in chunks and exports the current account/association, node, and partition metadata.
   
   This ```tools/export_slurm_cluster.py``` is a ```standalone``` ```python 3.6``` compatible script, meaning you can copy it to ```anywhere``` to run, ```without``` installing the whole package.
   Please be careful about the ```--chunk-days```, which will request ```xxx``` days' data at a time. If your cluster if very big, you might need to set it to be
   shorter, like 10 days, otherwise it might take very long time, and also might get timeout from the local database and fails.

   Also, please do not change the ```FIELDS``` randomly inside this script, it must be the same as the ```SACCT_FIELDS``` in source file ```apps/slurm_analytics/sacct.py```.
   
```
 time ./apps/tools/export_slurm_cluster.py  --start 2025-05-01   --end 2026-10-09   --chunk-days 90 --output-dir ./apps/rawdata/
```
2. **Transfer the export:** copy the generated files to the dashboard host, keeping each cluster's files in its own directory. Keep every `sacct_*.out` file with its matching `.out.json` sidecar.
3. **Import into that cluster's database:** run `initialize_slurm` for the first import, then `refresh_slurm_db` with newly collected files for routine updates. Set `SLURM_DUCKDB_PATH` to the database configured for that cluster.

In short: **source Slurm cluster → export files → dashboard host → cluster-specific DuckDB database**. The detailed export, initial-import, and update commands are documented in [Importing and updating a cluster database](#importing-and-updating-a-cluster-database) below.

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

For routine updates, use `refresh_slurm_db` against the **same DuckDB file** that was initialized for the cluster. Collect a recent overlapping date range, transfer the new accounting files and current metadata exports, then import them together:

```bash
SLURM_DUCKDB_PATH=/var/lib/slurm-analytics/cluster2.duckdb \
  python manage.py refresh_slurm_db \
  --history-file "/data/cluster2-export/sacct_*.out" \
  --accounts-file /data/cluster2-export/accounts.json \
  --nodes-file /data/cluster2-export/nodes.json \
  --partitions-file /data/cluster2-export/partitions.json \
  --start 2026-10-10 --end 2026-10-13
```

`refresh_slurm_db --history-file` delegates to the existing `initialize_slurm` importer; it does not introduce a second parser or a new schema. Existing job and step record keys are upserted, so overlapping files can be imported repeatedly without duplicating those records. The account/association, node, and partition inventory imports update their current metadata snapshots; they do not remove old job history. If metadata files are not supplied, they are not refreshed by this file-based invocation. For remote clusters, include the remote cluster's `accounts.json`, `nodes.json`, and `partitions.json` rather than querying the dashboard host's local Slurm installation.

The `--start` and `--end` options set import metadata/cursor values; in file-import mode they **do not filter records inside the `.out` files**. The actual accounting range is determined by the files you collected. Keep each `.out.json` sidecar next to its `.out` file. Do not run multiple writers against the same DuckDB file concurrently. File-import clusters do not update automatically: schedule or manually run collection, transfer, and import when you want refreshed data.

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


## Importing and updating a cluster database

This workflow uses the current collector, export formats, importer, and DuckDB tables. There is no separate remote-cluster database implementation: collect on a machine that can access the target Slurm cluster, transfer the resulting files, and run the existing Django management command against the target cluster's DuckDB file.

### A. Collect data on the Slurm cluster

A remote cluster does not need this Django application or its DuckDB/Polars dependencies installed. It needs Python 3.6+ and access to `sacct`, `sacctmgr`, and `scontrol`. Copy `tools/export_slurm_cluster.py` to the cluster or run it from a shared filesystem:

```bash
python3 export_slurm_cluster.py \
  --start 2020-01-01 \
  --end 2026-10-13 \
  --output-dir /tmp/cluster2-export \
  --chunk-days 3
```

`--start` is inclusive and `--end` is exclusive. Choose a manageable `--chunk-days` (often 1–3 days for busy clusters or older SlurmDBD instances). The exporter writes:

| File | Contents |
|---|---|
| `sacct_*.out` | Raw parsable Slurm accounting output, split into date chunks |
| `sacct_*.out.json` | The matching file's exact field list and collection metadata; required to map columns correctly |
| `accounts.json` | Account hierarchy and associations exported using `sacctmgr` |
| `nodes.json` | Current node inventory from `scontrol show node -a` |
| `partitions.json` | Current partition inventory from `scontrol show partition -o` |

The exporter supports `--skip-jobs`, `--skip-accounts`, `--skip-nodes`, and `--skip-partitions` if you intentionally want to omit a data category. For a full cluster import, do not use those flags. Keep every `.out.json` sidecar beside its corresponding `.out` file. Do not manually reorder fields in the sidecar or change the exporter's `FIELDS` list unless the importer is updated to match.

Transfer the whole export directory to the dashboard host, for example with `scp` or your normal file-transfer mechanism. Keep the exports separated by cluster so that files from different clusters are never mixed.

### B. Initial import into a cluster's DuckDB database

Set `SLURM_DUCKDB_PATH` to the database configured for the target cluster. The path must be writable by the user running Django. Run the command inside the application container/environment so the project dependencies are available:

```bash
export SLURM_DUCKDB_PATH=/workspace/data/cluster2.duckdb

python /workspace/manage.py initialize_slurm \
  --history-file '/workspace/import/cluster2/sacct_*.out' \
  --accounts-file /workspace/import/cluster2/accounts.json \
  --nodes-file /workspace/import/cluster2/nodes.json \
  --partitions-file /workspace/import/cluster2/partitions.json \
  --start 1970-01-01 \
  --end 2026-10-13
```

The quoted wildcard is expanded by the management command. It imports each matched history file once and uses the matching `.out.json` sidecar to interpret its columns. `--start`/`--end` describe the initialization range in database metadata; they do not filter the contents of the supplied files. Ensure the export files themselves cover the history you intend to import.

Supplying `--accounts-file` is important for remote clusters: it imports that cluster's account hierarchy and associations rather than accidentally querying the dashboard host's local `sacctmgr`. Likewise, the node and partition JSON files populate that cluster's current inventory snapshots. If you intentionally have no account export, add `--no-accounts` to avoid querying local `sacctmgr`; be aware that hierarchical account views may then be incomplete. Node and partition inventory will not be refreshed unless their respective files are supplied.

### C. Routine update of the same database

For later updates, **do not run the initial full-history collection every time**. Collect a recent range with an overlap so recently completed jobs and accounting corrections are picked up. For example, collect October 10 through October 13 (the end date is exclusive), transfer the newly generated `.out` files and the latest metadata JSON files, then run:

```bash
export SLURM_DUCKDB_PATH=/workspace/data/cluster2.duckdb

python /workspace/manage.py refresh_slurm_db \
  --history-file '/workspace/import/cluster2/sacct_*.out' \
  --accounts-file /workspace/import/cluster2/accounts.json \
  --nodes-file /workspace/import/cluster2/nodes.json \
  --partitions-file /workspace/import/cluster2/partitions.json \
  --start 2026-10-10 \
  --end 2026-10-13
```

Use the newly collected files or a directory containing only the intended update files. Do not point the update command at an archive of all years of `.out` files unless you deliberately want to reimport that archive. Reimporting overlapping records is supported, but unnecessary full-history imports take longer.

The file-based `refresh_slurm_db` path delegates to `initialize_slurm`, reusing the existing parser and database upsert logic:

- New job and step records are inserted.
- Existing matching job and step records are updated rather than duplicated.
- Supplied account/association, node, and partition metadata refreshes the corresponding current snapshots.
- Historical job and step rows outside the new collection window are preserved.
- Metadata files omitted from the command are left unchanged by that file-based invocation.

The `--start` and `--end` flags do not filter records inside the files; they set initialization metadata. The collector's date range controls which records are actually present in the export. Keep the cluster's `SLURM_DUCKDB_PATH` consistent with its `db_path` in `clusters.json`. Never import a remote cluster's files into another cluster's database.

### D. Update a local cluster directly

If the application host can query the local Slurm cluster, use the existing incremental database refresh without file arguments:

```bash
python /workspace/manage.py refresh_slurm_db \
  --overlap-minutes 60 \
  --chunk-days 3
```

The command uses the saved `initialized_through` cursor, re-queries an overlap window, and updates persistent job and step accounting. On the first run, initialize the database first. For an explicit repair range, supply `--start` and `--end`; reduce `--chunk-days` if SlurmDBD reports `slurm_pack_list: size limit exceeded`.

`refresh_slurm_db` updates the persistent historical accounting database. `refresh_slurm` is a separate command for the live in-memory snapshot/cache and local node inventory. Keep those roles distinct.

### E. Verify the import

After a successful import, check the command summary for job/step counts and metadata import messages. Then confirm that the dashboard cluster route uses the same DuckDB file. You can also run an existing YAML report against that database:

```bash
SLURM_DUCKDB_PATH=/workspace/data/cluster2.duckdb \
  python -m slurm_analytics.cli report accounts --output /workspace/accounts.yaml
```

Back up the database before a large initial import or historical repair. Do not run concurrent import/refresh writers against the same DuckDB file. If an import fails, inspect the error before retrying and verify that the correct database path and matching sidecar files were used.

