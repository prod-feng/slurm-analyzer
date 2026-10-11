Slurm historical jobs analyzer

Python3, Django, Polars, DuckDB, Singularity.

# Slurm Analytics

A Django-based analytics dashboard and command-line toolkit for Slurm accounting data. It ingests `sacct` job and job-step records, stores normalized historical data in DuckDB, and provides time-series, account, user, GPU, and resource-usage reporting.

## Features

- Historical Slurm accounting ingestion from either a live `sacct` query or previously collected `sacct` output files.
- Persistent DuckDB storage for jobs and job steps.
- Incremental refreshes after the initial historical import.
- Django dashboard and JSON API for exploring resource usage over time.
- Account hierarchy and association data collected through `sacctmgr`.
- Standalone YAML reports for account and user usage.
- GPU allocation parsing that treats generic and typed GPU TRES fields as representations of the same allocation when both are present.

## How historical ingestion works

There are **two supported ways** to populate the persistent database. Choose one for the initial import. Both methods use the same ingestion and normalization pipeline.

### Method 1 — Query Slurm directly with `sacct`

Use this when the application environment can run `sacct` and contact your Slurm accounting service.

From the project root (the directory containing `manage.py`):

```bash
python manage.py initialize_slurm --start 2021-01-01 --chunk-days 31
```

This imports from January 1, 2021 through the current UTC time. Change the start date to the earliest date you need. If `--end` is omitted, the command uses the current UTC time.

Useful options:

| Option | Meaning |
|---|---|
| `--start YYYY-MM-DD` | Earliest accounting timestamp. Defaults to `1970-01-01`. |
| `--end YYYY-MM-DD` | Latest accounting timestamp. Defaults to current UTC time. |
| `--chunk-days N` | Number of days in each `sacct` request. Defaults to `31`. |
| `--clusters all` | Cluster(s) to query. Defaults to `all`. |
| `--timeout SECONDS` | Timeout per query. Defaults to `1800`. |
| `--no-accounts` | Skip refreshing account and association hierarchy. |
| `--verbose` | Print raw suspicious input rows when diagnosing import issues. |

For large or older histories, reduce the chunk size if the accounting service struggles with large responses. For example:

```bash
python manage.py initialize_slurm \
  --start 2021-01-01 \
  --end 2026-01-01 \
  --chunk-days 3
```

Initialization reports the time range being imported and the number of job and job-step records. Progress is recorded after each successfully imported date chunk, so the database metadata indicates how far the initialization has progressed.

**SlurmDBD note:** Large historical requests can fail with messages such as `slurm_pack_list: size limit exceeded`. Smaller `--chunk-days` values may help reduce response sizes. If the service still rejects requests, use Method 2 to collect smaller files and import them.

### Method 2 — Import pre-collected historical `sacct` files

Use this when you want to collect history separately from the application, preserve raw output for auditing, or avoid one very large SlurmDBD query. This method has two stages.

#### Stage A: Collect history into files

The repository includes `slurm_analytics/collect_sacct.py`. It runs `sacct` in small date windows and writes `.out` files plus adjacent `.out.json` metadata sidecars. The sidecars record the output fields and are important for safely interpreting the files.

Example:

```bash
python -m slurm_analytics.collect_sacct \
  --start 2021-01-01 \
  --end 2026-01-01 \
  --output-dir /data/slurm-history \
  --chunk-days 3
```

`--end` is the exclusive end of the collection range. `--chunk-days` defaults to `3`. Ensure the `SACCT` executable path in `slurm_analytics/collect_sacct.py` matches your environment before running this script; its current default is `/cm/shared/apps/slurm/current/bin/sacct`.

Keep each generated `.out` file with its matching `.out.json` sidecar. Do not edit the field order in the files unless you also preserve/update the metadata.

#### Stage B: Import the files into DuckDB

Run from the project root, using paths visible inside the environment where the Django command runs:

```bash
python manage.py initialize_slurm \
  --history-file "/data/slurm-history/*.out"
```

The importer expands wildcard patterns. You can also specify files explicitly:

```bash
python manage.py initialize_slurm \
  --history-file /data/slurm-history/part-1.out /data/slurm-history/part-2.out
```

Files may overlap; ingestion uses record-key upserts to avoid treating overlapping collection windows as unrelated jobs. The `--start` and `--end` values in file-import mode are initialization metadata; they **do not filter the records inside the supplied files**.

If account hierarchy information should not be queried during import (for example, the import environment does not have access to `sacctmgr`), use:

```bash
python manage.py initialize_slurm \
  --history-file "/data/slurm-history/*.out" \
  --no-accounts
```

You can troubleshoot malformed or unexpected file rows with:

```bash
python manage.py initialize_slurm \
  --history-file "/data/slurm-history/*.out" \
  --verbose
```

### Which method should I use?

- **Method 1 (live query):** simplest when `sacct` and SlurmDBD work reliably for the desired date range.
- **Method 2 (file-based):** preferable for very long histories, repeatable imports, archiving raw accounting output, or environments where collecting and importing should be separate operations.

Avoid running two initializations or a refresh against the same DuckDB file at the same time.

## Incremental updates after initialization

After the historical import succeeds, use `refresh_slurm_db` for ongoing updates. It resumes from the saved `initialized_through` cursor and queries an overlapping interval so recent records can be updated.

```bash
python manage.py refresh_slurm_db
```

By default, it re-queries the last 60 minutes and proceeds in chunks of up to 7 days. You can override the date range and chunk size:

```bash
python manage.py refresh_slurm_db \
  --start 2026-10-01 \
  --end 2026-10-08 \
  --chunk-days 2
```

Useful options include `--overlap-minutes` (default `60`), `--chunk-days` (default `7`), `--clusters` (default `all`), `--timeout` (default `1800`), and `--no-accounts`.

There is also a separate `refresh_slurm` command that refreshes the live snapshot/cache used by the web application:

```bash
python manage.py refresh_slurm
```

For the persistent historical accounting store, use **`refresh_slurm_db`** after running **`initialize_slurm`**.

## Installation and requirements

Requirements listed in `requirements.txt` include:

- Python 3.8+
- Django 4.2 through 5.x
- Django REST Framework
- Polars
- DuckDB
- PyYAML

The environment also needs access to the Slurm client commands (`sacct`; and `sacctmgr` if account hierarchy collection is enabled).

Create/activate a virtual environment if appropriate for your cluster, then install dependencies from the project root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e .
```

The editable install makes the `slurm-analytics` console command available. If the application runs inside Apptainer/Singularity, execute these commands inside the same container/environment that has the project files, Slurm clients, and required library bindings.

## Run the web application

From the project root:

```bash
python manage.py check
python manage.py runserver 127.0.0.1:8666
```

Open `http://127.0.0.1:8666/` in a browser. For access from another machine, bind to an appropriate interface and configure `ALLOWED_HOSTS`, firewall rules, and deployment security for your environment. The checked-in Django settings are development-oriented (`DEBUG = True`); do not expose the development server directly to an untrusted network.

For an Apptainer deployment, run `manage.py` inside the configured container and make sure the source directory, Slurm client binaries, accounting configuration, and required libraries are available in that container.

## Persistent data and database location

The persistent accounting database is DuckDB. By default it is created at:

```text
data/slurm.duckdb
```

Override the location with `SLURM_DUCKDB_PATH`:

```bash
export SLURM_DUCKDB_PATH=/path/to/slurm.duckdb
```

Set the variable consistently for initialization, refresh, report generation, and the web application so all components use the same database. The database contains operational accounting data and should not normally be committed to Git; back it up according to your site's data-retention policy.

The Django application's own SQLite database (`db.sqlite3`) is separate from the DuckDB accounting store.

## Generate YAML reports

After the DuckDB database has been initialized, generate standalone reports from the project root.

Account usage:

```bash
slurm-analytics report accounts --output accounts.yaml
```

User usage:

```bash
slurm-analytics report users --output users.yaml
```

You can also invoke the module without installing the console script:

```bash
python -m slurm_analytics.cli report accounts --output accounts.yaml
python -m slurm_analytics.cli report users --output users.yaml
```

Optional report filters:

```bash
slurm-analytics report accounts \
  --start 2026-01-01 \
  --end 2026-10-01 \
  --output accounts-2026.yaml

slurm-analytics report users \
  --start 2026-01-01 \
  --end 2026-10-01 \
  --user 'jdoe*' \
  --output users-2026.yaml
```

The `--end` date/time is interpreted by the report implementation; use ISO-formatted dates or timestamps. Reports are generated from the stored data and do not query Slurm directly.

## Tests

Run the test suite from the project root:

```bash
python -m unittest discover -s tests
```

If your checkout uses a different test runner or test configuration, follow the project's CI configuration.

## Troubleshooting

### No historical data appears

1. Confirm initialization completed successfully.
2. Check that the web application and commands use the same `SLURM_DUCKDB_PATH`.
3. Run `python manage.py refresh_slurm_db` after initialization if you need to update recent records.
4. Check the command output for query failures or malformed historical rows.

### SlurmDBD reports `slurm_pack_list: size limit exceeded`

Try a smaller `--chunk-days` value for live queries. If large requests still fail, collect history in smaller windows with Method 2 and import the generated files.

### Account hierarchy is missing

Initialization refreshes accounts and associations through `sacctmgr` unless `--no-accounts` is specified. Confirm `sacctmgr` is available and the current environment has permission to query the accounting database, then rerun initialization or a database refresh without `--no-accounts`.

### GPU totals look doubled

Slurm's `AllocTRES` may contain both a generic GPU field (for example, `gres/gpu=4`) and typed GPU fields (for example, `gres/gpu:h200=4`). These can describe the same allocation, not separate allocations. The parser prefers the generic total when present and otherwise sums typed GPU fields. If a database was populated by an older parser, rerun `initialize_slurm` with the corrected code to recalculate stored values, after taking a database backup.

## Typical first-time workflow

For a direct import:

```bash
python manage.py initialize_slurm --start 2021-01-01 --chunk-days 7
python manage.py refresh_slurm_db
python manage.py runserver 127.0.0.1:8666
```

For a file-based import:

```bash
python -m slurm_analytics.collect_sacct \
  --start 2021-01-01 \
  --end 2026-01-01 \
  --output-dir /data/slurm-history \
  --chunk-days 3

python manage.py initialize_slurm \
  --history-file "/data/slurm-history/*.out"

python manage.py refresh_slurm_db
python manage.py runserver 127.0.0.1:8666
```

Adjust the date range and paths to your cluster. For very large histories, start with smaller chunks and confirm the import summary before scheduling regular refreshes.

