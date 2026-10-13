"""Standalone account and user resource analytics over DuckDB."""
from __future__ import annotations

import polars as pl

from .storage import connect, fetch_polars


SORT_COLUMNS = {
    "name": "name",
    "account": "name",
    "user": "name",
    "jobs": "jobs",
    "nodes": "allocated_nodes",
    "allocated_nodes": "allocated_nodes",
    "cpu_hours": "cpu_hours",
    "gpu_hours": "gpu_hours",
    "memory_gb_hours": "memory_gb_hours",
    "gpu_memory_gb": "gpu_memory_gb",
    "elapsed_hours": "elapsed_hours",
    "tasks": "total_tasks",
    "total_tasks": "total_tasks",
    "disk_read_gib": "disk_read_gib",
    "disk_write_gib": "disk_write_gib",
}


def _like(pattern):
    return pattern.replace("*", "%").replace("?", "_") if pattern else "%"


def _order_by(sort, direction, name_alias="name"):
    column = SORT_COLUMNS.get(sort or "cpu_hours", "cpu_hours")
    if column == "name":
        column = name_alias
    direction = "ASC" if str(direction).lower() == "asc" else "DESC"
    return "{} {} NULLS LAST".format(column, direction)


def _usage_sql(table, name_column, clauses):
    """Build per-allocation resource metrics, falling back to step telemetry.

    Some Slurm versions/clusters leave MaxDiskRead/MaxDiskWrite empty or zero
    on the allocation row while recording them on job steps. For each job we
    prefer a positive allocation-row value; otherwise use the maximum reported
    value across its steps. This preserves the meaning of Slurm's ``Max*``
    fields and avoids adding the same job's allocation and step telemetry twice.
    Task count similarly falls back to the maximum step NTasks only when the
    allocation-row NTasks value is missing or zero.
    """
    where = " AND ".join(clauses)
    memory_bytes = r"""(
        COALESCE(TRY_CAST(regexp_extract(j.reqmem, '^([0-9]+(?:\.[0-9]+)?)[KMGTPE]?[cn]?$', 1) AS DOUBLE), 0)
        * CASE upper(regexp_extract(j.reqmem, '^[0-9]+(?:\.[0-9]+)?([KMGTPE]?)', 1))
            WHEN 'K' THEN 1024.0
            WHEN 'M' THEN 1048576.0
            WHEN 'G' THEN 1073741824.0
            WHEN 'T' THEN 1099511627776.0
            WHEN 'P' THEN 1125899906842624.0
            WHEN 'E' THEN 1152921504606846976.0
            ELSE 1.0
          END
        * CASE lower(regexp_extract(j.reqmem, '([cn])$', 1))
            WHEN 'c' THEN COALESCE(TRY_CAST(j.ncpus AS DOUBLE), 0)
            ELSE COALESCE(TRY_CAST(j.nnodes AS DOUBLE), 0)
          END
    )"""
    schema_con = connect()
    try:
        available_columns = {row[0].lower() for row in schema_con.execute("DESCRIBE jobs").fetchall()}
        try:
            step_columns = {row[0].lower() for row in schema_con.execute("DESCRIBE job_steps").fetchall()}
        except Exception:
            step_columns = set()
    finally:
        schema_con.close()

    def disk_gib(column, alias, columns):
        if column.lower() not in columns:
            return "NULL::DOUBLE"
        value = "TRY_CAST(regexp_extract(upper(CAST({0}.{1} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 1) AS DOUBLE)".format(alias, column)
        unit = "regexp_extract(upper(CAST({0}.{1} AS VARCHAR)), '^([0-9]+(?:\\.[0-9]+)?)[ ]*([KMGTPE]?)', 2)".format(alias, column)
        return "(({value}) * CASE {unit} WHEN 'K' THEN 1.0/1048576.0 WHEN 'M' THEN 1.0/1024.0 WHEN 'G' THEN 1.0 WHEN 'T' THEN 1024.0 WHEN 'P' THEN 1048576.0 WHEN 'E' THEN 1073741824.0 ELSE 1.0/1073741824.0 END)".format(value=value, unit=unit)

    job_read = disk_gib("maxdiskread", "j", available_columns)
    job_write = disk_gib("maxdiskwrite", "j", available_columns)
    step_read = disk_gib("maxdiskread", "js", step_columns)
    step_write = disk_gib("maxdiskwrite", "js", step_columns)
    step_tasks = "TRY_CAST(js.ntasks AS DOUBLE)" if "ntasks" in step_columns else "NULL::DOUBLE"
    if "canonical_job_id" in step_columns and "canonical_job_id" in available_columns:
        step_io_sql = """SELECT js.canonical_job_id,
                   MAX({step_read}) AS disk_read_gib,
                   MAX({step_write}) AS disk_write_gib,
                   MAX({step_tasks}) AS max_ntasks
            FROM job_steps js
            JOIN filtered_jobs fj ON fj.canonical_job_id = js.canonical_job_id
            GROUP BY js.canonical_job_id""".format(
                step_read=step_read,
                step_write=step_write,
                step_tasks=step_tasks,
            )
    else:
        step_io_sql = "SELECT NULL::VARCHAR AS canonical_job_id, NULL::DOUBLE AS disk_read_gib, NULL::DOUBLE AS disk_write_gib, NULL::DOUBLE AS max_ntasks WHERE FALSE"

    job_tasks = "TRY_CAST(j.ntasks AS DOUBLE)" if "ntasks" in available_columns else "NULL::DOUBLE"
    tasks_expr = "COALESCE(NULLIF({job_tasks}, 0), step_io.max_ntasks, 0.0)".format(job_tasks=job_tasks)
    read_gib = "CASE WHEN COALESCE({job_read}, 0) > 0 THEN {job_read} ELSE COALESCE(step_io.disk_read_gib, {job_read}, 0.0) END".format(job_read=job_read)
    write_gib = "CASE WHEN COALESCE({job_write}, 0) > 0 THEN {job_write} ELSE COALESCE(step_io.disk_write_gib, {job_write}, 0.0) END".format(job_write=job_write)
    return """
        WITH filtered_jobs AS (
            SELECT * FROM {table} WHERE {where}
        ), step_io AS ({step_io_sql})
        SELECT {name_column} AS name,
               COUNT(*) AS jobs,
               SUM(COALESCE(TRY_CAST(j.nnodes AS DOUBLE), 0)) AS allocated_nodes,
               SUM(COALESCE(TRY_CAST(j.ncpus AS DOUBLE), 0) * COALESCE(TRY_CAST(j.elapsed_seconds AS DOUBLE), 0)) / 3600.0 AS cpu_hours,
               SUM(COALESCE(TRY_CAST(j.gpu_count AS DOUBLE), 0) * COALESCE(TRY_CAST(j.elapsed_seconds AS DOUBLE), 0)) / 3600.0 AS gpu_hours,
               SUM(({memory_bytes} / 1073741824.0) * COALESCE(TRY_CAST(j.elapsed_seconds AS DOUBLE), 0)) / 3600.0 AS memory_gb_hours,
               MAX(COALESCE(TRY_CAST(j.gpu_mem_max_bytes AS DOUBLE), 0)) / 1073741824.0 AS gpu_memory_gb,
               SUM(COALESCE(TRY_CAST(j.elapsed_seconds AS DOUBLE), 0)) / 3600.0 AS elapsed_hours,
               SUM(COALESCE({tasks_expr}, 0)) AS total_tasks,
               SUM(COALESCE({read_gib}, 0.0)) AS disk_read_gib,
               SUM(COALESCE({write_gib}, 0.0)) AS disk_write_gib
        FROM filtered_jobs j
        LEFT JOIN step_io ON step_io.canonical_job_id = j.canonical_job_id
        GROUP BY {name_column}
    """.format(
        name_column=name_column, table=table, where=where, step_io_sql=step_io_sql,
        memory_bytes=memory_bytes, read_gib=read_gib, write_gib=write_gib,
        tasks_expr=tasks_expr,
    )


def _round_metrics(records):
    """Round displayed resource metrics to two decimal places."""
    metrics = (
        "allocated_nodes", "cpu_hours", "gpu_hours",
        "memory_gb_hours", "gpu_memory_gb", "elapsed_hours",
        "total_tasks",
    )
    precise_metrics = ("disk_read_gib", "disk_write_gib")
    for record in records:
        for key in metrics:
            value = record.get(key)
            if value is not None:
                record[key] = round(float(value), 2)
        # Keep sub-centi-GiB usage for pie-share calculations. UI tables format
        # values separately, while rounding each user/account to 0.01 GiB can
        # erase small but valid I/O totals entirely.
        for key in precise_metrics:
            value = record.get(key)
            if value is not None:
                record[key] = round(float(value), 6)
    return records


def account_usage(start=None, end=None, account=None, limit=100, sort="cpu_hours", direction="desc", round_metrics=True):
    """Return current accounts with historical job usage.

    ``accounts`` is the current sacctmgr state. Historical ``jobs.account``
    values are never rewritten. ``Unknown`` is the one special historical
    bucket for jobs whose accounting record has no account value.
    """
    con = connect()
    try:
        usage_where = ["1=1"]
        usage_params = []
        if start:
            usage_where.append("j.start_time >= ?")
            usage_params.append(start)
        if end:
            usage_where.append("j.start_time < ?")
            usage_params.append(end)
        if account:
            usage_where.append("COALESCE(NULLIF(TRIM(j.account), ''), 'Unknown') LIKE ?")
            usage_params.append(_like(account))

        base_sql = _usage_sql(
            "jobs j",
            "COALESCE(NULLIF(TRIM(j.account), ''), 'Unknown')",
            usage_where,
        )
        sql = """
            WITH usage AS ({base_sql}),
            current_accounts AS (
                SELECT account_name AS account, parent_name AS parent, level, path
                FROM accounts
                {account_filter}
            ),
            combined AS (
                SELECT ca.account, ca.parent, ca.level, ca.path,
                       COALESCE(u.jobs, 0) AS jobs,
                       COALESCE(u.allocated_nodes, 0.0) AS allocated_nodes,
                       COALESCE(u.cpu_hours, 0.0) AS cpu_hours,
                       COALESCE(u.gpu_hours, 0.0) AS gpu_hours,
                       COALESCE(u.memory_gb_hours, 0.0) AS memory_gb_hours,
                       COALESCE(u.gpu_memory_gb, 0.0) AS gpu_memory_gb,
                       COALESCE(u.elapsed_hours, 0.0) AS elapsed_hours,
                       COALESCE(u.total_tasks, 0.0) AS total_tasks,
                       COALESCE(u.disk_read_gib, 0.0) AS disk_read_gib,
                       COALESCE(u.disk_write_gib, 0.0) AS disk_write_gib
                FROM current_accounts ca
                LEFT JOIN usage u ON u.name = ca.account
                UNION ALL
                SELECT u.name AS account, NULL AS parent, 0 AS level, u.name AS path,
                       u.jobs, u.allocated_nodes, u.cpu_hours, u.gpu_hours,
                       u.memory_gb_hours, u.gpu_memory_gb, u.elapsed_hours,
                       u.total_tasks, u.disk_read_gib, u.disk_write_gib
                FROM usage u
                WHERE u.name = 'Unknown'
                  AND NOT EXISTS (SELECT 1 FROM accounts WHERE account_name = 'Unknown')
            )
            SELECT * FROM combined
            ORDER BY {order_by}
            LIMIT ?
        """.format(
            base_sql=base_sql,
            account_filter=("WHERE account_name LIKE ?" if account else ""),
            order_by=_order_by(sort, direction, "account"),
        )
        final_params = list(usage_params)
        if account:
            final_params.append(_like(account))
        final_params.append(limit)
        result = fetch_polars(con, sql, final_params)
        if round_metrics:
            result = result.with_columns([pl.col(c).round(2) for c in ("allocated_nodes", "cpu_hours", "gpu_hours", "memory_gb_hours", "gpu_memory_gb", "elapsed_hours", "total_tasks") if c in result.columns])
            result = result.with_columns([pl.col(c).round(6) for c in ("disk_read_gib", "disk_write_gib") if c in result.columns])
        return result
    finally:
        con.close()

def user_usage(start=None, end=None, user=None, account=None, limit=100, sort="cpu_hours", direction="desc", round_metrics=True):
    """Return usage for CURRENT Slurm users, left-joined to historical jobs."""
    con = connect()
    try:
        usage_where = ["1=1"]
        usage_params = []
        if start:
            usage_where.append("j.start_time >= ?")
            usage_params.append(start)
        if end:
            usage_where.append("j.start_time < ?")
            usage_params.append(end)
        if user:
            usage_where.append("j.user LIKE ?")
            usage_params.append(_like(user))
        if account:
            usage_where.append("j.account LIKE ?")
            usage_params.append(_like(account))
        base_sql = _usage_sql("jobs j", 'COALESCE(NULLIF(TRIM(j."user"), \'\'), \'(Unknown user)\')', usage_where)
        sql = """
            WITH usage AS ({base_sql}), current_users AS (
                SELECT DISTINCT user_name AS user
                FROM associations
                WHERE user_name IS NOT NULL AND TRIM(user_name) <> ''
                  {account_filter}
            )
            SELECT cu.user,
                   COALESCE(u.jobs, 0) AS jobs,
                   COALESCE(u.allocated_nodes, 0.0) AS allocated_nodes,
                   COALESCE(u.cpu_hours, 0.0) AS cpu_hours,
                   COALESCE(u.gpu_hours, 0.0) AS gpu_hours,
                   COALESCE(u.memory_gb_hours, 0.0) AS memory_gb_hours,
                   COALESCE(u.gpu_memory_gb, 0.0) AS gpu_memory_gb,
                   COALESCE(u.elapsed_hours, 0.0) AS elapsed_hours,
                   COALESCE(u.total_tasks, 0.0) AS total_tasks,
                   COALESCE(u.disk_read_gib, 0.0) AS disk_read_gib,
                   COALESCE(u.disk_write_gib, 0.0) AS disk_write_gib
            FROM current_users cu
            LEFT JOIN usage u ON u.name = cu.user
            WHERE {user_filter}
            ORDER BY {order_by}
            LIMIT ?
        """.format(
            base_sql=base_sql,
            account_filter=("AND account_name LIKE ?" if account else ""),
            user_filter=("cu.user LIKE ?" if user else "1=1"),
            order_by=_order_by(sort, direction, "cu.user"),
        )
        final_params = list(usage_params)
        if account:
            # current_users filter
            final_params.append(_like(account))
        if user:
            final_params.append(_like(user))
        final_params.append(limit)
        result = fetch_polars(con, sql, final_params)
        if round_metrics:
            result = result.with_columns([pl.col(c).round(2) for c in ("allocated_nodes", "cpu_hours", "gpu_hours", "memory_gb_hours", "gpu_memory_gb", "elapsed_hours", "total_tasks") if c in result.columns])
            result = result.with_columns([pl.col(c).round(6) for c in ("disk_read_gib", "disk_write_gib") if c in result.columns])
        return result
    finally:
        con.close()

def account_tree(start=None, end=None, sort="name", direction="asc"):
    """Return account usage rolled up from children into every ancestor."""
    con = connect()
    try:
        rows = con.execute("SELECT account_name,parent_name,level,path FROM accounts ORDER BY path").fetchall()
        direct_rows = account_usage(start, end, limit=100000, sort="cpu_hours", direction="desc", round_metrics=False).to_dicts()
        direct = {r["account"]: r for r in direct_rows}
        result = {
            name: {
                "account": name, "parent": parent, "level": level, "path": path,
                "jobs": 0, "allocated_nodes": 0.0, "cpu_hours": 0.0,
                "gpu_hours": 0.0, "memory_gb_hours": 0.0,
                "gpu_memory_gb": 0.0, "elapsed_hours": 0.0,
                "total_tasks": 0.0, "disk_read_gib": 0.0, "disk_write_gib": 0.0,
            }
            for name, parent, level, path in rows
            if name
        }
        for name, values in direct.items():
            if name == "Unknown":
                result[name] = {
                    "account": name, "parent": None, "level": 0, "path": name,
                    "jobs": 0, "allocated_nodes": 0.0, "cpu_hours": 0.0,
                    "gpu_hours": 0.0, "memory_gb_hours": 0.0,
                    "gpu_memory_gb": 0.0, "elapsed_hours": 0.0,
                    "total_tasks": 0.0, "disk_read_gib": 0.0, "disk_write_gib": 0.0,
                }
            elif name not in result:
                result[name] = {
                    "account": name, "parent": None, "level": 0, "path": name,
                    "jobs": 0, "allocated_nodes": 0.0, "cpu_hours": 0.0,
                    "gpu_hours": 0.0, "memory_gb_hours": 0.0,
                    "gpu_memory_gb": 0.0, "elapsed_hours": 0.0,
                    "total_tasks": 0.0, "disk_read_gib": 0.0, "disk_write_gib": 0.0,
                }
            result[name].update(values)

        # Roll each node into its immediate parent, deepest accounts first.
        # Ancestors then propagate their already-combined totals once, avoiding
        # double counting when the hierarchy has three or more levels.
        metrics = (
            "jobs", "allocated_nodes", "cpu_hours", "gpu_hours",
            "memory_gb_hours", "gpu_memory_gb", "elapsed_hours",
            "total_tasks", "disk_read_gib", "disk_write_gib",
        )
        parents = {name: parent for name, parent, _, _ in rows}
        depth_by_name = {name: int(level or 0) for name, parent, level, path in rows}
        ordered_names = sorted(
            [name for name in result if name in parents],
            key=lambda name: depth_by_name.get(name, 0),
            reverse=True,
        )
        for name in ordered_names:
            parent = parents.get(name)
            if not parent or parent not in result:
                continue
            values = result[name]
            for key in metrics:
                # GPU memory is a peak metric, not an additive usage total.
                if key == "gpu_memory_gb":
                    result[parent][key] = max(
                        result[parent].get(key, 0) or 0,
                        values.get(key, 0) or 0,
                    )
                else:
                    result[parent][key] += values.get(key, 0) or 0

        values = list(result.values())
        key_map = {
            "name": lambda r: r.get("path") or r.get("account") or "",
            "account": lambda r: r.get("account") or "",
            "jobs": lambda r: r.get("jobs", 0),
            "nodes": lambda r: r.get("allocated_nodes", 0),
            "allocated_nodes": lambda r: r.get("allocated_nodes", 0),
            "cpu_hours": lambda r: r.get("cpu_hours", 0),
            "gpu_hours": lambda r: r.get("gpu_hours", 0),
            "memory_gb_hours": lambda r: r.get("memory_gb_hours", 0),
            "gpu_memory_gb": lambda r: r.get("gpu_memory_gb", 0),
            "elapsed_hours": lambda r: r.get("elapsed_hours", 0),
        }
        key = key_map.get(sort or "name", key_map["name"])
        values.sort(key=key, reverse=str(direction).lower() != "asc")
        return _round_metrics(values)
    finally:
        con.close()
