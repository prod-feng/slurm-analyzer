"""Per-job resource efficiency summaries for the web dashboard.

Efficiency is calculated per allocation (never by summing job-step rows):
- CPU: TotalCPU / (allocated CPUs * elapsed seconds), as a percentage.
- RAM: peak recorded MaxRSS / allocated memory, as a percentage.
- GPU: recorded gres/gpuutil telemetry, as reported by Slurm.

Missing telemetry remains null and is excluded from averages/medians.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import statistics
import re

from .storage import connect
from .parser import parse_slurm_duration, parse_slurm_memory, parse_gpu_utilization


def _number(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        result = float(value)
        return result if result >= 0 else None
    except (TypeError, ValueError):
        return None


def _stats(jobs, key):
    values = [float(row[key]) for row in jobs if row.get(key) is not None]
    return {
        "average": round(statistics.mean(values), 2) if values else None,
        "median": round(statistics.median(values), 2) if values else None,
        "measured_jobs": len(values),
    }


def _summarize(jobs, name_key, name):
    result = {name_key: name, "jobs": len(jobs)}
    for metric in ("cpu_efficiency_pct", "ram_efficiency_pct", "gpu_utilization_pct"):
        result[metric] = _stats(jobs, metric)
    return result


def _available_columns(con, table):
    try:
        return {str(row[0]).lower() for row in con.execute("DESCRIBE {}".format(table)).fetchall() if row and row[0] is not None}
    except Exception:
        return set()


def resource_efficiency(start=None, end=None, user_filter="", account_filter="", include_jobs=False, limit=10000, waiting_only=False):
    """Return resource efficiency and queue-wait summaries by job/user/account/partition."""
    now = datetime.now(timezone.utc)
    end = end or now.isoformat()
    start = start or (now - timedelta(days=7)).isoformat()
    con = connect()
    try:
        job_cols = _available_columns(con, "jobs")
        step_cols = _available_columns(con, "job_steps")
        account_cols = _available_columns(con, "accounts")
        if not job_cols:
            return {"users": [], "departments": [], "pis": [], "projects": [], "partitions": [], "waiting_summaries": [], "state_summaries": [], "total_jobs_in_range": 0, "jobs": [], "note": "No jobs table is available."}

        def col(name, alias="j"):
            return "{}.{}".format(alias, name) if name in job_cols else "NULL"

        if "canonical_job_id" in job_cols and "jobid" in job_cols:
            id_expr = "COALESCE(NULLIF(CAST(j.canonical_job_id AS VARCHAR), ''), CAST(j.jobid AS VARCHAR))"
        elif "canonical_job_id" in job_cols:
            id_expr = "CAST(j.canonical_job_id AS VARCHAR)"
        elif "jobid" in job_cols:
            id_expr = "CAST(j.jobid AS VARCHAR)"
        else:
            id_expr = "''"
        start_expr = "TRY_CAST(j.start_time AS TIMESTAMPTZ)" if "start_time" in job_cols else "NULL::TIMESTAMPTZ"
        submit_expr = "TRY_CAST(j.submit_time AS TIMESTAMPTZ)" if "submit_time" in job_cols else "NULL::TIMESTAMPTZ"
        end_expr = "TRY_CAST(j.end_time AS TIMESTAMPTZ)" if "end_time" in job_cols else "NULL::TIMESTAMPTZ"
        user_expr = "COALESCE(NULLIF(TRIM(CAST(j.user AS VARCHAR)), ''), '(Unknown user)')" if "user" in job_cols else "'(Unknown user)'"
        account_expr = "COALESCE(NULLIF(TRIM(CAST(j.account AS VARCHAR)), ''), '(Unknown account)')" if "account" in job_cols else "'(Unknown account)'"
        # Keep the original Slurm state for state charts; state_category is used
        # separately to support normalized schemas. Prefer raw state when available.
        state_expr = "COALESCE(NULLIF(TRIM(CAST(j.state AS VARCHAR)), ''), CAST(j.state_category AS VARCHAR), '')" if "state" in job_cols and "state_category" in job_cols else "COALESCE(CAST(j.state AS VARCHAR), '')" if "state" in job_cols else "COALESCE(CAST(j.state_category AS VARCHAR), '')" if "state_category" in job_cols else "''"

        # Job-step telemetry is fetched only after selecting jobs in the requested
        # date range. Scanning the complete job_steps history on every Efficiency
        # page load is very expensive on multi-year databases.
        step_id_col = "canonical_job_id" if "canonical_job_id" in step_cols else "jobid" if "jobid" in step_cols else None
        # Match step rows by their original JobID prefix whenever possible. In some
        # imported databases canonical_job_id on job_steps is blank or normalized
        # differently from jobs.canonical_job_id; using it first can silently lose
        # TRESUsageInAve telemetry for otherwise matching steps.
        step_id_expr = (
            "split_part(CAST(jobid AS VARCHAR), '.', 1)"
            if "jobid" in step_cols
            else "CAST({} AS VARCHAR)".format(step_id_col) if step_id_col else None
        )

        # Avoid relying on optional columns being present in older DuckDB schemas.
        select_fields = [
            "{} AS job_id".format(id_expr), "{} AS user_name".format(user_expr),
            "{} AS account_name".format(account_expr), "CAST(({}) AS VARCHAR) AS start_at".format(start_expr),
            "CAST(({}) AS VARCHAR) AS end_at".format(end_expr), "{} AS state_name".format(state_expr),
            "{} AS ncpus".format(col("ncpus")), "{} AS elapsed_seconds".format(col("elapsed_seconds")),
            "{} AS totalcpu".format(col("totalcpu")), "{} AS allocated_memory_bytes".format(col("allocated_memory_bytes")),
            "{} AS alloctres".format(col("alloctres")), "{} AS maxrss".format(col("maxrss")), "{} AS gpu_count".format(col("gpu_count")),
            "{} AS job_gpu_util".format(col("gpu_util")),
            "{} AS job_gresusageinave".format(col("gresusageinave")),
            "{} AS job_gresusageinmax".format(col("gresusageinmax")),
            "{} AS job_gresusageintot".format(col("gresusageintot")),
            "{} AS job_tresusageinave".format(col("tresusageinave")),
            "{} AS job_tresusageinmax".format(col("tresusageinmax")),
            "{} AS job_tresusageintot".format(col("tresusageintot")),
            "CAST(({}) AS VARCHAR) AS submit_at".format(submit_expr),
            "{} AS partition_name".format(col("partition")),
            "{} AS nodelist".format(next(("j." + c for c in ("nodelist", "node_list", "nodes") if c in job_cols), "NULL")),
            "{} AS state_category".format("lower(COALESCE(CAST(j.state_category AS VARCHAR), ''))" if "state_category" in job_cols else "''"),
            "date_diff('second', {}, {}) AS waiting_seconds".format(
                submit_expr, start_expr
            ),
        ]
        # Include jobs in the selected range regardless of state. Started jobs are
        # selected by start time; jobs without a start time (e.g. pending jobs) are
        # selected by submit time instead. This avoids silently dropping pending jobs
        # because start_time is NULL.
        where = "(({} >= TRY_CAST(? AS TIMESTAMPTZ) AND {} < TRY_CAST(? AS TIMESTAMPTZ)) OR ({} IS NULL AND {} >= TRY_CAST(? AS TIMESTAMPTZ) AND {} < TRY_CAST(? AS TIMESTAMPTZ)))".format(
            start_expr, start_expr, start_expr, submit_expr, submit_expr
        )
        params = [start, end, start, end]
        if user_filter.strip():
            where += " AND lower(CAST({} AS VARCHAR)) LIKE lower(?)".format(user_expr)
            params.append(user_filter.strip().replace("*", "%").replace("?", "_"))
        if account_filter.strip():
            where += " AND lower(CAST({} AS VARCHAR)) LIKE lower(?)".format(account_expr)
            params.append(account_filter.strip().replace("*", "%").replace("?", "_"))
        # State counts are queried separately without the detail/efficiency limit,
        # so the state chart represents every matching stored job in the range.
        state_count_rows = con.execute(
            "SELECT {} AS state_name, COUNT(*) AS jobs FROM jobs j WHERE {} GROUP BY 1".format(state_expr, where),
            params,
        ).fetchall()
        state_counts = defaultdict(int)
        excluded_plot_states = {"PENDING", "RUNNING"}
        for raw_state, count in state_count_rows:
            raw_label = str(raw_state or "Unknown").strip()
            state_key = re.sub(r"\s+BY\s+.*$", "", raw_label.upper()).rstrip("+") or "UNKNOWN"
            if state_key in excluded_plot_states:
                continue
            state_counts[state_key] += int(count or 0)
        # This total is used by the state plot title, so it reflects plotted jobs only.
        total_jobs_in_range = sum(state_counts.values())
        rows = con.execute("SELECT {} FROM jobs j WHERE {} ORDER BY start_at DESC LIMIT ?".format(", ".join(select_fields), where), params + [max(1, min(int(limit), 50000))]).fetchall()
        names = [item[0] for item in con.description]
        raw_jobs = [dict(zip(names, row)) for row in rows]

        # Read only step rows belonging to the selected job IDs. Some older/imported
        # schemas have no normalized gpu_util column, but retain Slurm's original
        # TRESUsageInAve (e.g. "gres/gpuutil=99"). Parse that raw telemetry as a
        # fallback so GPU utilization is not blank on imported clusters.
        step_gpu = defaultdict(list)
        step_rss = defaultdict(list)
        def job_needs_step_metrics(row):
            # Avoid reading step telemetry for CPU-only jobs whose parent row
            # already has MaxRSS. This is a major reduction on large clusters.
            tres = str(row.get("alloctres") or "")
            gpu_count = _number(row.get("gpu_count")) or 0.0
            if gpu_count <= 0:
                typed = re.findall(r"(?:^|,)\s*gres/gpu:[^=,]+=(\d+(?:\.\d+)?)", tres, re.IGNORECASE)
                if typed:
                    gpu_count = sum(float(value) for value in typed)
                else:
                    generic = re.search(r"(?:^|,)\s*gres/gpu=(\d+(?:\.\d+)?)", tres, re.IGNORECASE)
                    gpu_count = float(generic.group(1)) if generic else 0.0
            needs_rss = row.get("maxrss") in (None, "", "0", 0)
            return gpu_count > 0 or needs_rss

        selected_ids = sorted({
            str(row.get("job_id") or "").split(".", 1)[0]
            for row in raw_jobs if row.get("job_id") and job_needs_step_metrics(row)
        })
        if step_id_col and not waiting_only and selected_ids:
            rss_expr = "maxrss" if "maxrss" in step_cols else "NULL"
            gpu_expr = "TRY_CAST(gpu_util AS DOUBLE)" if "gpu_util" in step_cols else "NULL::DOUBLE"
            # GresUsageInAve is the primary source requested for GPU telemetry;
            # retain TRES fields as compatibility fallbacks for existing imports.
            tres_cols = [name for name in (
                "tresusageinave", "gresusageinave",
                "tresusageinmax", "gresusageinmax",
                "tresusageintot", "gresusageintot",
            ) if name in step_cols]
            tres_exprs = [name for name in tres_cols]
            step_name_expr = "CAST(jobid AS VARCHAR)" if "jobid" in step_cols else step_id_expr
            select_step = [step_id_expr, step_name_expr, rss_expr, gpu_expr] + tres_exprs
            # Chunk IN lists to avoid database parameter limits on very large ranges.
            for offset in range(0, len(selected_ids), 500):
                chunk = selected_ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                step_where = "split_part({}, '.', 1) IN ({})".format(step_id_expr, placeholders)
                # External steps commonly contain zero GPU utilization and add
                # substantial row volume. Keep batch/application steps, which
                # carry the useful workload telemetry.
                if "jobid" in step_cols:
                    step_where += " AND lower(CAST(jobid AS VARCHAR)) NOT LIKE '%.extern'"
                step_sql = "SELECT {} FROM job_steps WHERE {}".format(
                    ", ".join(select_step), step_where
                )
                for step_row in con.execute(step_sql, chunk).fetchall():
                    job_id, step_name, raw_rss, raw_gpu = step_row[:4]
                    key = str(job_id or "").split(".", 1)[0]
                    # Prefer the raw GRES/TRES strings to a pre-normalized
                    # gpu_util value: older databases may have normalized the
                    # wrong field to zero while retaining valid GresUsageInAve.
                    gpu = None
                    for raw_tres in step_row[4:]:
                        gpu = parse_gpu_utilization(raw_tres) if raw_tres is not None else None
                        if gpu is not None:
                            break
                    if gpu is None:
                        gpu = _number(raw_gpu)
                    if gpu is not None:
                        step_suffix = str(step_name or "").rsplit(".", 1)[-1].lower() if "." in str(step_name or "") else ""
                        step_gpu[key].append((step_suffix, gpu))
                    rss = parse_slurm_memory(raw_rss) if raw_rss is not None else None
                    if rss is not None:
                        step_rss[key].append(float(rss))

        # Current account tree gives each job account its parent chain. A top-level
        # account is used as the department group; the next child is the PI group.
        parent_map, level_map, path_map = {}, {}, {}
        if {"account_name", "parent_name"}.issubset(account_cols):
            fields = ["account_name", "parent_name"]
            fields += [x for x in ("level", "path") if x in account_cols]
            for row in con.execute("SELECT {} FROM accounts".format(", ".join(fields))).fetchall():
                data = dict(zip(fields, row))
                key = str(data.get("account_name") or "")
                parent_map[key] = str(data.get("parent_name") or "")
                level_map[key] = data.get("level")
                path_map[key] = data.get("path")

        # Project/PN is the terminal account level (leaf accounts). This avoids
        # counting PI/department accounts as projects and plotting the same
        # account once in the Account chart and again in the Project chart.
        child_accounts = {parent for parent in parent_map.values() if parent}
        leaf_accounts = set(parent_map) - child_accounts

        def chain_for(account):
            chain, seen, current = [], set(), account
            while current and current not in seen and current in parent_map:
                seen.add(current); chain.append(current); current = parent_map.get(current, "")
            chain.reverse()
            # Ignore a synthetic Slurm root if it appears in the chain.
            return [str(part) for part in chain if part is not None and str(part).lower() != "root"]

        jobs = []
        waiting_records = []
        # Queue and requeue/intermediate states are excluded from efficiency/wait
        # metrics. Pending/running are also excluded from plots, including state counts.
        excluded_metric_states = {"PENDING", "RUNNING", "PREEMPTED", "REQUEUED", "REVOKED", "SUSPENDED", "RESIZING"}
        for row in raw_jobs:
            job_id = str(row.get("job_id") or "")
            raw_state = str(row.get("state_name") or "Unknown").strip()
            state_key = re.sub(r"\s+BY\s+.*$", "", raw_state.upper()).rstrip("+") or "UNKNOWN"
            state = state_key.lower()
            metric_eligible = state_key not in excluded_metric_states
            account = str(row.get("account_name") or "(Unknown account)")
            chain = chain_for(account)
            department = chain[0] if chain else account
            pi = chain[1] if len(chain) > 1 else (chain[0] if chain else account)
            project = account if (not parent_map or account in leaf_accounts) else None
            waiting_seconds = _number(row.get("waiting_seconds"))
            waiting_hours = waiting_seconds / 3600.0 if waiting_seconds is not None and waiting_seconds >= 0 else None
            if metric_eligible:
                waiting_records.append({
                    "user": str(row.get("user_name") or "(Unknown user)"),
                    "account": account, "department": department,
                    "pi": pi, "project": project,
                    "is_project": project is not None,
                    "partition": str(row.get("partition_name") or "(Unknown partition)"),
                    "waiting_hours": waiting_hours,
                })
            ncpus, elapsed = _number(row.get("ncpus")), _number(row.get("elapsed_seconds"))
            total_cpu = parse_slurm_duration(row.get("totalcpu")) if row.get("totalcpu") is not None else None
            if total_cpu is not None and ncpus and elapsed and ncpus > 0 and elapsed > 0:
                cpu_pct = max(0.0, float(total_cpu) / (ncpus * elapsed) * 100.0)
            else:
                cpu_pct = None
            # AllocTRES mem is the memory actually allocated to the job. ReqMem-derived
            # allocated_memory_bytes can be much smaller when Slurm allocates a whole
            # GPU node (e.g. ReqMem=4016M but AllocTRES mem=184000M), producing false
            # RAM efficiencies far above 100%. Prefer AllocTRES and only fall back when
            # the allocation does not include a usable mem= value.
            alloc_mem = None
            alloctres = str(row.get("alloctres") or "")
            mem_match = re.search(r"(?:^|,)\s*mem=([^,]+)", alloctres, re.IGNORECASE)
            if mem_match:
                alloc_mem = parse_slurm_memory(mem_match.group(1).strip())
            if alloc_mem is None or alloc_mem <= 0:
                alloc_mem = _number(row.get("allocated_memory_bytes"))
            peak_rss = parse_slurm_memory(row.get("maxrss")) if row.get("maxrss") is not None else None
            if step_rss.get(job_id):
                peak_rss = max(([float(peak_rss)] if peak_rss is not None else []) + step_rss[job_id])
            if alloc_mem and peak_rss is not None and alloc_mem > 0:
                ram_pct = max(0.0, float(peak_rss) / alloc_mem * 100.0)
            else:
                ram_pct = None
            # Older imports / some clusters may not populate the normalized
            # gpu_count column even though AllocTRES and step gpu-util telemetry
            # are present. Recover the allocation count from AllocTRES before
            # deciding whether the telemetry belongs to a GPU job.
            gpu_count = _number(row.get("gpu_count")) or 0.0
            if gpu_count <= 0 and alloctres:
                typed_counts = re.findall(
                    r"(?:^|,)\s*gres/gpu:[^=,]+=(\d+(?:\.\d+)?)",
                    alloctres, re.IGNORECASE,
                )
                if typed_counts:
                    gpu_count = sum(float(value) for value in typed_counts)
                else:
                    generic_match = re.search(
                        r"(?:^|,)\s*gres/gpu=(\d+(?:\.\d+)?)",
                        alloctres, re.IGNORECASE,
                    )
                    if generic_match:
                        gpu_count = float(generic_match.group(1))
            gpu_values = step_gpu.get(job_id, [])
            # Prefer batch/application step telemetry over .extern records, which
            # commonly report zero and would dilute the useful utilization value.
            batch_values = [value for suffix, value in gpu_values if suffix == "batch"]
            non_extern_values = [value for suffix, value in gpu_values if suffix != "extern"]
            selected_gpu_values = batch_values or non_extern_values or [value for _, value in gpu_values]
            # Some Slurm configurations store GresUsageInAve on the allocation
            # row rather than (or in addition to) the .batch step. Use it as a
            # fallback after preferring useful step telemetry.
            if not selected_gpu_values:
                for raw_value in (
                    row.get("job_tresusageinave"), row.get("job_gresusageinave"),
                    row.get("job_tresusageinmax"), row.get("job_gresusageinmax"),
                    row.get("job_tresusageintot"), row.get("job_gresusageintot"),
                ):
                    value = parse_gpu_utilization(raw_value) if raw_value is not None else None
                    if value is not None:
                        selected_gpu_values = [value]
                        break
                if not selected_gpu_values:
                    direct_value = _number(row.get("job_gpu_util"))
                    if direct_value is not None:
                        selected_gpu_values = [direct_value]
            gpu_pct = statistics.mean(selected_gpu_values) if selected_gpu_values and gpu_count > 0 else None
            # Root/top-level account represents the department; next child is PI.
            if not metric_eligible:
                cpu_pct = ram_pct = gpu_pct = None
            jobs.append({
                "job_id": job_id, "user": str(row.get("user_name") or "(Unknown user)"),
                "account": account, "department": department, "pi": pi, "project": project,
                "partition": str(row.get("partition_name") or "(Unknown partition)"),
                "nodelist": str(row.get("nodelist") or "") or None,
                "state": row.get("state_name"), "submit_time": str(row.get("submit_at")) if row.get("submit_at") else None,
                "start_time": str(row.get("start_at")) if row.get("start_at") else None,
                "waiting_hours": round(waiting_hours, 3) if waiting_hours is not None else None,
                "cpu_efficiency_pct": round(cpu_pct, 2) if cpu_pct is not None else None,
                "ram_efficiency_pct": round(ram_pct, 2) if ram_pct is not None else None,
                "gpu_utilization_pct": round(gpu_pct, 2) if gpu_pct is not None else None,
            })

        # Efficiency rollups contain only eligible final/non-queue jobs. The detailed
        # jobs response may still include queue states for inspection, but plots omit them.
        metric_jobs = [job for job in jobs if str(job.get("state") or "").strip().upper().split()[0].rstrip("+") not in excluded_metric_states]
        by_user, by_dept, by_pi = defaultdict(list), defaultdict(list), defaultdict(list)
        by_project, by_partition = defaultdict(list), defaultdict(list)
        for job in metric_jobs:
            by_user[job["user"]].append(job)
            by_dept[job["department"]].append(job)
            by_pi[(job["department"], job["pi"])].append(job)
            # Only leaf-account jobs have a project/PN. Do not create a None
            # project bucket for department/PI accounts; sorting it calls .lower()
            # on None and fails the entire endpoint.
            if job.get("project") is not None:
                by_project[job["project"]].append(job)
            by_partition[job["partition"]].append(job)
        users = [_summarize(items, "user", name) for name, items in by_user.items()]
        departments = [_summarize(items, "department", name) for name, items in by_dept.items()]
        pis = [_summarize(items, "pi", name) | {"department": dept} for (dept, name), items in by_pi.items()]
        projects = [_summarize(items, "project", name) for name, items in by_project.items()]
        partitions = [_summarize(items, "partition", name) for name, items in by_partition.items()]

        def waiting_summary(group_type, name, items):
            stats = _stats(items, "waiting_hours")
            return {"group_type": group_type, "group": name, "jobs": len(items),
                    "measured_jobs": stats["measured_jobs"],
                    "average_hours": stats["average"], "median_hours": stats["median"]}

        wait_by_user, wait_by_account, wait_by_dept, wait_by_pi = defaultdict(list), defaultdict(list), defaultdict(list), defaultdict(list)
        wait_by_project, wait_by_partition = defaultdict(list), defaultdict(list)
        for job in waiting_records:
            wait_by_user[job["user"]].append(job)
            wait_by_account[job["account"]].append(job)
            wait_by_dept[job["department"]].append(job)
            # Keep labels compact: show the PI account name only, not
            # "department / PI". Same-named PI accounts are combined for this
            # chart rather than displaying long parent-qualified labels.
            wait_by_pi[job["pi"]].append(job)
            if job.get("is_project"):
                wait_by_project[job["project"]].append(job)
            wait_by_partition[job["partition"]].append(job)
        waiting_summaries = [waiting_summary("All jobs", "All jobs", waiting_records)]
        waiting_summaries += [waiting_summary("Account", name, items) for name, items in wait_by_account.items()]
        waiting_summaries += [waiting_summary("Department", name, items) for name, items in wait_by_dept.items()]
        waiting_summaries += [waiting_summary("PI", name, items) for name, items in wait_by_pi.items()]
        waiting_summaries += [waiting_summary("Project/PN", name, items) for name, items in wait_by_project.items()]
        waiting_summaries += [waiting_summary("Partition", name, items) for name, items in wait_by_partition.items()]
        waiting_summaries += [waiting_summary("User", name, items) for name, items in wait_by_user.items()]
        users.sort(key=lambda x: x["jobs"], reverse=True)
        departments.sort(key=lambda x: str(x.get("department") or "").lower())
        pis.sort(key=lambda x: (str(x.get("department") or "").lower(), str(x.get("pi") or "").lower()))
        projects.sort(key=lambda x: str(x.get("project") or "").lower())
        partitions.sort(key=lambda x: str(x.get("partition") or "").lower())
        waiting_summaries.sort(key=lambda x: (x.get("group_type") != "All jobs", str(x.get("group_type") or "").lower(), str(x.get("group") or "").lower()))
        state_summaries = [{"state": state, "jobs": count} for state, count in sorted(state_counts.items(), key=lambda item: (-item[1], item[0]))]
        return {"users": users, "departments": departments, "pis": pis, "projects": projects,
                "partitions": partitions, "waiting_summaries": waiting_summaries,
                "state_summaries": state_summaries, "total_jobs_in_range": total_jobs_in_range,
                "jobs": jobs if include_jobs else [], "range": {"start": start, "end": end}}
    finally:
        con.close()
