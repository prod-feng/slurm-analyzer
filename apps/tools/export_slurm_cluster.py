#!/usr/bin/env python3
"""Export Slurm history and cluster metadata without installing the dashboard.

Compatible with Python 3.6. Requires only access to sacct, sacctmgr, and scontrol.
Outputs sacct .out files plus .out.json sidecars, accounts.json, nodes.json, and partitions.json.
"""
from __future__ import print_function
import argparse
import datetime
import json
import os
import subprocess
import sys
import time

FIELDS = [
    "jobid",
    "jobidraw",
    "cluster",
    "partition",
    "qos",
    "account",
    "user",
    "submit",
    "eligible",
    "start",
    "end",
    "elapsed",
    "exitcode",
    "state",
    "nnodes",
    "ntasks",
    "ncpus",
    "reqcpus",
    "reqmem",
    "reqtres",
    "alloctres",
    "timelimit",
    "nodelist",
    "gresusageinave",
    "gresusageinmax",
    "gresusageintot",
    "tresusageinave",
    "tresusageinmax",
    "avecpu",
    "totalcpu",
    "maxrss",
    "maxdiskread",
    "maxdiskwrite",
    "jobname",
]


def run(args, timeout=1800, stdout=None):
    result = subprocess.run(args, stdout=stdout if stdout is not None else subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=(stdout is None), timeout=timeout)
    if result.returncode:
        err = result.stderr if isinstance(result.stderr, str) else result.stderr.decode("utf-8", "replace")
        raise RuntimeError("Command failed ({}): {}\n{}".format(result.returncode, " ".join(args), err))
    return result.stdout if stdout is None else ""


def parse_pipe_rows(output, minimum=2):
    rows = []
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = [item.strip() for item in line.split("|")]
        if len(parts) >= minimum:
            rows.append(parts)
    return rows


def export_accounts(outdir):
    tree = run(["sacctmgr", "-p", "list", "associations", "tree", "format=Account,user,ParentName"])
    desc_out = run(["sacctmgr", "-n", "-P", "show", "account", "format=Account,Description"])
    assoc_out = run(["sacctmgr", "-n", "-P", "show", "assoc", "format=Cluster,Account,User"])
    descriptions = {}
    for row in parse_pipe_rows(desc_out):
        if row[0] and row[0].lower() != "account":
            descriptions[row[0]] = row[1] if len(row) > 1 and row[1] else None
    parents = {}
    for row in parse_pipe_rows(tree, 3):
        name, user, parent = row[0], row[1], row[2]
        if not name or name.lower() == "account":
            continue
        if parent.lower() in ("(null)", "none", "null", "-"):
            parent = ""
        if name not in parents or (not parents[name] and parent):
            parents[name] = parent or None
    for name in descriptions:
        parents.setdefault(name, None)
    accounts = []
    for name, parent in parents.items():
        chain, current, seen = [], name, set()
        while current and current not in seen:
            seen.add(current)
            chain.append(current)
            current = parents.get(current)
        chain.reverse()
        accounts.append([name, parent, descriptions.get(name), max(0, len(chain)-1), "/".join(chain)])
    associations = []
    for row in parse_pipe_rows(assoc_out, 2):
        if row[0].lower() == "cluster" or not row[1]:
            continue
        user = row[2] if len(row) > 2 else ""
        if user.lower() in ("(null)", "none", "null", "-"):
            user = ""
        associations.append([row[0] or None, row[1], user or None])
    payload = {"format_version": 1, "accounts": accounts, "associations": associations}
    path = os.path.join(outdir, "accounts.json")
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    print("Exported {} accounts and {} associations: {}".format(len(accounts), len(associations), path))


def export_nodes(outdir):
    output = run(["scontrol", "show", "node", "-a"], timeout=300)
    if not output.strip():
        raise RuntimeError("scontrol returned empty node inventory")
    path = os.path.join(outdir, "nodes.json")
    with open(path, "w") as handle:
        json.dump({"format_version": 1, "scontrol_output": output}, handle)
        handle.write("\n")
    count = sum(1 for line in output.splitlines() if "NodeName=" in line)
    print("Exported node inventory (approximately {} nodes): {}".format(count, path))



def export_partitions(outdir):
    output = run(["scontrol", "show", "partition", "-o"], timeout=300)
    if not output.strip():
        raise RuntimeError("scontrol returned empty partition inventory")
    path = os.path.join(outdir, "partitions.json")
    with open(path, "w") as handle:
        json.dump({"format_version": 1, "scontrol_output": output}, handle)
        handle.write("\n")
    count = sum(1 for line in output.splitlines() if "PartitionName=" in line)
    print("Exported partition inventory ({} partitions): {}".format(count, path))

def collect_chunk(start, end, outdir):
    stamp = lambda value: value.strftime("%Y%m%dT%H%M%S")
    filename = "sacct_{}_{}.out".format(stamp(start), stamp(end))
    final_path = os.path.join(outdir, filename)
    temp_path = final_path + ".tmp"
    command = ["sacct", "--clusters", "all", "-a", "--parsable2", "--noheader", "--duplicates",
               "--format=" + ",".join(FIELDS), "-S", start.strftime("%Y-%m-%dT%H:%M:%S"),
               "-E", end.strftime("%Y-%m-%dT%H:%M:%S")]
    print("Collecting {} -> {}".format(command[-2], command[-1]), flush=True)
    try:
        with open(temp_path, "wb") as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.PIPE, timeout=3600)
        if result.returncode:
            raise RuntimeError("sacct failed: " + result.stderr.decode("utf-8", "replace"))
        os.rename(temp_path, final_path)
        metadata = {"start": command[-2], "end": command[-1], "fields": FIELDS,
                    "command": command, "returncode": result.returncode, "bytes": os.path.getsize(final_path),
                    "completed_at": datetime.datetime.now().isoformat()}
        with open(final_path + ".json", "w") as handle:
            json.dump(metadata, handle, indent=2)
        print("Wrote {} ({} bytes)".format(final_path, metadata["bytes"]), flush=True)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date (exclusive), YYYY-MM-DD")
    parser.add_argument("--output-dir", required=True, help="Directory to write export files")
    parser.add_argument("--chunk-days", type=int, default=3)
    parser.add_argument("--skip-jobs", action="store_true")
    parser.add_argument("--skip-accounts", action="store_true")
    parser.add_argument("--skip-nodes", action="store_true")
    parser.add_argument("--skip-partitions", action="store_true")
    args = parser.parse_args()
    if args.chunk_days < 1:
        parser.error("--chunk-days must be at least 1")
    start = datetime.datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.datetime.strptime(args.end, "%Y-%m-%d")
    if start >= end:
        parser.error("--start must be earlier than --end")
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    if not args.skip_jobs:
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + datetime.timedelta(days=args.chunk_days), end)
            collect_chunk(cursor, chunk_end, args.output_dir)
            cursor = chunk_end
    if not args.skip_accounts:
        export_accounts(args.output_dir)
    if not args.skip_nodes:
        export_nodes(args.output_dir)
    if not args.skip_partitions:
        export_partitions(args.output_dir)
    print("Export complete: {}".format(os.path.abspath(args.output_dir)))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)
