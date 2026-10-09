#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


SACCT = "/cm/shared/apps/slurm/current/bin/sacct"

FIELDS = [
    "jobid", "jobidraw", "cluster", "partition", "qos",
    "account", "group", "gid", "user", "uid",
    "submit", "eligible", "start", "end", "elapsed",
    "exitcode", "state", "nnodes", "ncpus", "reqcpus",
    "reqmem", "reqtres", "alloctres", "timelimit",
    "nodelist", "jobname", "tresusageintot",
    "tresusageinave", "tresusageinmax", "avecpu",
    "totalcpu", "maxrss", "maxvmsize",
]


def collect(start, end, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        SACCT,
        "--clusters", "all",
        "-a",
        "--parsable2",
        "--noheader",
        "--duplicates",
        "--format=" + ",".join(FIELDS),
        "-S", start,
        "-E", end,
    ]

    # Use a temporary file so an interrupted collection is never
    # mistaken for a complete raw-data file.
    filename = "{}_{}.out".format(
        start.replace(":", "").replace("-", ""),
        end.replace(":", "").replace("-", ""),
    )
    final_path = output_dir / filename
    temp_path = output_dir / (filename + ".tmp")

    print("Collecting {} -> {}".format(start, end), flush=True)
    print("Output: {}".format(final_path), flush=True)

    started = time.monotonic()

    try:
        with open(str(temp_path), "wb") as stdout_file:
            result = subprocess.run(
                command,
                stdout=stdout_file,
                stderr=subprocess.PIPE,
                timeout=3600,
            )

        if result.returncode != 0:
            raise RuntimeError(
                "sacct exited with code {}: {}".format(
                    result.returncode,
                    result.stderr.decode("utf-8", errors="replace"),
                )
            )

        os.replace(str(temp_path), str(final_path))

        metadata = {
            "start": start,
            "end": end,
            "fields": FIELDS,
            "command": command,
            "returncode": result.returncode,
            "bytes": final_path.stat().st_size,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "completed_at": datetime.now().isoformat(),
        }

        metadata_path = Path(str(final_path) + ".json")
        with open(str(metadata_path), "w") as f:
            json.dump(metadata, f, indent=2)

        print(
            "Completed: {} bytes in {:.1f}s".format(
                metadata["bytes"], metadata["elapsed_seconds"]
            ),
            flush=True,
        )

    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--chunk-days", type=int, default=3)
    args = parser.parse_args()

    if args.chunk_days < 1:
        parser.error("--chunk-days must be at least 1")

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d")

    if start >= end:
        parser.error("--start must be earlier than --end")

    current = start

    while current < end:
        chunk_end = min(
            current + timedelta(days=args.chunk_days),
            end,
        )

        collect(
            current.strftime("%Y-%m-%dT%H:%M:%S"),
            chunk_end.strftime("%Y-%m-%dT%H:%M:%S"),
            args.output_dir,
        )

        current = chunk_end


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)

