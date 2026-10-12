
from __future__ import annotations

from pathlib import Path

from datetime import datetime, timedelta, timezone

import glob
import json
import os

from django.core.management.base import BaseCommand

from slurm_analytics.pipeline import ingest_sacct
from slurm_analytics.sacct import SacctConfig
from slurm_analytics.sacctmgr import (
    add_hierarchy,
    fetch_accounts,
    fetch_associations,
)
from slurm_analytics.storage import (
    connect,
    ensure_metadata,
    set_metadata,
    store_ingestion,
    replace_accounts,
)

UTC = timezone.utc
MIN_CHUNK = timedelta(days=1)


def parse_dt(value):
    if not value:
        return None

    value = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    return dt.astimezone(UTC)


def ingest_range(clusters, start, end, timeout, history_file=None, verbose=False):
    """Ingest a range, splitting it recursively if sacct times out."""
    try:
        ingestion = ingest_sacct(
            SacctConfig(
                clusters=clusters,
                start=start.isoformat(),
                end=end.isoformat(),
                timeout=timeout,
            ),
            input_file=history_file,
            verbose=verbose,
        )

    except RuntimeError as exc:
        # A history file is not a live sacct query.
        # Do not retry it by splitting the date range.
        if history_file:
            raise

        duration = end - start

        if "timed out" not in str(exc).lower() or duration <= MIN_CHUNK:
            raise

        midpoint = start + duration / 2

        print(
            "sacct timed out for {} -> {}; splitting into smaller ranges.".format(
                start.isoformat(), end.isoformat()
            )
        )

        first = ingest_range(
            clusters, start, midpoint, timeout, verbose=verbose
        )
        second = ingest_range(
            clusters, midpoint, end, timeout, verbose=verbose
        )

        return first[0] + second[0], first[1] + second[1]

    return store_ingestion(ingestion)


class Command(BaseCommand):
    help = (
        "Initialize the permanent DuckDB Slurm accounting store "
        "from historical sacct data."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--start",
            default="1970-01-01",
            help="Earliest accounting timestamp (default: 1970-01-01)",
        )
        parser.add_argument(
            "--end",
            default=None,
            help="Latest accounting timestamp (default: current UTC time)",
        )
        parser.add_argument(
            "--clusters",
            default="all",
            help="Slurm clusters to query (default: all)",
        )
        parser.add_argument(
            "--chunk-days",
            type=int,
            default=31,
            help="Number of days per live sacct query (default: 31)",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=1800,
            help="sacct query timeout in seconds (default: 1800)",
        )
        parser.add_argument(
            "--no-accounts",
            action="store_true",
            help="Skip refreshing the Slurm account hierarchy",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Print raw suspicious sacct rows during import (normally only a compact warning is shown)",
        )
        parser.add_argument(
            "--history-file",
            nargs="+",
            default=None,
            metavar="FILE",
            help="Read one or more raw sacct output files instead of running sacct (for example: --history-file history1.out history2.out)",
        )
        parser.add_argument(
            "--accounts-file",
            default=None,
            metavar="JSON",
            help="Import account hierarchy and associations from an exported JSON file",
        )
        parser.add_argument(
            "--nodes-file",
            default=None,
            metavar="JSON",
            help="Import node inventory from JSON produced by the standalone Slurm exporter",
        )

    def handle(self, *args, **options):
        start = parse_dt(options["start"])
        end = parse_dt(options["end"]) or datetime.now(UTC)

        if start >= end:
            raise self.CommandError("--start must be before --end")

        if options["chunk_days"] < 1:
            raise self.CommandError("--chunk-days must be positive")

        history_file = options.get("history_file")

        # Expand wildcard patterns here because Python open() does not expand
        # shell globs. This supports quoted patterns and invocations where
        # the shell does not perform wildcard expansion.
        if history_file:
            expanded_history_files = []
            for item in history_file:
                matches = sorted(glob.glob(item))
                if matches:
                    expanded_history_files.extend(
                        match for match in matches if os.path.isfile(match)
                    )
                elif os.path.isfile(item):
                    expanded_history_files.append(item)
                else:
                    raise self.CommandError(
                        "History file or pattern did not match any files: {}".format(item)
                    )

            # Deduplicate paths while preserving their first-seen order.
            history_file = list(dict.fromkeys(expanded_history_files))
            if not history_file:
                raise self.CommandError(
                    "No history files found. Check --history-file paths and patterns."
                )

        # Initialize metadata.
        con = connect()
        try:
            ensure_metadata(con)
            set_metadata(con, "initialization_start", start.isoformat())
            set_metadata(con, "initialization_end", end.isoformat())
        finally:
            con.close()

        total_jobs = 0
        total_steps = 0
        chunk_size = timedelta(days=options["chunk_days"])

        if history_file:
            # Import each supplied file once. Upserts make overlapping files
            # safe, which is common when collections use adjacent/overlapping
            # sacct date windows. .out.json sidecars preserve each file's field
            # list so older collection formats are mapped correctly.
            for filename in history_file:
                self.stdout.write("Importing history file: {}".format(filename))
                jobs, steps = ingest_range(
                    options["clusters"],
                    start,
                    end,
                    options["timeout"],
                    history_file=filename,
                    verbose=options.get("verbose", False),
                )
                total_jobs += jobs
                total_steps += steps
                self.stdout.write(self.style.SUCCESS("  jobs={} steps={}".format(jobs, steps)))

            con = connect()
            try:
                set_metadata(con, "initialized_through", end.isoformat())
            finally:
                con.close()

        else:
            # Normal mode: retain chunked live sacct ingestion.
            cursor = start

            while cursor < end:
                chunk_end = min(cursor + chunk_size, end)

                self.stdout.write(
                    "Importing {} -> {}".format(
                        cursor.isoformat(),
                        chunk_end.isoformat(),
                    )
                )

                jobs, steps = ingest_range(
                    options["clusters"],
                    cursor,
                    chunk_end,
                    options["timeout"],
                    verbose=options.get("verbose", False),
                )

                total_jobs += jobs
                total_steps += steps

                # Record progress only after this range has been ingested.
                con = connect()
                try:
                    set_metadata(
                        con,
                        "initialized_through",
                        chunk_end.isoformat(),
                    )
                finally:
                    con.close()

                self.stdout.write(
                    self.style.SUCCESS(
                        "  jobs={} steps={}".format(jobs, steps)
                    )
                )

                cursor = chunk_end

        # Persist each cluster's account hierarchy in its own DuckDB. For a
        # remote cluster, import an export collected on a host with its own
        # sacctmgr access instead of querying the local cluster by accident.
        accounts_file = options.get("accounts_file")
        if accounts_file:
            try:
                with open(accounts_file, "r", encoding="utf-8") as handle:
                    account_data = json.load(handle)
                accounts = [tuple(row) for row in account_data.get("accounts", [])]
                associations = [tuple(row) for row in account_data.get("associations", [])]
                if not accounts and not associations:
                    raise ValueError("JSON contains no accounts or associations")
                replace_accounts(accounts, associations)
            except (OSError, ValueError, TypeError, KeyError) as exc:
                raise self.CommandError("Unable to import accounts file {}: {}".format(accounts_file, exc))
            self.stdout.write(self.style.SUCCESS(
                "Imported accounts={} associations={} from {}".format(
                    len(accounts), len(associations), accounts_file
                )
            ))
        elif not options["no_accounts"]:
            self.stdout.write("Refreshing Slurm account hierarchy...")
            accounts = add_hierarchy(fetch_accounts())
            associations = fetch_associations()
            replace_accounts(accounts, associations)
            self.stdout.write(self.style.SUCCESS(
                "  accounts={} associations={}".format(len(accounts), len(associations))
            ))

        nodes_file = options.get("nodes_file")
        if nodes_file:
            try:
                with open(nodes_file, "r", encoding="utf-8") as handle:
                    node_data = json.load(handle)
                raw_nodes = node_data.get("scontrol_output", "") if isinstance(node_data, dict) else ""
                if not raw_nodes.strip():
                    raise ValueError("JSON contains no scontrol_output")
                from slurm_analytics.node_inventory import parse_scontrol_nodes
                from slurm_analytics.storage import replace_node_inventory
                node_frame = parse_scontrol_nodes(raw_nodes)
                if node_frame.is_empty():
                    raise ValueError("No nodes could be parsed from scontrol output")
                replace_node_inventory(node_frame)
                con = connect()
                try:
                    set_metadata(con, "cluster_mode", "import")
                finally:
                    con.close()
            except (OSError, ValueError, TypeError, KeyError) as exc:
                raise self.CommandError("Unable to import node inventory {}: {}".format(nodes_file, exc))
            self.stdout.write(self.style.SUCCESS(
                "Imported node inventory: {} nodes from {}".format(node_frame.height, nodes_file)
            ))

        # Mark initialization complete.
        con = connect()
        try:
            set_metadata(con, "initialized", "true")
        finally:
            con.close()

        self.stdout.write(
            self.style.SUCCESS(
                "Initialization complete: {} job records, "
                "{} job-step records.".format(
                    total_jobs,
                    total_steps,
                )
            )
        )
