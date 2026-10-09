
from __future__ import annotations

from datetime import datetime, timedelta, timezone

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


def ingest_range(clusters, start, end, timeout, history_file=None):
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
            clusters, start, midpoint, timeout
        )
        second = ingest_range(
            clusters, midpoint, end, timeout
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
            "--history-file",
            type=str,
            default=None,
            help="Read raw sacct output from this file instead of running sacct",
        )

    def handle(self, *args, **options):
        start = parse_dt(options["start"])
        end = parse_dt(options["end"]) or datetime.now(UTC)

        if start >= end:
            raise self.CommandError("--start must be before --end")

        if options["chunk_days"] < 1:
            raise self.CommandError("--chunk-days must be positive")

        history_file = options.get("history_file")

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
            # Import the entire supplied file exactly once.
            # The start/end options do not filter records in the file.
            self.stdout.write(
                "Importing history file: {}".format(history_file)
            )

            jobs, steps = ingest_range(
                options["clusters"],
                start,
                end,
                options["timeout"],
                history_file=history_file,
            )

            total_jobs += jobs
            total_steps += steps

            # Mark progress only after the file has been imported.
            con = connect()
            try:
                set_metadata(con, "initialized_through", end.isoformat())
            finally:
                con.close()

            self.stdout.write(
                self.style.SUCCESS(
                    "  jobs={} steps={}".format(jobs, steps)
                )
            )

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

        # Refresh account hierarchy unless explicitly disabled.
        if not options["no_accounts"]:
            self.stdout.write("Refreshing Slurm account hierarchy...")

            accounts = add_hierarchy(fetch_accounts())
            associations = fetch_associations()
            replace_accounts(accounts, associations)

            self.stdout.write(
                self.style.SUCCESS(
                    "  accounts={} associations={}".format(
                        len(accounts),
                        len(associations),
                    )
                )
            )

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
