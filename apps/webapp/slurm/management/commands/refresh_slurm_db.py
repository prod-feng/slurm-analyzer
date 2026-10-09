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
    get_metadata,
    set_metadata,
    store_ingestion,
    replace_accounts,
)

UTC = timezone.utc
MIN_CHUNK = timedelta(days=1)


def parse_dt(value):
    value = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def ingest_range(clusters, start, end, timeout):
    """Ingest a range, splitting it recursively if sacct times out."""
    try:
        ingestion = ingest_sacct(
            SacctConfig(
                clusters=clusters,
                start=start.isoformat(),
                end=end.isoformat(),
                timeout=timeout,
            )
        )
    except RuntimeError as exc:
        duration = end - start
        if "timed out" not in str(exc).lower() or duration <= MIN_CHUNK:
            raise

        midpoint = start + duration / 2
        print(
            "sacct timed out for {} -> {}; splitting into smaller ranges.".format(
                start.isoformat(), end.isoformat()
            )
        )

        first = ingest_range(clusters, start, midpoint, timeout)
        second = ingest_range(clusters, midpoint, end, timeout)

        return first[0] + second[0], first[1] + second[1]

    return store_ingestion(ingestion)


class Command(BaseCommand):
    help = "Incrementally refresh the permanent DuckDB Slurm accounting store."

    def add_arguments(self, parser):
        parser.add_argument("--clusters", default="all")
        parser.add_argument("--overlap-minutes", type=int, default=60)
        parser.add_argument("--chunk-days", type=int, default=7)
        parser.add_argument("--start", default=None)
        parser.add_argument("--end", default=None)
        parser.add_argument("--timeout", type=int, default=1800)
        parser.add_argument("--no-accounts", action="store_true")

    def handle(self, *args, **options):
        if options["chunk_days"] < 1:
            raise self.CommandError("--chunk-days must be positive")

        if options["overlap_minutes"] < 0:
            raise self.CommandError("--overlap-minutes cannot be negative")

        con = connect()
        try:
            through = get_metadata(con, "initialized_through")
        finally:
            con.close()

        start = parse_dt(options["start"]) if options["start"] else None

        if start is None:
            if not through:
                raise self.CommandError(
                    "Database is not initialized; run initialize_slurm first."
                )
            start = parse_dt(through) - timedelta(
                minutes=options["overlap_minutes"]
            )

        end = parse_dt(options["end"]) if options["end"] else datetime.now(UTC)

        if start >= end:
            raise self.CommandError("--start must be before --end")

        total_jobs = 0
        total_steps = 0
        cursor = start
        chunk_size = timedelta(days=options["chunk_days"])

        while cursor < end:
            chunk_end = min(cursor + chunk_size, end)

            self.stdout.write(
                "Refreshing {} -> {}".format(
                    cursor.isoformat(), chunk_end.isoformat()
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

            self.stdout.write(
                self.style.SUCCESS(
                    "  jobs={} steps={}".format(jobs, steps)
                )
            )

            cursor = chunk_end

        # Update accounts before advancing the refresh cursor. If account
        # refresh fails, the next run will retry the accounting interval.
        if not options["no_accounts"]:
            self.stdout.write("Refreshing Slurm account hierarchy...")
            accounts = add_hierarchy(fetch_accounts())
            associations = fetch_associations()
            replace_accounts(accounts, associations)

        # Advance the cursor only after all requested ingestion chunks succeed.
        con = connect()
        try:
            set_metadata(con, "initialized_through", end.isoformat())
            set_metadata(con, "last_refresh_start", start.isoformat())
            set_metadata(con, "last_refresh_end", end.isoformat())
        finally:
            con.close()

        self.stdout.write(
            self.style.SUCCESS(
                "Refresh complete: {} job records, {} job-step records; "
                "through {}".format(total_jobs, total_steps, end.isoformat())
            )
        )
