from __future__ import absolute_import

from django.core.management.base import BaseCommand, CommandError

from slurm_analytics.storage import database_path
from webapp.slurm.services import _build_snapshot_from_db
from webapp.slurm.state import state_for_database


class Command(BaseCommand):
    help = (
        "Build and persist a compact analytics snapshot from the existing DuckDB. "
        "This does not query sacct or modify accounting rows."
    )

    def handle(self, *args, **options):
        snapshot = _build_snapshot_from_db()
        if snapshot is None:
            raise CommandError(
                "No initialized Slurm DuckDB data was found at {}".format(database_path())
            )
        job_count, step_count = snapshot.job_count, snapshot.step_count
        snapshot.compact()
        state_for_database(database_path()).replace(snapshot)
        self.stdout.write(self.style.SUCCESS(
            "Prepared compact dashboard snapshot at {} ({} jobs, {} steps).".format(
                state_for_database(database_path()).path, job_count, step_count,
            )
        ))
