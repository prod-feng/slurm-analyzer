from __future__ import absolute_import

from django.core.management.base import BaseCommand

from slurm_analytics.sacct import SacctConfig
from webapp.slurm.services import refresh_slurm


class Command(BaseCommand):
    help = "Incrementally refresh Slurm accounting and publish a snapshot containing all stored job history."

    def add_arguments(self, parser):
        parser.add_argument("--start", default=None, help="Optional explicit query start; otherwise refresh incrementally from the saved cursor")
        parser.add_argument("--end", default=None, help="Optional explicit query end; otherwise use the current time")
        parser.add_argument("--clusters", default="all")
        parser.add_argument("--no-nodes", action="store_true")

    def handle(self, *args, **options):
        config = SacctConfig(
            clusters=options["clusters"],
            start=options["start"],
            end=options["end"],
        )

        snapshot = refresh_slurm(
            sacct_config=config,
            refresh_nodes=not options["no_nodes"],
        )
        jobs = snapshot.ingestion.consolidated_jobs

        self.stdout.write(
            self.style.SUCCESS(
                "Slurm refresh completed: {} jobs, refreshed {}".format(
                    jobs.height,
                    snapshot.refreshed_at,
                )
            )
        )
