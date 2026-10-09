from __future__ import annotations

from django.core.management.base import BaseCommand

from slurm_analytics.reports import build_accounts_report, build_users_report, write_yaml


class Command(BaseCommand):
    help = "Generate standalone Slurm account/user YAML reports from DuckDB."

    def add_arguments(self, parser):
        parser.add_argument("report", choices=("accounts", "users"))
        parser.add_argument("--start")
        parser.add_argument("--end")
        parser.add_argument("--user")
        parser.add_argument("--output", required=True)
        parser.add_argument("--limit", type=int, default=100000)

    def handle(self, *args, **options):
        if options["report"] == "accounts":
            data = build_accounts_report(options["start"], options["end"], options["limit"])
        else:
            data = build_users_report(options["start"], options["end"], options["user"], options["limit"])
        write_yaml(data, options["output"])
        self.stdout.write(self.style.SUCCESS("Wrote {}".format(options["output"])))
