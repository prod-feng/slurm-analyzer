"""Export this Slurm cluster's account hierarchy for remote-cluster import."""
import json
from django.core.management.base import BaseCommand, CommandError
from slurm_analytics.sacctmgr import add_hierarchy, fetch_accounts, fetch_associations


class Command(BaseCommand):
    help = "Export the current Slurm account hierarchy and associations to JSON."

    def add_arguments(self, parser):
        parser.add_argument("--output", required=True, help="Destination JSON file")

    def handle(self, *args, **options):
        try:
            accounts = add_hierarchy(fetch_accounts())
            associations = fetch_associations()
            payload = {
                "format_version": 1,
                "accounts": accounts,
                "associations": associations,
            }
            with open(options["output"], "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False)
                handle.write("\n")
        except Exception as exc:
            raise CommandError("Unable to export Slurm account hierarchy: {}".format(exc))
        self.stdout.write(self.style.SUCCESS(
            "Exported {} accounts and {} associations to {}".format(
                len(accounts), len(associations), options["output"]
            )
        ))
