"""Command-line interface for standalone Slurm analytics."""
from __future__ import print_function

import argparse

from .reports import build_accounts_report, build_users_report, write_yaml


def main(argv=None):
    parser = argparse.ArgumentParser(prog="slurm-analytics")
    sub = parser.add_subparsers(dest="command", required=True)
    report = sub.add_parser("report")
    report.add_argument("kind", choices=("accounts", "users"))
    report.add_argument("--start")
    report.add_argument("--end")
    report.add_argument("--user")
    report.add_argument("--output", required=True)
    report.add_argument("--limit", type=int, default=100000)
    args = parser.parse_args(argv)
    if args.command == "report":
        if args.kind == "accounts":
            data = build_accounts_report(args.start, args.end, args.limit)
        else:
            data = build_users_report(args.start, args.end, args.user, args.limit)
        write_yaml(data, args.output)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
