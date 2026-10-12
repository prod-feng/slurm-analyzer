#!/usr/bin/env python3
"""Measure dashboard API latency before/after snapshot optimization.

Example:
  python tools/benchmark_dashboard.py --base-url http://127.0.0.1:8666 --runs 5

Use an authenticated URL/proxy if your deployment requires authentication.
This script reports client-observed latency; it does not modify application data.
"""
import argparse
import statistics
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8666")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    endpoints = [
        ("metadata", "/api/metadata/"),
        ("summary", "/api/summary/"),
        ("account usage", "/api/accounts/?limit=100"),
        ("user usage", "/api/users/usage/?limit=100"),
        ("timeseries 30d", "/api/timeseries/?interval=1h"),
        ("users", "/api/users/"),
        ("partitions", "/api/partitions/"),
    ]
    base = args.base_url.rstrip("/")
    for name, path in endpoints:
        samples = []
        statuses = []
        for _ in range(max(1, args.runs)):
            request = urllib.request.Request(base + path, headers={"Accept": "application/json"})
            started = time.perf_counter()
            try:
                with urllib.request.urlopen(request, timeout=args.timeout) as response:
                    response.read()
                    statuses.append(str(response.status))
            except urllib.error.HTTPError as exc:
                exc.read()
                statuses.append(str(exc.code))
            except Exception as exc:
                statuses.append(type(exc).__name__)
            samples.append(time.perf_counter() - started)
        median = statistics.median(samples)
        p95 = sorted(samples)[min(len(samples) - 1, int(0.95 * (len(samples) - 1)))]
        print("{:<18} median={:8.3f}s p95={:8.3f}s statuses={}".format(
            name, median, p95, ",".join(statuses)
        ))


if __name__ == "__main__":
    main()
