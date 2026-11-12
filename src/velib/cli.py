"""Command-line interface.

velib ingest                 fetch the feeds into the landing zone (idempotent)
velib load                   load pending landing files, rebuild gold
velib check                  run data quality checks (exit 1 on any failure)
velib report --out site      build the dashboard
velib run                    ingest + load + check + report: what the schedule runs
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

from . import ingest, load, quality, report


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    p = argparse.ArgumentParser(
        prog="velib", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--lake", default="lake", type=Path)
    p.add_argument("--db", default="lake/warehouse.duckdb")
    sub = p.add_subparsers(dest="command", required=True)
    for name in ["ingest", "load", "check", "run"]:
        sub.add_parser(name)
    sp = sub.add_parser("report")
    sp.add_argument("--out", default="site/index.html")
    args = p.parse_args(argv)

    failed = False
    if args.command in ("ingest", "run"):
        for r in ingest.ingest(args.lake):
            print(f"{r.feed}: published {r.published_at}, {r.stations} stations, "
                  f"{'new' if r.new else 'already landed'}")  # fmt: skip
    if args.command in ("load", "run"):
        print("load:", json.dumps(load.run(args.db, args.lake)))
    if args.command in ("check", "run"):
        with duckdb.connect(args.db, read_only=True) as con:
            for c in quality.run_checks(con):
                print(f"[{c.status:>4}] {c.name}: {c.value}  ({c.rule})")
                failed |= c.status == "fail"
    if args.command == "report" or (args.command == "run" and not failed):
        out = getattr(args, "out", "site/index.html")
        print("report:", report.build(args.db, out))
    if failed:
        sys.exit("data quality checks failed")


if __name__ == "__main__":
    main()
