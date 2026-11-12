"""Data quality checks, run after every load.

Each check returns ok / warn / fail with the measured value, and the CLI exits
non-zero on any `fail`, so a scheduled run that ingests broken or stale data
goes red instead of publishing a dashboard built on it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import duckdb


@dataclass
class Check:
    name: str
    status: str  # ok | warn | fail
    value: float | int | str
    rule: str


def _status(ok: bool, warn_only: bool = False) -> str:
    return "ok" if ok else ("warn" if warn_only else "fail")


def run_checks(con: duckdb.DuckDBPyConnection, now: datetime | None = None) -> list[Check]:
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    checks: list[Check] = []

    latest = con.execute("SELECT max(published_at) FROM meta.ingestion_log").fetchone()[0]
    age_min = (now - latest).total_seconds() / 60 if latest else float("inf")
    checks.append(Check("freshness", _status(age_min <= 120), round(age_min, 1),
                        "latest publication at most 120 minutes old"))  # fmt: skip

    stations = con.execute(
        "SELECT stations FROM gold.network_snapshot ORDER BY published_at DESC LIMIT 1"
    ).fetchone()
    n = stations[0] if stations else 0
    checks.append(Check("volume", _status(n >= 1400), n,
                        "latest publication covers at least 1,400 stations"))  # fmt: skip

    # Judged on the latest status publication: what matters is whether *this* load can
    # be trusted, not whether some past publication once went wrong.
    rec, rej = con.execute(
        """SELECT coalesce(records, 0), coalesce(rejected, 0) FROM meta.ingestion_log
           WHERE feed = 'station_status' ORDER BY published_at DESC LIMIT 1"""
    ).fetchone() or (0, 0)
    rate = rej / (rec + rej) if rec + rej else 0.0
    checks.append(Check("quarantine_rate", _status(rate < 0.01), round(rate, 5),
                        "under 1% of the latest publication's records break the data contract"))  # fmt: skip

    untyped = con.execute(
        """SELECT coalesce(avg((ebike IS NULL)::INT), 0) FROM bronze.station_status
           WHERE _published_at = (SELECT max(_published_at) FROM bronze.station_status)"""
    ).fetchone()[0]
    checks.append(Check("bike_types_missing", _status(untyped < 0.01, warn_only=True),
                        round(untyped, 4),
                        "the latest publication breaks bikes down by type for every station"))  # fmt: skip

    orphans = con.execute(
        """SELECT count(DISTINCT b.station_id) FROM bronze.station_status b
           LEFT JOIN silver.stations s ON s.station_id = b.station_id
           WHERE s.station_id IS NULL"""
    ).fetchone()[0]
    checks.append(Check("referential_integrity", _status(orphans == 0, warn_only=True), orphans,
                        "every station in the status feed exists in station_information"))  # fmt: skip

    over = con.execute(
        """SELECT round(avg((b.num_bikes_available + b.num_docks_available > s.capacity)::INT), 4)
           FROM silver.station_status b
           JOIN silver.stations s ON s.station_id = b.station_id AND s.is_current
           WHERE s.capacity > 0"""
    ).fetchone()[0]
    checks.append(Check("capacity_consistency", _status((over or 0) < 0.05, warn_only=True),
                        over or 0.0,
                        "bikes + docks exceed capacity in under 5% of observations"))  # fmt: skip

    stale = con.execute("SELECT count(*) FROM gold.stale_stations").fetchone()[0]
    checks.append(Check("stale_stations", _status(stale <= 50, warn_only=True), stale,
                        "at most 50 stations silent for more than a day"))  # fmt: skip

    missing_code = con.execute(
        """SELECT coalesce(avg((station_code IS NULL)::INT), 0) FROM bronze.station_status
           WHERE _published_at = (SELECT max(_published_at) FROM bronze.station_status)"""
    ).fetchone()[0]
    checks.append(Check("station_code_missing", _status(missing_code < 0.01, warn_only=True),
                        round(missing_code, 4),
                        "the latest publication carries a stationCode for every station"))  # fmt: skip

    dupes = con.execute(
        """SELECT count(*) - count(DISTINCT (station_id, last_reported))
           FROM silver.station_status"""
    ).fetchone()[0]
    checks.append(Check("silver_unique", _status(dupes == 0), dupes,
                        "silver has one row per (station, last_reported)"))  # fmt: skip
    return checks


def as_dicts(checks: list[Check]) -> list[dict]:
    return [asdict(c) for c in checks]
