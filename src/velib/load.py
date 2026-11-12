"""Transform and load: landing files -> DuckDB bronze / silver / gold, incrementally.

Each run processes only the landing files not yet recorded in `meta.ingestion_log`,
inside one transaction per file, so a crash mid-run leaves the warehouse exactly
as it was before that file and the next run picks it up again.

    bronze.station_status        every valid record, as published (with _published_at)
    quarantine.records           records that broke the contract, with the reason
    silver.station_status        one row per (station, last_reported): the feed repeats
                                 unchanged stations in every publication
    silver.stations              type-2 history of station name / capacity / position
    gold.*                       rebuilt from silver on every run (cheap, always consistent)
"""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .contracts import StationInformation, StationStatus, validate_records

DDL = """
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE TABLE IF NOT EXISTS meta.ingestion_log (
    file VARCHAR PRIMARY KEY, feed VARCHAR, published_at TIMESTAMP,
    records INTEGER, rejected INTEGER, processed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze.station_status (
    station_id BIGINT, station_code VARCHAR, num_bikes_available INTEGER,
    num_docks_available INTEGER, mechanical INTEGER, ebike INTEGER,
    is_installed BOOLEAN, is_renting BOOLEAN, is_returning BOOLEAN,
    last_reported TIMESTAMP, _published_at TIMESTAMP, _file VARCHAR
);
CREATE TABLE IF NOT EXISTS quarantine.records (
    feed VARCHAR, _file VARCHAR, station_id BIGINT, reason VARCHAR, _published_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.station_status (
    station_id BIGINT, station_code VARCHAR, num_bikes_available INTEGER,
    num_docks_available INTEGER, mechanical INTEGER, ebike INTEGER,
    is_installed BOOLEAN, is_renting BOOLEAN, is_returning BOOLEAN,
    last_reported TIMESTAMP, first_seen_at TIMESTAMP,
    PRIMARY KEY (station_id, last_reported)
);
CREATE TABLE IF NOT EXISTS silver.stations (
    station_id BIGINT, station_code VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE,
    capacity INTEGER, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN
);
"""


def _ts(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(tzinfo=None)


def pending_files(con: duckdb.DuckDBPyConnection, lake: Path, feed: str) -> list[Path]:
    done = {r[0] for r in con.execute("SELECT file FROM meta.ingestion_log").fetchall()}
    files = sorted((lake / "landing" / feed).glob("dt=*/*.json.gz"), key=lambda p: p.name)
    return [f for f in files if f.as_posix() not in done]


def _load_status(con, path: Path, payload: dict) -> tuple[int, int]:
    published = int(payload["lastUpdatedOther"])
    valid, rejected = validate_records(StationStatus, payload["data"]["stations"], published)
    pub, f = _ts(published), path.as_posix()
    rows = [
        (s.station_id, s.station_code, s.num_bikes_available, s.num_docks_available,
         s.mechanical, s.ebike, s.is_installed, s.is_renting, s.is_returning,
         _ts(s.last_reported), pub, f)
        for s in valid
    ]  # fmt: skip
    if rows:  # executemany rejects an empty batch
        con.executemany("INSERT INTO bronze.station_status VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    if rejected:
        con.executemany(
            "INSERT INTO quarantine.records VALUES ('station_status', ?, ?, ?, ?)",
            [(f, r["station_id"], r["reason"], pub) for r in rejected],
        )
    # New observations only: the feed republishes unchanged stations every time.
    con.execute(
        """
        INSERT INTO silver.station_status
        SELECT DISTINCT ON (station_id, last_reported)
            station_id, station_code, num_bikes_available, num_docks_available, mechanical,
            ebike, is_installed, is_renting, is_returning, last_reported, _published_at
        FROM bronze.station_status b
        WHERE b._file = ?
          AND NOT EXISTS (
            SELECT 1 FROM silver.station_status s
            WHERE s.station_id = b.station_id AND s.last_reported = b.last_reported)
        """,
        [f],
    )
    return len(valid), len(rejected)


def _load_information(con, path: Path, payload: dict) -> tuple[int, int]:
    published = int(payload["lastUpdatedOther"])
    valid, rejected = validate_records(StationInformation, payload["data"]["stations"], published)
    pub, f = _ts(published), path.as_posix()
    if rejected:
        con.executemany(
            "INSERT INTO quarantine.records VALUES ('station_information', ?, ?, ?, ?)",
            [(f, r["station_id"], r["reason"], pub) for r in rejected],
        )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE incoming (station_id BIGINT, station_code VARCHAR, "
        "name VARCHAR, lat DOUBLE, lon DOUBLE, capacity INTEGER)"
    )
    if valid:
        con.executemany(
            "INSERT INTO incoming VALUES (?,?,?,?,?,?)",
            [(s.station_id, s.station_code, s.name, s.lat, s.lon, s.capacity) for s in valid],
        )
    # A missing code is not a change: keep the last known one.
    con.execute(
        """
        UPDATE incoming i SET station_code = s.station_code
        FROM silver.stations s
        WHERE i.station_code IS NULL AND s.station_id = i.station_id AND s.is_current
        """
    )
    # SCD2: close the current version of stations whose attributes changed...
    con.execute(
        """
        UPDATE silver.stations s SET valid_to = ?, is_current = false
        FROM incoming i
        WHERE s.station_id = i.station_id AND s.is_current
          AND (s.name != i.name OR s.capacity != i.capacity
               OR abs(s.lat - i.lat) > 1e-5 OR abs(s.lon - i.lon) > 1e-5)
        """,
        [pub],
    )
    # ...and open a version for every station without a current one (new or changed).
    con.execute(
        """
        INSERT INTO silver.stations
        SELECT i.*, ?, NULL, true FROM incoming i
        WHERE NOT EXISTS (
            SELECT 1 FROM silver.stations s WHERE s.station_id = i.station_id AND s.is_current)
        """,
        [pub],
    )
    return len(valid), len(rejected)


LOADERS = {"station_information": _load_information, "station_status": _load_status}


def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE gold.network_snapshot AS
        -- One row per publication: the state of the whole network at that moment.
        SELECT
            _published_at AS published_at,
            count(*) AS stations,
            sum(num_bikes_available) AS bikes,
            sum(ebike) AS ebikes,
            -- e-bike share is only defined where the type breakdown was published
            sum(num_bikes_available) FILTER (WHERE ebike IS NOT NULL) AS bikes_typed,
            round(avg((num_bikes_available = 0)::INT), 4) AS share_empty,
            round(avg((num_docks_available = 0)::INT), 4) AS share_full,
            round(avg((NOT is_renting)::INT), 4) AS share_not_renting
        FROM bronze.station_status
        GROUP BY 1 ORDER BY 1;

        CREATE OR REPLACE TABLE gold.station_hourly AS
        SELECT
            b.station_id,
            date_trunc('hour', b._published_at) AS hour,
            round(avg(b.num_bikes_available), 2) AS avg_bikes,
            round(avg(b.ebike), 2) AS avg_ebikes,
            min(b.num_bikes_available) AS min_bikes,
            round(avg((b.num_bikes_available = 0)::INT), 3) AS share_empty,
            round(avg((b.num_docks_available = 0)::INT), 3) AS share_full,
            count(*) AS observations
        FROM bronze.station_status b
        GROUP BY ALL;

        CREATE OR REPLACE TABLE gold.station_service AS
        -- Service quality per station over everything loaded: how often a user finds
        -- no bike to take, or no dock to return one.
        SELECT
            s.station_id, s.station_code, s.name, s.capacity, s.lat, s.lon,
            count(*) AS observations,
            round(avg((b.num_bikes_available = 0)::INT), 3) AS share_empty,
            round(avg((b.num_docks_available = 0)::INT), 3) AS share_full,
            round(avg(b.num_bikes_available), 1) AS avg_bikes,
            round(avg(b.ebike / nullif(b.num_bikes_available, 0)), 3) AS ebike_share
        FROM bronze.station_status b
        JOIN silver.stations s ON s.station_id = b.station_id AND s.is_current
        GROUP BY ALL;

        CREATE OR REPLACE TABLE gold.stale_stations AS
        -- Stations still published by the live feed but silent for over a day: they
        -- inflate "available" counts and should be excluded or escalated to operations.
        WITH latest AS (
            SELECT station_id, max(last_reported) AS last_reported,
                   max(_published_at) AS seen_at
            FROM bronze.station_status GROUP BY 1
        )
        SELECT l.station_id, s.name, s.capacity, l.last_reported,
               round(date_diff('hour', l.last_reported, l.seen_at) / 24.0, 1) AS days_silent
        FROM latest l LEFT JOIN silver.stations s ON s.station_id = l.station_id AND s.is_current
        WHERE l.seen_at - l.last_reported > INTERVAL 1 DAY
        ORDER BY days_silent DESC;
        """
    )


def run(db_path: str | Path, lake: Path) -> dict:
    """Load every pending landing file, then rebuild gold. Returns what was done."""
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    summary = {"files": 0, "records": 0, "rejected": 0}
    # Station information first, so a status file never references an unknown station
    # that the same run is about to create.
    for feed in ["station_information", "station_status"]:
        for path in pending_files(con, lake, feed):
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
            con.execute("BEGIN")
            try:
                n, bad = LOADERS[feed](con, path, payload)
                con.execute(
                    "INSERT INTO meta.ingestion_log VALUES (?, ?, ?, ?, ?, now())",
                    [path.as_posix(), feed, _ts(int(payload["lastUpdatedOther"])), n, bad],
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            summary["files"] += 1
            summary["records"] += n
            summary["rejected"] += bad
    build_gold(con)
    con.close()
    return summary
