"""Tests run on synthetic feeds shaped like the real ones: no network, fully deterministic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb
import httpx
import pytest

from velib import ingest, load, quality, report
from velib.contracts import StationInformation, StationStatus, validate_records

T0 = 1_794_500_000


def status(sid: int, bikes: int, docks: int, ebike: int = 0, reported: int = T0) -> dict:
    return {
        "station_id": sid, "stationCode": str(sid), "num_bikes_available": bikes,
        "num_bikes_available_types": [{"mechanical": bikes - ebike}, {"ebike": ebike}],
        "num_docks_available": docks, "is_installed": 1, "is_renting": 1, "is_returning": 1,
        "last_reported": reported,
    }  # fmt: skip


def info(sid: int, capacity: int = 20, name: str | None = None) -> dict:
    return {"station_id": sid, "stationCode": str(sid), "name": name or f"Station {sid}",
            "lat": 48.86, "lon": 2.35, "capacity": capacity}  # fmt: skip


def feed(stations: list[dict], published: int) -> dict:
    return {"lastUpdatedOther": published, "ttl": 3600, "data": {"stations": stations}}


@pytest.fixture
def lake(tmp_path):
    return tmp_path / "lake"


# Contracts ------------------------------------------------------------------------------


def test_missing_breakdown_is_unknown_not_invalid():
    """Regression for the degraded publication of 12 Nov 2026 (types published as [{}, {}])."""
    record = {**status(1, 25, 9), "num_bikes_available_types": [{}, {}], "stationCode": None}
    [s], bad = validate_records(StationStatus, [record], T0)
    assert bad == [] and s.ebike is None and s.mechanical is None and s.station_code is None


def test_valid_status_is_flattened():
    [s], bad = validate_records(StationStatus, [status(1, 10, 5, ebike=4)], T0)
    assert bad == [] and (s.mechanical, s.ebike) == (6, 4)


@pytest.mark.parametrize(
    "record, reason",
    [
        ({**status(1, 10, 5), "num_bikes_available": -1}, "greater than or equal to 0"),
        ({**status(1, 10, 5), "num_bikes_available_types": [{"mechanical": 3}]}, "bike types"),
        (status(1, 10, 5, reported=1_000_000_000), "before 2018"),
        (status(1, 10, 5, reported=T0 + 3600), "after publication"),
        ({k: v for k, v in status(1, 10, 5).items() if k != "station_id"}, "station_id"),
    ],
)
def test_contract_violations_are_quarantined_with_a_reason(record, reason):
    valid, bad = validate_records(StationStatus, [record], T0)
    assert valid == [] and reason in bad[0]["reason"]


def test_station_information_bounds():
    _, bad = validate_records(StationInformation, [{**info(1), "lat": 43.3}], T0)
    assert "lat" in bad[0]["reason"]
    ok, _ = validate_records(StationInformation, [info(2, capacity=410)], T0)
    assert ok  # real event stations have ~410 docks


# Ingestion ------------------------------------------------------------------------------


def _client(payloads: dict[str, dict]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1].removesuffix(".json")
        return httpx.Response(200, json=payloads[name])

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_ingestion_is_idempotent(lake):
    client = _client({"station_status": feed([status(1, 5, 5)], T0),
                      "station_information": feed([info(1)], T0)})  # fmt: skip
    first = ingest.ingest(lake, client)
    again = ingest.ingest(lake, client)
    assert all(r.new for r in first) and not any(r.new for r in again)
    assert len(list(lake.rglob("*.json.gz"))) == 2


def test_ingestion_rejects_unexpected_payload(lake):
    client = _client({"station_status": {"oops": 1}, "station_information": feed([], T0)})
    with pytest.raises(ValueError):
        ingest.fetch("station_status", client, retries=1)


# Loading --------------------------------------------------------------------------------


def _land(lake, published, statuses, infos=None):
    ingest.land("station_status", feed(statuses, published), lake)
    if infos is not None:
        ingest.land("station_information", feed(infos, published), lake)


def test_incremental_load_dedup_and_quarantine(lake, tmp_path):
    db = tmp_path / "w.duckdb"
    _land(lake, T0, [status(1, 5, 15), status(2, 0, 20)], [info(1), info(2)])
    first = load.run(db, lake)
    assert first == {"files": 2, "records": 4, "rejected": 0}
    # Next publication: station 1 unchanged (same last_reported), station 2 updated,
    # plus one record that breaks the contract.
    bad = {**status(3, 2, 2), "num_bikes_available_types": [{"mechanical": 9}]}
    _land(lake, T0 + 60, [status(1, 5, 15), status(2, 3, 17, reported=T0 + 50), bad])
    second = load.run(db, lake)
    assert second == {"files": 1, "records": 2, "rejected": 1}
    with duckdb.connect(str(db), read_only=True) as con:
        latest = {c.name: c for c in quality.run_checks(con)}["quarantine_rate"]
    assert latest.value == round(1 / 3, 5) and latest.status == "fail"
    assert load.run(db, lake)["files"] == 0  # nothing pending: re-runs are no-ops

    with duckdb.connect(str(db)) as con:
        assert con.execute("SELECT count(*) FROM bronze.station_status").fetchone()[0] == 4
        assert con.execute("SELECT count(*) FROM silver.station_status").fetchone()[0] == 3
        assert con.execute("SELECT count(*) FROM quarantine.records").fetchone()[0] == 1
        snap = con.execute(
            "SELECT stations, share_empty FROM gold.network_snapshot ORDER BY 1"
        ).fetchall()
        assert snap[0] == (2, 0.5)


def test_scd2_station_history(lake, tmp_path):
    db = tmp_path / "w.duckdb"
    _land(lake, T0, [status(1, 1, 1)], [info(1, capacity=20, name="Old name")])
    load.run(db, lake)
    _land(lake, T0 + 60, [status(1, 1, 1)], [info(1, capacity=24, name="New name")])
    load.run(db, lake)
    with duckdb.connect(str(db)) as con:
        rows = con.execute(
            "SELECT name, capacity, is_current, valid_to IS NULL FROM silver.stations "
            "ORDER BY valid_from"
        ).fetchall()
    assert rows == [("Old name", 20, False, False), ("New name", 24, True, True)]


def test_a_failing_file_leaves_no_partial_load(lake, tmp_path, monkeypatch):
    db = tmp_path / "w.duckdb"
    _land(lake, T0, [status(1, 1, 1)], [info(1)])

    def boom(*_args, **_kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setitem(load.LOADERS, "station_status", boom)
    with pytest.raises(RuntimeError):
        load.run(db, lake)
    monkeypatch.undo()
    assert load.run(db, lake)["files"] == 1  # the status file is still pending and loads now


# Quality and report ---------------------------------------------------------------------


def test_quality_checks_flag_stale_and_small_feeds(lake, tmp_path):
    db = tmp_path / "w.duckdb"
    stations = [status(i, 2, 2) for i in range(1, 11)]
    stations.append(status(99, 0, 0, reported=T0 - 5 * 86400))  # silent for 5 days
    _land(lake, T0, stations, [info(i) for i in [*range(1, 11), 99]])
    load.run(db, lake)
    published = datetime.fromtimestamp(T0, tz=timezone.utc).replace(tzinfo=None)
    with duckdb.connect(str(db), read_only=True) as con:
        checks = {c.name: c for c in quality.run_checks(con, now=published + timedelta(minutes=5))}
        stale_later = quality.run_checks(con, now=published + timedelta(hours=5))
    assert checks["freshness"].status == "ok"
    assert checks["volume"].status == "fail"  # 11 stations, not a full network
    assert checks["stale_stations"].value == 1
    assert {c.name: c.status for c in stale_later}["freshness"] == "fail"


def test_report_renders(lake, tmp_path):
    db = tmp_path / "w.duckdb"
    _land(lake, T0, [status(i, i % 3, 5) for i in range(1, 6)], [info(i) for i in range(1, 6)])
    load.run(db, lake)
    page = report.build(db, tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    assert "Stations le plus souvent vides" in page and "{stations}" not in page


def test_null_station_code_keeps_last_known_code(lake, tmp_path):
    """Regression for the producer change of 12 Nov 2026 (stationCode published as null)."""
    db = tmp_path / "w.duckdb"
    _land(lake, T0, [status(1, 1, 1)], [info(1)])
    load.run(db, lake)
    _land(
        lake, T0 + 60,
        [{**status(1, 2, 0, reported=T0 + 30), "stationCode": None}],
        [{**info(1), "stationCode": None}],
    )  # fmt: skip
    assert load.run(db, lake)["rejected"] == 0
    with duckdb.connect(str(db), read_only=True) as con:
        stations = con.execute("SELECT station_code, is_current FROM silver.stations").fetchall()
        checks = {c.name: c for c in quality.run_checks(con)}
    assert stations == [("1", True)]  # no new SCD2 version, code preserved
    assert checks["station_code_missing"].status == "warn"


def test_empty_valid_batch_does_not_crash(lake, tmp_path):
    db = tmp_path / "w.duckdb"
    _land(lake, T0, [{**status(1, 1, 1), "num_bikes_available": -5}], [{**info(1), "lat": 0.0}])
    assert load.run(db, lake) == {"files": 2, "records": 0, "rejected": 2}
