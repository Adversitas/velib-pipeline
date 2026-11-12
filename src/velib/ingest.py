"""Extract: poll the Vélib' Métropole GBFS feeds into an immutable landing zone.

Each run fetches the two feeds the pipeline needs and stores them unchanged
(gzipped JSON), partitioned by fetch date:

    lake/landing/station_status/dt=2026-11-12/1794506235.json.gz
    lake/landing/station_information/dt=2026-11-12/1794506235.json.gz

The file name is the feed's own `lastUpdatedOther` timestamp, so fetching the
same publication twice writes the same file once: ingestion is idempotent and
can be retried or run on overlapping schedules without creating duplicates.
Raw files are never modified; everything downstream can be rebuilt from them.
"""

from __future__ import annotations

import gzip
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

BASE_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
FEEDS = ["station_status", "station_information"]
DEFAULT_LAKE = Path("lake")
USER_AGENT = "velib-pipeline (portfolio data project; github.com/Adversitas/velib-pipeline)"


@dataclass
class FetchResult:
    feed: str
    published_at: int
    path: Path
    new: bool
    stations: int


def fetch(feed: str, client: httpx.Client, retries: int = 3) -> dict:
    """GET one feed, retrying transient failures with backoff."""
    for attempt in range(1, retries + 1):
        try:
            r = client.get(f"{BASE_URL}/{feed}.json", timeout=30)
            r.raise_for_status()
            payload = r.json()
            if "data" not in payload or "lastUpdatedOther" not in payload:
                raise ValueError(f"{feed}: unexpected payload keys {list(payload)[:5]}")
            return payload
        except (httpx.HTTPError, ValueError):
            if attempt == retries:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def land(feed: str, payload: dict, lake: Path = DEFAULT_LAKE) -> FetchResult:
    published = int(payload["lastUpdatedOther"])
    day = datetime.fromtimestamp(published, tz=timezone.utc).strftime("%Y-%m-%d")
    path = lake / "landing" / feed / f"dt={day}" / f"{published}.json.gz"
    new = not path.exists()
    if new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(payload, f)
        tmp.replace(path)  # atomic: a crash never leaves a half-written file behind
    return FetchResult(feed, published, path, new, len(payload["data"]["stations"]))


def ingest(lake: Path = DEFAULT_LAKE, client: httpx.Client | None = None) -> list[FetchResult]:
    own = client is None
    client = client or httpx.Client(headers={"User-Agent": USER_AGENT})
    try:
        return [land(feed, fetch(feed, client), lake) for feed in FEEDS]
    finally:
        if own:
            client.close()
