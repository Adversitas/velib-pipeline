"""Data contracts for the two feeds.

The producer's schema is not ours to control, so every record is validated
before it reaches the warehouse. A record that breaks the contract is not
dropped silently: it goes to a quarantine table with the reason, so a schema
change on the producer side shows up as a quarantine spike, not as missing data.

`station_id` is the key and is required. `stationCode` (the code printed on the
station) is optional: on 12 Nov 2026 the producer started publishing it as null
for every station. Rejecting whole publications over a secondary attribute would
have blanked the network; instead the last known code is kept from the station
history (see load.py) and the `station_code_missing` quality check reports it.

Rules beyond types (the ones that catch real producer bugs):
- counts are non-negative;
- the bike-type breakdown adds up to `num_bikes_available`;
- bikes + docks do not exceed the station's capacity (checked in `load.py`,
  where capacity from the other feed is available);
- `last_reported` is not in the future and not before the network existed.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

EARLIEST = 1_514_764_800  # 2018-01-01: the current Vélib' network launched that year


class StationStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")

    station_id: int
    station_code: str | None = Field(alias="stationCode")
    num_bikes_available: int = Field(ge=0)
    num_docks_available: int = Field(ge=0)
    mechanical: int | None = Field(None, ge=0)  # None: breakdown not published
    ebike: int | None = Field(None, ge=0)
    is_installed: bool
    is_renting: bool
    is_returning: bool
    last_reported: int

    @model_validator(mode="before")
    @classmethod
    def flatten_bike_types(cls, data: dict) -> dict:
        # The feed encodes types as [{"mechanical": 10}, {"ebike": 16}].
        types: dict[str, int] = {}
        for item in data.get("num_bikes_available_types") or []:
            types.update(item or {})
        if not types:
            return {**data, "mechanical": None, "ebike": None}
        return {**data, "mechanical": types.get("mechanical", 0), "ebike": types.get("ebike", 0)}

    @model_validator(mode="after")
    def check_consistency(self) -> StationStatus:
        known = self.mechanical is not None and self.ebike is not None
        if known and self.mechanical + self.ebike != self.num_bikes_available:
            raise ValueError(
                f"bike types ({self.mechanical}+{self.ebike}) != total {self.num_bikes_available}"
            )
        if self.last_reported < EARLIEST:
            raise ValueError(f"last_reported {self.last_reported} is before 2018")
        return self


class StationInformation(BaseModel):
    model_config = ConfigDict(extra="ignore")

    station_id: int
    station_code: str | None = Field(alias="stationCode")
    name: str = Field(min_length=1)
    lat: float = Field(ge=48.6, le=49.1)  # Vélib' Métropole bounding box
    lon: float = Field(ge=2.0, le=2.7)
    # Event stations reach ~410 docks (Invalides, Concorde); 500 still catches unit errors.
    capacity: int = Field(ge=0, le=500)


def validate_records(model: type[BaseModel], records: list[dict], published_at: int):
    """Split records into valid model instances and quarantine rows (record id, reason)."""
    valid, rejected = [], []
    for rec in records:
        try:
            obj = model.model_validate(rec)
        except ValidationError as exc:
            reason = "; ".join(
                f"{'.'.join(str(p) for p in e['loc']) or 'record'}: {e['msg']}"
                for e in exc.errors()
            )
            rejected.append({"station_id": rec.get("station_id"), "reason": reason[:500]})
            continue
        if isinstance(obj, StationStatus) and obj.last_reported > published_at + 600:
            rejected.append(
                {"station_id": obj.station_id, "reason": "last_reported is after publication"}
            )
            continue
        valid.append(obj)
    return valid, rejected
