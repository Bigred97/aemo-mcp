"""DUID → region/fuel lookup for generation_scada filtering.

NEMWEB's DISPATCH.UNIT_SCADA section only carries DUID + SCADAVALUE — to
filter by region or fuel we need to join against AEMO's DUDETAILSUMMARY.

For v0 we ship a static snapshot (data/duid_snapshot.csv) refreshed
periodically. DUID changes are infrequent (a unit registers/deregisters
once and stays), so a static snapshot remains accurate for months.

The snapshot is loaded lazily on first call and cached for the process.
"""
from __future__ import annotations

import csv
from importlib import resources
from pathlib import Path
from typing import Iterable

_CACHE: dict[str, dict[str, str]] | None = None


def _data_path() -> Path:
    try:
        ref = resources.files("aemo_mcp").joinpath("data/duid_snapshot.csv")
        if ref.is_file():
            return Path(str(ref))
    except (ModuleNotFoundError, AttributeError):
        pass
    here = Path(__file__).resolve().parent / "data" / "duid_snapshot.csv"
    if here.is_file():
        return here
    raise FileNotFoundError("Could not locate aemo_mcp/data/duid_snapshot.csv")


def _load() -> dict[str, dict[str, str]]:
    """Load duid_snapshot.csv → {duid: {region, fuel, station, ...}}.

    Lines starting with `#` are treated as comments. Empty DUID rows are
    skipped. Anything else parses as a DUID → (region, fuel, station, owner).
    """
    out: dict[str, dict[str, str]] = {}
    path = _data_path()
    with path.open("r", encoding="utf-8") as f:
        # Filter out comment lines BEFORE csv.DictReader sees them, so the
        # parser doesn't accidentally treat them as data rows.
        lines = [ln for ln in f if ln and not ln.lstrip().startswith("#")]
    reader = csv.DictReader(lines)
    for row in reader:
        duid = (row.get("DUID") or "").strip().upper()
        if not duid or duid.startswith("#"):
            continue
        out[duid] = {
            "region": (row.get("REGION") or "").strip().upper(),
            "fuel": (row.get("FUEL") or "").strip().lower(),
            "station": (row.get("STATION") or "").strip(),
            "owner": (row.get("OWNER") or "").strip(),
        }
    return out


def _registry() -> dict[str, dict[str, str]]:
    global _CACHE
    if _CACHE is None:
        _CACHE = _load()
    return _CACHE


def reset_registry() -> None:
    global _CACHE
    _CACHE = None


def all_duids() -> list[str]:
    return sorted(_registry().keys())


def duid_info(duid: str) -> dict[str, str] | None:
    return _registry().get(duid.strip().upper())


def all_fuels() -> list[str]:
    return sorted({info["fuel"] for info in _registry().values() if info.get("fuel")})


def all_regions() -> list[str]:
    return sorted({info["region"] for info in _registry().values() if info.get("region")})


def lookup_duids_for(
    region: str | None = None,
    fuel: str | None = None,
) -> list[str]:
    """Return DUIDs matching the given region and/or fuel filter.

    `region` is matched case-insensitively (NSW1, QLD1, SA1, TAS1, VIC1).
    `fuel` is matched case-insensitively against a small known set (black_coal,
    brown_coal, gas, hydro, wind, solar, battery, biomass, distillate).
    """
    region_u = region.strip().upper() if region else None
    fuel_l = fuel.strip().lower() if fuel else None
    out: list[str] = []
    for duid, info in _registry().items():
        if region_u and info.get("region") != region_u:
            continue
        if fuel_l and info.get("fuel") != fuel_l:
            continue
        out.append(duid)
    return out


def aggregate_by(
    rows: Iterable[dict[str, str]], dimension: str
) -> dict[str, list[str]]:
    """Group an iterable of NEMWEB rows by region or fuel of their DUID.

    Returns {dimension_value: [duids]}.
    """
    dim = dimension.strip().lower()
    if dim not in ("region", "fuel"):
        raise ValueError(
            f"Unsupported aggregation dimension {dimension!r}. "
            f"Valid options: ['region', 'fuel']. "
            f"Try aggregate_by(rows, 'region') for NSW1/QLD1/SA1/TAS1/VIC1 "
            f"grouping, or 'fuel' for black_coal/gas/wind/solar/battery/..."
        )
    out: dict[str, list[str]] = {}
    for row in rows:
        duid = (row.get("DUID") or "").strip().upper()
        if not duid:
            continue
        info = _registry().get(duid)
        if info is None:
            continue
        key = info.get(dim, "")
        if not key:
            continue
        out.setdefault(key, []).append(duid)
    return out
