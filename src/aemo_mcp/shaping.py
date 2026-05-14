"""Shape parsed AEMO records into DataResponse objects.

The pipeline:
  (CSV section, list[dict]) → filter by user query → flatten to Observations
                                                  → build DataResponse

Three output formats:
  - records: flat list of Observation (default)
  - series:  observations grouped by dimensions
  - csv:     re-serialise records as CSV text

No pandas at this layer — stdlib only, mirrors the rba-mcp shape.
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from typing import Any

from .curated import CuratedDataset
from .models import DataResponse, Observation

# AEMO market time = UTC+10 (no DST — NEM is brisbane-aligned year-round).
NEM_TZ = timezone(timedelta(hours=10), name="AEST")


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, str):
        v = v.strip()
        if not v:
            return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # Reject NaN to keep response payloads JSON-safe.
    if f != f:  # NaN check
        return None
    return f


def _parse_aemo_datetime(s: str | None) -> datetime | None:
    """Parse an AEMO timestamp string like '2026/05/14 10:05:00'.

    AEMO uses 'YYYY/MM/DD HH:MM:SS' in NEM-time. Return tz-aware UTC+10.
    """
    if not s:
        return None
    s = s.strip().strip('"')
    if not s:
        return None
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=NEM_TZ)
        except ValueError:
            continue
    return None


def _format_period(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def records_to_observations(
    rows: list[dict[str, str]],
    dataset: CuratedDataset,
    extra_dimensions: dict[str, str] | None = None,
) -> list[Observation]:
    """One Observation per (row × metric) tuple.

    `extra_dimensions` is merged into every observation's `dimensions` (used
    when the section discriminator distinguishes ACTUAL vs FORECAST or
    multiple folders pile into one dataset).
    """
    obs: list[Observation] = []
    if not rows:
        return obs
    for row in rows:
        period_dt = _parse_aemo_datetime(row.get(dataset.settlement_column))
        period = _format_period(period_dt) or row.get(dataset.settlement_column, "")
        base_dims: dict[str, str] = dict(extra_dimensions or {})
        # Add every filter column the dataset declared as a dimension on the
        # observation — REGIONID, INTERCONNECTORID, DUID, etc.
        for f in dataset.filters:
            col = f.column or f.key.upper()
            val = row.get(col)
            if val:
                base_dims[f.key] = val
        for metric in dataset.metrics:
            value = _safe_float(row.get(metric.source_column))
            if value is None:
                continue
            dims = dict(base_dims)
            dims["metric"] = metric.key
            obs.append(
                Observation(
                    period=period,
                    value=value,
                    dimensions=dims,
                    unit=metric.unit,
                )
            )
    return obs


def to_series(records: list[Observation]) -> list[dict[str, Any]]:
    """Group observations by dimensions tuple."""
    groups: dict[tuple, list[dict[str, Any]]] = {}
    keys_seen: dict[tuple, dict[str, str]] = {}
    for r in records:
        key = tuple(sorted(r.dimensions.items()))
        groups.setdefault(key, []).append({"period": r.period, "value": r.value})
        keys_seen[key] = r.dimensions
    out: list[dict[str, Any]] = []
    for key, obs_list in groups.items():
        out.append({"dimensions": keys_seen[key], "observations": obs_list})
    return out


def to_csv(records: list[Observation]) -> str:
    """Re-serialise observations as CSV. Columns: period, value, unit, <dims>."""
    if not records:
        return ""
    dim_keys: list[str] = []
    seen: set[str] = set()
    for r in records:
        for k in r.dimensions:
            if k not in seen:
                seen.add(k)
                dim_keys.append(k)
    buf = io.StringIO()
    fields = ["period", "value", "unit", *dim_keys]
    buf.write(",".join(fields) + "\n")
    for r in records:
        cells: list[str] = [
            r.period,
            "" if r.value is None else _csv_number(r.value),
            r.unit or "",
        ]
        for k in dim_keys:
            cells.append(r.dimensions.get(k, ""))
        buf.write(",".join(_csv_escape(c) for c in cells) + "\n")
    return buf.getvalue()


def _csv_number(v: float) -> str:
    """Render a float for CSV. Avoid scientific notation and trailing zeros."""
    if v == int(v):
        return str(int(v))
    return repr(v)


def _csv_escape(s: str) -> str:
    if any(c in s for c in ',"\n'):
        return '"' + s.replace('"', '""') + '"'
    return s


def is_stale(latest_period_iso: str | None, cadence_seconds: int) -> bool:
    """True if the latest observation is older than 2x the feed cadence."""
    if not latest_period_iso:
        return True
    try:
        dt = datetime.fromisoformat(latest_period_iso)
    except (ValueError, TypeError):
        return True
    now = datetime.now(NEM_TZ)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=NEM_TZ)
    return (now - dt).total_seconds() > 2 * cadence_seconds


def _cadence_seconds(cadence_label: str) -> int:
    label = cadence_label.strip().lower()
    if "5 min" in label or "5-min" in label:
        return 300
    if "30 min" in label or "half" in label or "half-hour" in label:
        return 1800
    if "daily" in label:
        return 86400
    if "hour" in label:
        return 3600
    return 600  # safe default


def build_response(
    dataset: CuratedDataset,
    rows: list[dict[str, str]] | None,
    sections_with_discriminator: list[tuple[str | None, list[dict[str, str]]]] | None,
    fmt: str,
    user_query: dict[str, Any],
    source_url: str,
    start_period: str | None,
    end_period: str | None,
) -> DataResponse:
    """Build a DataResponse from already-filtered AEMO rows.

    Pass either `rows` (single section) or `sections_with_discriminator`
    (a list of (discriminator, rows) pairs — for datasets that stitch
    actual+forecast or multi-folder feeds).
    """
    all_obs: list[Observation] = []
    if sections_with_discriminator is not None:
        for discriminator, section_rows in sections_with_discriminator:
            extra = {"section": discriminator} if discriminator else None
            all_obs.extend(records_to_observations(section_rows, dataset, extra))
    elif rows is not None:
        all_obs.extend(records_to_observations(rows, dataset))

    # Determine response unit if homogeneous
    response_unit: str | None = None
    units = {o.unit for o in all_obs if o.unit}
    if len(units) == 1:
        response_unit = next(iter(units))

    # interval_start/interval_end from data
    periods = sorted({o.period for o in all_obs if o.period})
    interval_start = periods[0] if periods else None
    interval_end = periods[-1] if periods else None

    records: list[Observation] | list[dict[str, Any]]
    csv_text: str | None = None
    if fmt == "csv":
        csv_text = to_csv(all_obs)
        records = []
    elif fmt == "series":
        records = to_series(all_obs)
    else:  # records
        records = all_obs

    stale = is_stale(interval_end, _cadence_seconds(dataset.cadence))

    resolved_start = start_period or interval_start
    resolved_end = end_period or interval_end

    return DataResponse(
        dataset_id=dataset.id,
        dataset_name=dataset.name,
        query=user_query,
        interval_start=resolved_start,
        interval_end=resolved_end,
        period={"start": resolved_start, "end": resolved_end},
        unit=response_unit,
        records=records,
        csv=csv_text,
        source_url=source_url,
        retrieved_at=datetime.now(timezone.utc),
        stale=stale,
    )
