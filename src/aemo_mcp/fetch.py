"""Orchestration layer: dataset_id + filters + period → DataResponse.

This is the middle tier between server.py (input validation, tool surface)
and client.py / parsing.py (HTTP + CSV mechanics). It:

1. Resolves a curated dataset
2. Picks the right NEMWEB file(s) — usually the most recent by
   lexicographic filename order (or all files within a period window)
3. Fetches the ZIPs (cached + in-flight-deduped)
4. Unpacks + parses the multi-section CSVs
5. Applies user filters (region, interconnector, duid, fuel, ...)
6. Builds a DataResponse via shaping.build_response

Keeps server.py thin (validation only) and is the place to add new feeds
without touching the MCP tool surface.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from .client import AEMOAPIError, AEMOClient
from .curated import (
    CuratedDataset,
    CuratedFilter,
    CuratedFolder,
    compile_filename_regex,
)
from .duid_lookup import lookup_duids_for
from .models import DataResponse
from .parsing import AEMOParseError, find_sections, parse_csv, unzip
from .shaping import NEM_TZ, _parse_aemo_datetime, build_response

# Filename pattern: AEMO embeds the interval timestamp as the first ~12-digit
# group after the feed prefix. Capture it so we can window by period.
_TIMESTAMP_PATTERN = re.compile(r"(\d{12})")
# Archive filename: 8-digit date stamp (YYYYMMDD), one per trading day.
_ARCHIVE_DATE_PATTERN = re.compile(r"(\d{8})(?!\d)")
# Hours back from "now" considered "still in current". /Reports/Current/ holds
# ~48h of 5-min files, but downloading every file for a wide window hits
# NEMWEB's anti-scraping limits. We pivot to /Reports/Archive/ (one zip per
# day) for any window that starts more than this many hours ago.
_CURRENT_WINDOW_HOURS = 4
# Maximum number of per-file downloads we'll do from /Current/ in one call.
# Beyond this we force archive use to avoid drowning NEMWEB.
_MAX_CURRENT_FILES = 24


class FetchError(Exception):
    """Raised when fetching/parsing a dataset fails in a user-visible way."""


def _extract_filename_timestamp(filename: str) -> datetime | None:
    """Pull the first 12-digit YYYYMMDDHHmm timestamp out of a filename.

    Returns a tz-aware datetime in NEM time.
    """
    m = _TIMESTAMP_PATTERN.search(filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d%H%M").replace(tzinfo=NEM_TZ)
    except ValueError:
        return None


def _filenames_in_window(
    filenames: list[str],
    start: datetime | None,
    end: datetime | None,
) -> list[str]:
    """Filter a directory listing down to files whose timestamp falls in [start, end].

    Both bounds inclusive. If both are None, return the full list.
    """
    if start is None and end is None:
        return filenames
    out: list[str] = []
    for fn in filenames:
        ts = _extract_filename_timestamp(fn)
        if ts is None:
            continue
        if start is not None and ts < start:
            continue
        if end is not None and ts > end:
            continue
        out.append(fn)
    return out


def _parse_period(s: str | None) -> datetime | None:
    """Parse user-supplied period strings.

    Accepts:
      - YYYY              → Jan 1
      - YYYY-MM           → first of month
      - YYYY-MM-DD        → midnight
      - YYYY-MM-DD HH:MM  → that minute
      - ISO 8601          → tz-aware preferred

    Returns a tz-aware datetime in NEM time.
    """
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=NEM_TZ)
        except ValueError:
            continue
    raise FetchError(
        f"Could not parse period {s!r}. Use 'YYYY' (annual), 'YYYY-MM' "
        "(monthly), 'YYYY-MM-DD' (daily), or 'YYYY-MM-DD HH:MM' (5-min). "
        "Example: '2026-05-14' or '2026-05-14 09:00'."
    )


def _row_matches_filters(
    row: dict[str, str],
    dataset: CuratedDataset,
    filters: dict[str, Any] | None,
    resolved_duids: set[str] | None,
) -> bool:
    """True if this row matches the user-supplied filters."""
    if not filters:
        return True
    for key, val in filters.items():
        if val is None or val == "" or val == []:
            continue
        f = dataset.get_filter(key)
        if f is None:
            # Skip unknown filters — server.py will have warned the caller.
            continue
        # The "section" filter is a synthesised discriminator handled at the
        # folder-selection level (see fetch_dataset). It does not map to a
        # CSV column — skip it here so we don't accidentally reject rows.
        if key == "section":
            continue
        col = f.column or key.upper()
        if key == "duid" and resolved_duids is not None:
            # If we pre-resolved a DUID list (eg from region/fuel filter),
            # the row's DUID must be in that set.
            if row.get("DUID", "") not in resolved_duids:
                return False
            continue
        if key in ("region", "fuel") and resolved_duids is not None:
            # Region/fuel resolved to a DUID set for generation_scada.
            if row.get("DUID", "") not in resolved_duids:
                return False
            continue
        # Normal column equality (case-insensitive for safety)
        cell = row.get(col, "")
        if isinstance(val, list):
            if cell not in val and cell.upper() not in {v.upper() for v in val}:
                return False
        else:
            if cell != val and cell.upper() != str(val).upper():
                return False
    return True


def _resolve_duid_filter(
    dataset_id: str, filters: dict[str, Any] | None
) -> set[str] | None:
    """For generation_scada, turn region/fuel/duid filters into a DUID allow-set.

    Returns None if no DUID-narrowing filter was supplied or the dataset
    isn't generation_scada.
    """
    if dataset_id != "generation_scada":
        return None
    if not filters:
        return None
    region = filters.get("region")
    fuel = filters.get("fuel")
    duid = filters.get("duid")
    if region is None and fuel is None and duid is None:
        return None
    # Start from "all" — narrow by each filter present.
    allow: set[str] | None = None
    if region is not None:
        allow = set(lookup_duids_for(region=region))
    if fuel is not None:
        f_set = set(lookup_duids_for(fuel=fuel))
        allow = f_set if allow is None else allow & f_set
    if duid is not None:
        if isinstance(duid, list):
            d_set = {str(d).upper() for d in duid}
        else:
            d_set = {str(duid).upper()}
        allow = d_set if allow is None else allow & d_set
    return allow


async def fetch_dataset(
    client: AEMOClient,
    dataset: CuratedDataset,
    filters: dict[str, Any] | None,
    start_period: str | None,
    end_period: str | None,
    fmt: str,
    only_latest: bool = False,
) -> DataResponse:
    """Top-level fetch: discover → download → parse → filter → shape."""
    start_dt = _parse_period(start_period)
    end_dt = _parse_period(end_period)
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise FetchError(
            f"end_period ({end_period}) is before start_period ({start_period})."
        )

    resolved_duids = _resolve_duid_filter(dataset.id, filters)

    sections_with_discriminator: list[tuple[str | None, list[dict[str, str]]]] = []

    # If the user supplied a discriminator filter (e.g. section=actual/forecast),
    # narrow down which folders we hit so we don't pay for unnecessary listings.
    requested_section = None
    if filters:
        sec_val = filters.get("section")
        if isinstance(sec_val, str) and sec_val.strip():
            requested_section = sec_val.strip().lower()

    for folder in dataset.folders:
        if (
            requested_section is not None
            and folder.discriminator is not None
            and folder.discriminator.lower() != requested_section
        ):
            continue
        # Decide whether to use /Current/ or /Archive/ for this query.
        use_archive = _should_use_archive(start_dt, end_dt)
        zip_bodies: list[tuple[str, bytes]] = []  # (url, body) pairs

        if use_archive:
            zip_bodies = await _fetch_archive_zips(
                client, folder, dataset.cache_kind, start_dt, end_dt
            )
            if not zip_bodies:
                # Fall back to Current if archive returns nothing (e.g. partial
                # window that crosses the archive cutover).
                zip_bodies = await _fetch_current_zips(
                    client, folder, dataset.cache_kind, start_dt, end_dt, only_latest
                )
        else:
            zip_bodies = await _fetch_current_zips(
                client, folder, dataset.cache_kind, start_dt, end_dt, only_latest
            )

        if not zip_bodies:
            continue

        for url, body in zip_bodies:
            try:
                inner = unzip(body)
            except AEMOParseError as e:
                raise FetchError(f"{url}: {e}") from e

            # An archive zip is a ZIP-of-ZIPs — its inner entries are
            # themselves the original 5-min ZIPs that once lived in /Current/.
            # Walk one extra level if the inner bytes look like ZIPs.
            csv_blobs: list[bytes] = []
            for inner_name, inner_bytes in inner.items():
                if _looks_like_zip(inner_bytes):
                    try:
                        nested = unzip(inner_bytes)
                    except AEMOParseError:
                        continue
                    for _n, nested_body in nested.items():
                        csv_blobs.append(nested_body)
                else:
                    csv_blobs.append(inner_bytes)

            for csv_bytes in csv_blobs:
                try:
                    sections = parse_csv(csv_bytes)
                except AEMOParseError as e:
                    # In a daily archive with 288 inner zips, one corrupt
                    # entry shouldn't kill the whole response. Skip it.
                    continue

                if folder.sections:
                    for cs in folder.sections:
                        matching = find_sections(sections, cs.name)
                        if not matching:
                            continue
                        # Combine records from all matching sections (eg
                        # DREGION. v2 + v3 in Daily_Reports). Dedupe by
                        # (settlement_column, all-filter-cols).
                        combined: list[dict[str, str]] = []
                        seen: set[tuple] = set()
                        for sec in matching:
                            for row in sec.records:
                                key = _row_dedup_key(row, dataset)
                                if key in seen:
                                    continue
                                seen.add(key)
                                combined.append(row)

                        discriminator = cs.discriminator or folder.discriminator
                        keep = []
                        for row in combined:
                            if not _row_matches_filters(
                                row, dataset, filters, resolved_duids
                            ):
                                continue
                            if start_dt is not None or end_dt is not None:
                                ts = _parse_aemo_datetime(
                                    row.get(dataset.settlement_column)
                                )
                                if ts is not None:
                                    if start_dt is not None and ts < start_dt:
                                        continue
                                    if end_dt is not None and ts > end_dt:
                                        continue
                            keep.append(row)
                        if only_latest and keep and not _is_forecast_folder(folder, dataset):
                            keep = _filter_to_latest_per_group(keep, dataset)
                        sections_with_discriminator.append((discriminator, keep))
                else:
                    # No section filter: emit one entry per section.
                    for sec in sections:
                        keep = [
                            row
                            for row in sec.records
                            if _row_matches_filters(
                                row, dataset, filters, resolved_duids
                            )
                        ]
                        sections_with_discriminator.append((sec.name, keep))

    use_discriminator = any(
        d is not None for d, _ in sections_with_discriminator
    )
    response_rows: list[dict[str, str]] | None = None
    if not use_discriminator and sections_with_discriminator:
        # Flatten — single-section response
        response_rows = []
        for _d, rows in sections_with_discriminator:
            response_rows.extend(rows)

    user_query: dict[str, Any] = {}
    if filters:
        user_query["filters"] = filters
    if start_period:
        user_query["start_period"] = start_period
    if end_period:
        user_query["end_period"] = end_period
    if only_latest:
        user_query["latest"] = True

    return build_response(
        dataset=dataset,
        rows=response_rows if not use_discriminator else None,
        sections_with_discriminator=(
            sections_with_discriminator if use_discriminator else None
        ),
        fmt=fmt,
        user_query=user_query,
        source_url=dataset.source_url,
        start_period=start_period,
        end_period=end_period,
    )


def _looks_like_zip(body: bytes) -> bool:
    """Cheap signature check — ZIPs start with 'PK\\x03\\x04' (local file header)."""
    return len(body) >= 4 and body[:2] == b"PK"


def _is_forecast_folder(folder: CuratedFolder, dataset: CuratedDataset) -> bool:
    """True if this folder publishes multiple future periods per file.

    For these, `latest()` should return ALL periods of the most-recent run
    (the full forward curve), not collapse to one row per filter dimension.
    """
    if folder.discriminator and folder.discriminator.lower() == "forecast":
        return True
    return dataset.cache_kind == "forecast"


def _should_use_archive(
    start_dt: datetime | None, end_dt: datetime | None
) -> bool:
    """True when the requested window is older than the /Current/ rolling window.

    AEMO's `/Reports/Current/` typically holds the last ~24-48h of 5-min data;
    older intervals are rolled into `/Reports/Archive/<feed>/PUBLIC_<feed>_YYYYMMDD.zip`
    daily compendia (ZIP-of-ZIPs).
    """
    if start_dt is None and end_dt is None:
        return False
    cutoff = datetime.now(NEM_TZ) - timedelta(hours=_CURRENT_WINDOW_HOURS)
    # If the user's whole window is older than the cutoff, use archive.
    if end_dt is not None and end_dt < cutoff:
        return True
    # If they're asking about a wide window starting before cutoff,
    # also archive-first (the per-day zips cover the whole window).
    if start_dt is not None and start_dt < cutoff:
        return True
    return False


async def _fetch_current_zips(
    client: AEMOClient,
    folder: CuratedFolder,
    cache_kind: str,
    start_dt: datetime | None,
    end_dt: datetime | None,
    only_latest: bool,
) -> list[tuple[str, bytes]]:
    """Fetch the listing for /Reports/Current/<folder> + the matching ZIPs.

    Resilient to individual file 403/404 — NEMWEB rolls files in/out of
    /Current/ continuously, so a file present in the listing may have moved
    to /Archive/ by the time we GET it. Skip and continue rather than
    failing the whole response.
    """
    regex = compile_filename_regex(folder)
    try:
        filenames = await client.fetch_directory_listing(
            folder.path, filename_regex=regex
        )
    except AEMOAPIError as e:
        raise FetchError(
            f"Could not list NEMWEB folder {folder.path}: {e}"
        ) from e

    if not filenames:
        return []

    if only_latest:
        target = [filenames[-1]]
    elif start_dt is not None or end_dt is not None:
        target = _filenames_in_window(filenames, start_dt, end_dt) or [filenames[-1]]
    else:
        target = [filenames[-1]]

    # Cap the number of /Current/ files we will download in one call. Wider
    # windows must use the archive (one zip per day).
    if len(target) > _MAX_CURRENT_FILES:
        target = target[-_MAX_CURRENT_FILES:]

    out: list[tuple[str, bytes]] = []
    for filename in target:
        url = client.build_url(folder.path, filename)
        try:
            body = await client.fetch_zip(url, kind=cache_kind)  # type: ignore[arg-type]
        except AEMOAPIError:
            # One missing/expired file shouldn't fail the whole response.
            continue
        out.append((url, body))
    return out


async def _fetch_archive_zips(
    client: AEMOClient,
    folder: CuratedFolder,
    cache_kind: str,
    start_dt: datetime | None,
    end_dt: datetime | None,
) -> list[tuple[str, bytes]]:
    """Fetch daily archive ZIPs (one per day) covering the requested window.

    Archive path: `/Reports/Archive/<folder_name>/` where <folder_name> is the
    last path segment of `folder.path` (e.g. `DispatchIS_Reports`).
    Each entry is named `PUBLIC_<FEED>_YYYYMMDD.zip` — a ZIP-of-ZIPs holding
    that day's 5-min files.

    Returns [] if the archive folder is empty or doesn't exist (caller will
    fall back to current).
    """
    if start_dt is None:
        # Default to a one-day archive window ending at end_dt or yesterday.
        if end_dt is None:
            return []
        start_dt = end_dt - timedelta(days=1)
    if end_dt is None:
        end_dt = datetime.now(NEM_TZ)

    # Pivot /Reports/Current/<X>/ → /Reports/Archive/<X>/. The "Current"
    # segment must NOT be carried into the archive path.
    seg = folder.path.strip("/").split("/")
    if (
        len(seg) < 3
        or seg[0].lower() != "reports"
        or seg[1].lower() != "current"
    ):
        # Non-standard folder layout — no archive fallback.
        return []
    archive_path = "/Reports/Archive/" + "/".join(seg[2:]) + "/"

    try:
        filenames = await client.fetch_directory_listing(
            archive_path,
            filename_regex=re.compile(r".+\.zip"),
        )
    except AEMOAPIError:
        return []
    if not filenames:
        return []

    # Filter filenames to those whose embedded YYYYMMDD falls in [start, end]
    # day-window. Archive filenames look like PUBLIC_DISPATCHIS_20250504.zip.
    start_d = start_dt.date()
    end_d = end_dt.date()
    target: list[str] = []
    for fn in filenames:
        m = _ARCHIVE_DATE_PATTERN.search(fn)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            continue
        if start_d <= d <= end_d:
            target.append(fn)

    if not target:
        return []

    out: list[tuple[str, bytes]] = []
    # Cap the number of days we'll fetch in one call to keep responses
    # bounded — 31 days covers a month, beyond that the user should narrow.
    MAX_ARCHIVE_DAYS = 31
    for filename in target[:MAX_ARCHIVE_DAYS]:
        url = client.build_url(archive_path, filename)
        try:
            body = await client.fetch_zip(url, kind="archive")
        except AEMOAPIError as e:
            # One missing day shouldn't fail the whole window.
            continue
        out.append((url, body))
    return out


def _row_dedup_key(row: dict[str, str], dataset: CuratedDataset) -> tuple:
    """Build a deterministic key for a row across (settlement, filter columns).

    Used to dedupe rows when the same section appears in two schema versions
    in one file (e.g. DREGION. v2 + v3 in Daily_Reports).
    """
    cols = [dataset.settlement_column] + [
        f.column or f.key.upper() for f in dataset.filters
    ]
    return tuple((c, row.get(c, "")) for c in cols)


def _filter_to_latest_per_group(
    rows: list[dict[str, str]], dataset: CuratedDataset
) -> list[dict[str, str]]:
    """For `latest()`: collapse to the most-recent settlement_time row per
    distinct combination of filter dimensions.

    For example: dispatch_price has 5 regions in one file. `latest()` should
    return 5 rows (one per region), all at the latest SETTLEMENTDATE — not
    every row in the file.
    """
    if not rows:
        return rows
    # Build dimension tuple keys
    dim_keys = [f.column or f.key.upper() for f in dataset.filters]

    def dim_tuple(row: dict[str, str]) -> tuple[tuple[str, str], ...]:
        return tuple((k, row.get(k, "")) for k in dim_keys)

    latest_per_dim: dict[tuple, dict[str, str]] = {}
    latest_dt_per_dim: dict[tuple, datetime | None] = {}
    for row in rows:
        dt = _parse_aemo_datetime(row.get(dataset.settlement_column))
        key = dim_tuple(row)
        existing_dt = latest_dt_per_dim.get(key)
        if existing_dt is None or (dt is not None and (existing_dt is None or dt > existing_dt)):
            latest_dt_per_dim[key] = dt
            latest_per_dim[key] = row
    return list(latest_per_dim.values())


# Re-exported for tests
__all__ = [
    "FetchError",
    "fetch_dataset",
    "_extract_filename_timestamp",
    "_filenames_in_window",
    "_parse_period",
    "_resolve_duid_filter",
]
