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
from .parsing import AEMOParseError, find_section, parse_csv, unzip
from .shaping import NEM_TZ, _parse_aemo_datetime, build_response

# Filename pattern: AEMO embeds the interval timestamp as the first ~12-digit
# group after the feed prefix. Capture it so we can window by period.
_TIMESTAMP_PATTERN = re.compile(r"(\d{12})")


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
        f"Could not parse period {s!r}. Use 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', "
        "or 'YYYY-MM-DD HH:MM'."
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

    for folder in dataset.folders:
        regex = compile_filename_regex(folder)
        filenames: list[str]
        try:
            filenames = await client.fetch_directory_listing(
                folder.path, filename_regex=regex
            )
        except AEMOAPIError as e:
            raise FetchError(
                f"Could not list NEMWEB folder {folder.path}: {e}"
            ) from e

        if not filenames:
            continue

        # Pick which file(s) to download.
        # - only_latest=True (called from `latest()`)        → just the last file
        # - start/end set                                    → window slice
        # - otherwise                                        → just the last file
        #   (default behaviour without a period is "current")
        if only_latest:
            target_files = [filenames[-1]]
        elif start_dt is not None or end_dt is not None:
            windowed = _filenames_in_window(filenames, start_dt, end_dt)
            target_files = windowed or [filenames[-1]]
        else:
            target_files = [filenames[-1]]

        for filename in target_files:
            url = client.build_url(folder.path, filename)
            try:
                body = await client.fetch_zip(url, kind=dataset.cache_kind)
            except AEMOAPIError as e:
                raise FetchError(f"Could not download {url}: {e}") from e

            try:
                inner = unzip(body)
            except AEMOParseError as e:
                raise FetchError(f"{url}: {e}") from e

            for _name, csv_bytes in inner.items():
                try:
                    sections = parse_csv(csv_bytes)
                except AEMOParseError as e:
                    raise FetchError(f"{url}: {e}") from e

                # Choose which sections to extract. If the folder declares
                # sections, use those names; otherwise, take all sections.
                wanted = folder.sections or tuple(
                    s_name for s_name in {s.name for s in sections} for _ in [None]
                )
                if folder.sections:
                    for cs in folder.sections:
                        sec = find_section(sections, cs.name)
                        if sec is None:
                            continue
                        discriminator = cs.discriminator or folder.discriminator
                        # Filter rows by user criteria + (for windowed reads)
                        # the row's settlement timestamp.
                        keep = []
                        for row in sec.records:
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
                        if only_latest and keep:
                            # Reduce to the most recent settlement-time row(s)
                            # per dimension combination — keeps `latest()`
                            # returning one snapshot, not the whole file.
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
