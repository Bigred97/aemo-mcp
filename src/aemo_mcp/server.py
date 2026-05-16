"""FastMCP server entrypoint for aemo-mcp.

Five tools, all thin orchestrators over `client`, `fetch`, `curated`,
`feeds`, and `shaping`. The shared `AEMOClient` is created lazily so
importing this module doesn't open the SQLite cache.

Validation guards mirror rba-mcp 0.1.9 / abs-mcp 0.2.x: explicit input-type
checks, URL-safe patterns for dataset IDs / region codes / period strings,
helpful error messages with "Try X" hints.
"""
from __future__ import annotations

import asyncio
import difflib
import re
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from pydantic import Field

from . import curated, feeds
from .client import (
    AEMOAPIError,
    AEMOClient,
    get_stale_signal,
    reset_stale_signal,
)
from .fetch import FetchError, fetch_dataset
from .models import DataResponse, DatasetDetail, DatasetSummary

# Dataset IDs are snake_case ASCII.
_DATASET_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
# Periods: YYYY, YYYY-MM, YYYY-MM-DD, optional HH:MM(:SS) tail.
_PERIOD_PATTERN = re.compile(r"^[0-9TZ:\- ]{4,25}$")
_VALID_FORMATS = {"records", "series", "csv"}

mcp = FastMCP("aemo-mcp")

_client: AEMOClient | None = None
_client_lock = asyncio.Lock()


def _suggest(needle: str, haystack: list[str], n: int = 1) -> str:
    """Return a `Did you mean 'X'?` suffix, or "" when no close match.

    Uses stdlib `difflib.get_close_matches` so we don't pull in rapidfuzz at
    the validation layer (rapidfuzz is already a runtime dep but kept out of
    error formatting paths to minimise import-time surface).
    """
    if not needle or not haystack:
        return ""
    matches = difflib.get_close_matches(
        needle.lower(), [h.lower() for h in haystack], n=n, cutoff=0.5
    )
    if not matches:
        return ""
    # Map back to original casing
    suggestion = next((h for h in haystack if h.lower() == matches[0]), matches[0])
    return f"Did you mean {suggestion!r}? "


async def _get_client() -> AEMOClient:
    global _client
    async with _client_lock:
        if _client is None:
            _client = AEMOClient()
        return _client


async def reset_client_for_tests() -> None:
    """Drop the cached client. Tests that span event loops must clear it."""
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        except Exception:
            pass
        _client = None


def _normalize_dataset_id(dataset_id: Any) -> str:
    if not isinstance(dataset_id, str):
        raise ValueError(
            f"dataset_id must be a string, got {type(dataset_id).__name__}. "
            "Try search_datasets() to discover IDs like 'dispatch_price' or "
            "'generation_scada'."
        )
    normalized = dataset_id.strip().lower()
    if not normalized:
        raise ValueError(
            "dataset_id is empty. Try search_datasets() to discover IDs "
            "like 'dispatch_price' or 'generation_scada'."
        )
    if not _DATASET_ID_PATTERN.match(normalized):
        raise ValueError(
            f"dataset_id {dataset_id!r} contains invalid characters — "
            "use snake_case ASCII like 'dispatch_price', 'generation_scada'. "
            "Try search_datasets() to discover valid IDs."
        )
    return normalized


def _validate_filters(filters: Any) -> dict[str, Any] | None:
    if filters is None:
        return None
    if isinstance(filters, str):
        import json as _json
        try:
            filters = _json.loads(filters)
        except _json.JSONDecodeError as exc:
            raise ValueError(
                f"filters must be a JSON object, got invalid JSON string: {exc}. "
                "Example: {\"region\": \"NSW1\", \"constraint_id\": \"C_V::N\"}."
            ) from exc
    if not isinstance(filters, dict):
        raise ValueError(
            f"filters must be a dict mapping filter keys to values, got "
            f"{type(filters).__name__}."
        )
    # No deep validation here — fetch._row_matches_filters handles unknown
    # keys gracefully and the curated YAML enumerates valid filter keys for
    # error messages in `_check_filter_keys`.
    return filters


def _validate_period(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    # MCP / LLM clients often send a year as a JSON number rather than a
    # string. Coerce int → str so both forms work. Exclude bool (subclasses
    # int) so True/False still raise type errors.
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str):
        raise ValueError(
            f"{field_name} must be a string in 'YYYY', 'YYYY-MM', "
            f"'YYYY-MM-DD', or 'YYYY-MM-DD HH:MM' format, "
            f"got {type(value).__name__}. "
            f"Example: {field_name}='2026-05-14' or '2026-05-14 09:00'."
        )
    s = value.strip()
    if not s:
        return None
    if not _PERIOD_PATTERN.match(s):
        raise ValueError(
            f"{field_name} {value!r} has invalid format. "
            "Use 'YYYY' (annual), 'YYYY-MM' (monthly), 'YYYY-MM-DD' (daily), "
            "or 'YYYY-MM-DD HH:MM' (5-min). "
            f"Example: {field_name}='2026-05-14' or '2026-05-14 09:00'."
        )
    return s


async def _fetch_with_stale_signal(
    cd: curated.CuratedDataset,
    filters: dict[str, Any] | None,
    start_validated: str | None,
    end_validated: str | None,
    fmt: str,
    only_latest: bool,
) -> DataResponse:
    """Run `fetch_dataset` under a reset stale-signal context and merge the
    cached-fallback flag into the returned DataResponse.

    Mirrors abs-mcp's `_get_data_impl` glue. The `stale` field on the response
    has dual meaning here (NEM delay OR cached fallback) so we OR our signal
    onto whatever shaping.build_response already set; `stale_reason` is set
    only when the cached-fallback path fired (the delay branch leaves it None,
    which agents can interpret as "delay" by looking at the latest interval
    versus retrieved_at).
    """
    reset_stale_signal()
    client = await _get_client()
    resp = await fetch_dataset(
        client,
        cd,
        filters,
        start_validated,
        end_validated,
        fmt,
        only_latest=only_latest,
    )
    stale, reason = get_stale_signal()
    if stale:
        resp.stale = True
        resp.stale_reason = reason
    return resp


def _check_filter_keys(dataset_id: str, filters: dict[str, Any] | None) -> None:
    if filters is None:
        return
    cd = curated.get(dataset_id)
    if cd is None:
        return
    known = sorted({f.key for f in cd.filters})
    unknown = [k for k in filters if k not in known]
    if unknown:
        hint = _suggest(unknown[0], known)
        raise ValueError(
            f"Unknown filter key(s) {unknown} for dataset '{dataset_id}'. "
            f"{hint}"
            f"Valid keys: {known}."
        )
    # Validate filter values too — invalid values used to return 0 records
    # silently, which agents misread as "no data" instead of "wrong code".
    _check_filter_values(cd, filters)


def _check_filter_values(
    cd: curated.CuratedDataset, filters: dict[str, Any]
) -> None:
    """Validate filter values against each CuratedFilter's `values` enumeration.

    Skips filters whose `values` is empty (open-ended like DUID or dataset
    enumerations too large to inline). For closed enumerations (region,
    interconnector, fuel, section, ...), raise on values not in the set —
    matching the user's casing for the suggestion.

    Pre-0.4.7, passing region='NSW' or fuel='coal' returned 0 records with
    no signal; agents saw the empty list as "no data for this NEM region"
    and gave up. Validation upfront produces a "Did you mean X?" hint.
    """
    for key, raw in filters.items():
        if raw is None or raw == "" or raw == []:
            continue
        f = cd.get_filter(key)
        if f is None or not f.values:
            continue
        # Build a canonical-value index for case-insensitive comparison.
        canonical = {v.lower(): v for v in f.values}
        # Accept a single string or a list of strings.
        values_to_check: list[Any]
        if isinstance(raw, list):
            values_to_check = list(raw)
        else:
            values_to_check = [raw]
        for val in values_to_check:
            if not isinstance(val, str):
                continue
            if val.lower() in canonical:
                continue
            hint = _suggest(val, list(f.values))
            raise ValueError(
                f"Filter {key}={val!r} is not a valid value for dataset "
                f"'{cd.id}'. {hint}"
                f"Valid {key} values: {list(f.values)}."
            )


@mcp.tool
async def search_datasets(
    query: Annotated[
        str,
        Field(
            description=(
                "Free-text search query. Matches against dataset IDs, names, "
                "descriptions, filter keys, region values, and search keywords. "
                "Case-insensitive."
            ),
            examples=[
                "spot price",
                "demand",
                "rooftop pv",
                "generation by fuel",
                "interconnector",
                "negative pricing",
            ],
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description=(
                "Maximum number of results to return, ranked by relevance."
            ),
            examples=[5, 7, 10],
            ge=1,
            le=100,
        ),
    ] = 10,
) -> list[DatasetSummary]:
    """Fuzzy-search the 7 curated AEMO NEM datasets.

    Use this when you don't know the exact dataset_id. The 7 curated
    datasets cover ~95% of typical NEM analytic queries — spot prices,
    demand, generation, rooftop PV, interconnector flows, forecasts.

    Examples:
        # Find the dataset that publishes the spot price
        results = await search_datasets("spot price")
        # → [{id: 'dispatch_price', name: 'NEM Dispatch Price ...', ...}]

        # Discover what's available on rooftop solar
        results = await search_datasets("rooftop pv", limit=5)

    Returns:
        List of DatasetSummary (id, name, description, cadence), ranked
        by relevance. All v0 datasets are curated.
    """
    if not isinstance(query, str):
        raise ValueError(
            f"query must be a string, got {type(query).__name__}. "
            "Try 'spot price', 'demand', 'rooftop pv', 'interconnector', or "
            "any other NEM topic."
        )
    if not query.strip():
        raise ValueError(
            "query is required. Try 'spot price', 'demand', 'rooftop pv', "
            "'interconnector', 'generation by fuel', or any other NEM topic."
        )
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError(
            f"limit must be a positive integer, got {limit!r} "
            f"({type(limit).__name__})."
        )
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}.")
    return feeds.search_datasets(query, limit=limit)


@mcp.tool
async def describe_dataset(
    dataset_id: Annotated[
        str,
        Field(
            description=(
                "Dataset ID like 'dispatch_price', 'generation_scada'. "
                "Use search_datasets() to discover, or list_curated() to "
                "enumerate. Case-insensitive."
            ),
            examples=[
                "dispatch_price",
                "dispatch_region",
                "interconnector_flows",
                "generation_scada",
                "rooftop_pv",
                "predispatch_30min",
                "daily_summary",
            ],
        ),
    ],
) -> DatasetDetail:
    """Describe one NEM dataset — schema, filters, cadence, source URL.

    Examples:
        detail = await describe_dataset("dispatch_price")
        # → filters: [{key: "region", values: ["NSW1", "QLD1", ...]}]
        # → metrics: {rrp: "$/MWh"}
        # → cadence: "5 min"

    Returns:
        DatasetDetail with id, name, description, filters, units, source URL,
        and example invocation strings.
    """
    norm = _normalize_dataset_id(dataset_id)
    cd = curated.get(norm)
    if cd is None:
        ids = curated.list_ids()
        hint = _suggest(norm, ids)
        raise ValueError(
            f"Dataset {dataset_id!r} is not a known AEMO dataset. "
            f"{hint}"
            f"Try search_datasets() to discover valid IDs, or list_curated() "
            f"to enumerate. All {len(ids)} IDs: {ids}"
        )
    return cd.to_detail()


@mcp.tool
async def get_data(
    dataset_id: Annotated[
        str,
        Field(
            description=(
                "Dataset ID like 'dispatch_price'. Use search_datasets() to "
                "discover."
            ),
            examples=[
                "dispatch_price",
                "dispatch_region",
                "interconnector_flows",
                "generation_scada",
                "rooftop_pv",
                "predispatch_30min",
                "daily_summary",
            ],
        ),
    ],
    filters: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Dict of filter key → value(s). Common filters: 'region' "
                "(NSW1/QLD1/SA1/TAS1/VIC1), 'interconnector' (V-SA/Basslink/...), "
                "'duid' (unit ID), 'fuel' (black_coal/gas/wind/solar/battery/...). "
                "Each dataset's valid filter keys + allowed values are listed "
                "in the dataset's detail metadata."
            ),
            examples=[
                {"region": "NSW1"},
                {"region": "QLD1", "fuel": "black_coal"},
                {"interconnector": "V-SA"},
                {"duid": "ER01"},
            ],
        ),
    ] = None,
    start_period: Annotated[
        str | int | None,
        Field(
            description=(
                "Inclusive start of the period window in AEMO market time "
                "(UTC+10). Accepts 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', or "
                "'YYYY-MM-DD HH:MM'. Defaults to None which fetches just "
                "the most recent NEMWEB file for the dataset."
            ),
            examples=["2026-05-14", "2026-05-13 09:00", 2026],
        ),
    ] = None,
    end_period: Annotated[
        str | int | None,
        Field(
            description="Inclusive end. Same format as start_period.",
            examples=["2026-05-14 23:55", "2026-05-14"],
        ),
    ] = None,
    format: Annotated[
        Literal["records", "series", "csv"],
        Field(
            description=(
                "Response shape. 'records' (default): flat list of "
                "observations. 'series': observations grouped by dimensions. "
                "'csv': returns the result as a CSV string in the `csv` field."
            ),
            examples=["records", "series", "csv"],
        ),
    ] = "records",
) -> DataResponse:
    """Query an AEMO NEM dataset and return observations.

    Examples:
        # Latest NSW dispatch price (preferred over latest() if you want
        # a window)
        resp = await get_data("dispatch_price", filters={"region": "NSW1"})

        # Whole-day NSW dispatch price for a specific day
        resp = await get_data(
            "dispatch_price",
            filters={"region": "NSW1"},
            start_period="2026-05-13",
            end_period="2026-05-13"
        )

        # Generation by fuel for QLD, current
        resp = await get_data("generation_scada", filters={"region": "QLD1"})

        # All 6 interconnectors right now
        resp = await get_data("interconnector_flows")

    Returns:
        DataResponse with records, units, period bounds, NEMWEB source URL,
        and AEMO attribution.
    """
    norm = _normalize_dataset_id(dataset_id)
    filters_validated = _validate_filters(filters)
    _check_filter_keys(norm, filters_validated)
    start_validated = _validate_period(start_period, "start_period")
    end_validated = _validate_period(end_period, "end_period")

    if format is None:
        fmt_norm = "records"
    elif isinstance(format, str):
        fmt_norm = format.lower()
    else:
        raise ValueError(
            f"format must be a string, got {type(format).__name__}. "
            f"Valid options: {sorted(_VALID_FORMATS)}. "
            f"Example: format='records' (default), 'series', or 'csv'."
        )
    if fmt_norm not in _VALID_FORMATS:
        hint = _suggest(fmt_norm, sorted(_VALID_FORMATS))
        raise ValueError(
            f"Unknown format {format!r}. {hint}"
            f"Valid options: {sorted(_VALID_FORMATS)}. "
            f"Example: format='records' (default), 'series', or 'csv'."
        )

    if start_validated and end_validated and start_validated > end_validated:
        raise ValueError(
            f"end_period ({end_validated}) is before start_period "
            f"({start_validated}). Try swapping them. "
            f"Period formats: 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', "
            f"or 'YYYY-MM-DD HH:MM'."
        )

    cd = curated.get(norm)
    if cd is None:
        ids = curated.list_ids()
        hint = _suggest(norm, ids)
        raise ValueError(
            f"Dataset {dataset_id!r} is not a known AEMO dataset. "
            f"{hint}"
            f"Try search_datasets() to discover valid IDs, or list_curated() "
            f"to enumerate. All {len(ids)} IDs: {ids}"
        )

    try:
        return await _fetch_with_stale_signal(
            cd,
            filters_validated,
            start_validated,
            end_validated,
            fmt_norm,
            only_latest=False,
        )
    except FetchError as e:
        raise ValueError(str(e)) from e
    except AEMOAPIError as e:
        raise ValueError(
            f"NEMWEB request failed: {e}. "
            f"NEMWEB occasionally rolls files between current and archive; "
            f"try narrowing the period or retrying in 30s."
        ) from e


@mcp.tool
async def latest(
    dataset_id: Annotated[
        str,
        Field(
            description=(
                "Dataset ID like 'dispatch_price'. Use search_datasets() to "
                "discover."
            ),
            examples=[
                "dispatch_price",
                "dispatch_region",
                "interconnector_flows",
                "generation_scada",
                "rooftop_pv",
            ],
        ),
    ],
    filters: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Optional filter dict. Same shape as get_data — narrow to a "
                "region, interconnector, fuel, etc."
            ),
            examples=[
                {"region": "NSW1"},
                {"region": "QLD1", "fuel": "wind"},
                {"interconnector": "V-SA"},
            ],
        ),
    ] = None,
) -> DataResponse:
    """Return the most recent interval(s) for a NEM dataset.

    For 5-min feeds (dispatch_price, dispatch_region, interconnector_flows,
    generation_scada): returns the most recent 5-minute interval, typically
    1-2 minutes after the interval close.

    For 30-min feeds (rooftop_pv, predispatch_30min): the most recent
    half-hour.

    For daily feeds (daily_summary): yesterday's data.

    Examples:
        # Current NSW spot price
        resp = await latest("dispatch_price", filters={"region": "NSW1"})

        # Current generation mix in QLD
        resp = await latest("generation_scada", filters={"region": "QLD1"})

        # Current flow across Heywood
        resp = await latest("interconnector_flows", filters={"interconnector": "V-SA"})

    Returns:
        DataResponse with one observation per filtered (dimension, metric)
        tuple at the most recent interval. `stale=True` flag indicates the
        most recent interval is older than 2× the feed cadence (NEMWEB
        delay).
    """
    norm = _normalize_dataset_id(dataset_id)
    filters_validated = _validate_filters(filters)
    _check_filter_keys(norm, filters_validated)
    cd = curated.get(norm)
    if cd is None:
        ids = curated.list_ids()
        hint = _suggest(norm, ids)
        raise ValueError(
            f"Dataset {dataset_id!r} is not a known AEMO dataset. "
            f"{hint}"
            f"Try search_datasets() to discover valid IDs, or list_curated() "
            f"to enumerate. All {len(ids)} IDs: {ids}"
        )
    try:
        return await _fetch_with_stale_signal(
            cd,
            filters_validated,
            None,
            None,
            "records",
            only_latest=True,
        )
    except FetchError as e:
        raise ValueError(str(e)) from e
    except AEMOAPIError as e:
        raise ValueError(
            f"NEMWEB request failed: {e}. "
            f"NEMWEB occasionally rolls files between current and archive; "
            f"try retrying in 30s."
        ) from e


@mcp.tool
def list_curated() -> list[str]:
    """List the 7 curated AEMO NEM dataset IDs.

    These cover ~95% of typical NEM analytic queries: spot prices, regional
    demand and generation, interconnector flows, unit-level SCADA,
    rooftop PV (actual + forecast), 30-min predispatch forecasts, and
    daily-settled summaries.

    Example:
        ids = list_curated()
        # → ['daily_summary', 'dispatch_price', 'dispatch_region',
        #    'generation_scada', 'interconnector_flows',
        #    'predispatch_30min', 'rooftop_pv']

    Returns:
        Sorted list of dataset IDs. Always 7 entries today.
    """
    return curated.list_ids()


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
