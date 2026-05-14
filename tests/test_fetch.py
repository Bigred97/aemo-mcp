"""Fetch orchestration tests — most use respx to mock NEMWEB."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
import respx

from aemo_mcp.cache import Cache
from aemo_mcp.client import AEMOClient
from aemo_mcp.curated import get as get_curated
from aemo_mcp.fetch import (
    FetchError,
    _extract_filename_timestamp,
    _filenames_in_window,
    _parse_period,
    _resolve_duid_filter,
    fetch_dataset,
)
from aemo_mcp.shaping import NEM_TZ


def test_extract_filename_timestamp_dispatchis():
    ts = _extract_filename_timestamp(
        "PUBLIC_DISPATCHIS_202605141005_0000000456789012.zip"
    )
    assert ts is not None
    assert ts.year == 2026
    assert ts.month == 5
    assert ts.day == 14
    assert ts.hour == 10
    assert ts.minute == 5


def test_extract_filename_timestamp_rooftop():
    ts = _extract_filename_timestamp(
        "PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20260514093000_0000000456789012.zip"
    )
    assert ts is not None
    assert ts.year == 2026


def test_extract_filename_timestamp_no_match_returns_none():
    assert _extract_filename_timestamp("garbage.zip") is None


def test_filenames_in_window_no_bounds_returns_all():
    files = [
        "PUBLIC_DISPATCHIS_202605141000_X.zip",
        "PUBLIC_DISPATCHIS_202605141005_X.zip",
    ]
    out = _filenames_in_window(files, None, None)
    assert out == files


def test_filenames_in_window_start_only():
    files = [
        "PUBLIC_DISPATCHIS_202605141000_X.zip",
        "PUBLIC_DISPATCHIS_202605141005_X.zip",
        "PUBLIC_DISPATCHIS_202605141010_X.zip",
    ]
    start = datetime(2026, 5, 14, 10, 5, tzinfo=NEM_TZ)
    out = _filenames_in_window(files, start, None)
    assert len(out) == 2
    assert all("202605141005" in f or "202605141010" in f for f in out)


def test_filenames_in_window_end_only():
    files = [
        "PUBLIC_DISPATCHIS_202605141000_X.zip",
        "PUBLIC_DISPATCHIS_202605141005_X.zip",
        "PUBLIC_DISPATCHIS_202605141010_X.zip",
    ]
    end = datetime(2026, 5, 14, 10, 5, tzinfo=NEM_TZ)
    out = _filenames_in_window(files, None, end)
    assert len(out) == 2
    assert all("202605141010" not in f for f in out)


def test_filenames_in_window_both_bounds():
    files = [
        "PUBLIC_DISPATCHIS_202605140955_X.zip",
        "PUBLIC_DISPATCHIS_202605141000_X.zip",
        "PUBLIC_DISPATCHIS_202605141005_X.zip",
        "PUBLIC_DISPATCHIS_202605141010_X.zip",
    ]
    start = datetime(2026, 5, 14, 10, 0, tzinfo=NEM_TZ)
    end = datetime(2026, 5, 14, 10, 5, tzinfo=NEM_TZ)
    out = _filenames_in_window(files, start, end)
    assert len(out) == 2


def test_parse_period_year_only():
    dt = _parse_period("2026")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 1
    assert dt.day == 1


def test_parse_period_iso_date():
    dt = _parse_period("2026-05-14")
    assert dt is not None
    assert dt.day == 14


def test_parse_period_with_time():
    dt = _parse_period("2026-05-14 10:30")
    assert dt is not None
    assert dt.hour == 10
    assert dt.minute == 30


def test_parse_period_invalid_raises():
    with pytest.raises(FetchError, match="Could not parse"):
        _parse_period("not a date")


def test_parse_period_none_returns_none():
    assert _parse_period(None) is None
    assert _parse_period("") is None


def test_resolve_duid_filter_no_filters():
    assert _resolve_duid_filter("generation_scada", None) is None
    assert _resolve_duid_filter("generation_scada", {}) is None


def test_resolve_duid_filter_wrong_dataset_returns_none():
    assert _resolve_duid_filter("dispatch_price", {"region": "NSW1"}) is None


def test_resolve_duid_filter_by_region():
    allow = _resolve_duid_filter("generation_scada", {"region": "NSW1"})
    assert allow is not None
    assert "BW01" in allow
    assert "LY_W1" not in allow  # VIC


def test_resolve_duid_filter_by_fuel():
    allow = _resolve_duid_filter("generation_scada", {"fuel": "wind"})
    assert allow is not None
    assert "COOPGWF1" in allow
    assert "BW01" not in allow


def test_resolve_duid_filter_intersection():
    allow = _resolve_duid_filter(
        "generation_scada", {"region": "VIC1", "fuel": "brown_coal"}
    )
    assert allow is not None
    assert "LY_W1" in allow
    assert "BW01" not in allow  # NSW black coal
    assert "MURRAY" not in allow  # VIC hydro


def test_resolve_duid_filter_explicit_duid_intersected():
    allow = _resolve_duid_filter(
        "generation_scada", {"region": "NSW1", "duid": "BW01"}
    )
    assert allow == {"BW01"}


def test_resolve_duid_filter_unknown_region_empty():
    allow = _resolve_duid_filter("generation_scada", {"region": "WA1"})
    assert allow == set()


@respx.mock
async def test_fetch_dispatch_price_e2e(
    tmp_path: Path, dispatchis_listing_html, dispatch_is_zip
):
    """Full path: listing → zip → parse → filter → response."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_is_zip)

        cd = get_curated("dispatch_price")
        resp = await fetch_dataset(
            client, cd, {"region": "NSW1"}, None, None, "records", only_latest=True
        )
        assert resp.dataset_id == "dispatch_price"
        assert len(resp.records) == 1
        assert resp.records[0].dimensions["region"] == "NSW1"  # type: ignore
        assert resp.records[0].value == 87.5  # type: ignore
        assert resp.unit == "$/MWh"
        assert resp.source == "Australian Energy Market Operator"
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_dispatch_price_all_regions(
    tmp_path: Path, dispatchis_listing_html, dispatch_is_zip
):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_is_zip)

        cd = get_curated("dispatch_price")
        resp = await fetch_dataset(client, cd, None, None, None, "records", True)
        regions = {o.dimensions["region"] for o in resp.records}  # type: ignore
        assert regions == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_dispatch_region_metrics(
    tmp_path: Path, dispatchis_listing_html, dispatch_is_zip
):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_is_zip)

        cd = get_curated("dispatch_region")
        resp = await fetch_dataset(
            client, cd, {"region": "NSW1"}, None, None, "records", True
        )
        metrics = {o.dimensions["metric"] for o in resp.records}  # type: ignore
        assert "total_demand" in metrics
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_interconnector_e2e(
    tmp_path: Path, dispatchis_listing_html, dispatch_is_zip
):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_is_zip)

        cd = get_curated("interconnector_flows")
        resp = await fetch_dataset(
            client, cd, {"interconnector": "V-SA"}, None, None, "records", True
        )
        assert all(
            o.dimensions["interconnector"] == "V-SA" for o in resp.records  # type: ignore
        )
        flow = next(
            o for o in resp.records if o.dimensions["metric"] == "mw_flow"  # type: ignore
        )
        assert flow.value == 250.0  # type: ignore
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_generation_scada_filter_by_region(
    tmp_path: Path, scada_listing_html, dispatch_scada_zip
):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
        ).respond(200, text=scada_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_scada_zip)

        cd = get_curated("generation_scada")
        resp = await fetch_dataset(
            client, cd, {"region": "NSW1"}, None, None, "records", True
        )
        duids = {o.dimensions["duid"] for o in resp.records}  # type: ignore
        # Only NSW DUIDs should remain (BW01, BW02, ER01)
        assert "BW01" in duids
        assert "ER01" in duids
        assert "LY_W1" not in duids  # VIC
        assert "COOPGWF1" not in duids  # QLD
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_generation_scada_filter_by_fuel(
    tmp_path: Path, scada_listing_html, dispatch_scada_zip
):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
        ).respond(200, text=scada_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_scada_zip)

        cd = get_curated("generation_scada")
        resp = await fetch_dataset(
            client, cd, {"fuel": "wind"}, None, None, "records", True
        )
        duids = {o.dimensions["duid"] for o in resp.records}  # type: ignore
        assert "COOPGWF1" in duids
        assert "BW01" not in duids
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_listing_404_raises(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(404)
        cd = get_curated("dispatch_price")
        with pytest.raises(FetchError, match="Could not list"):
            await fetch_dataset(client, cd, None, None, None, "records", True)
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_with_period_window(
    tmp_path: Path, dispatchis_listing_html, dispatch_is_zip
):
    """Calling with start_period should fetch matching windowed files."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        ).respond(200, content=dispatch_is_zip)

        cd = get_curated("dispatch_price")
        resp = await fetch_dataset(
            client, cd, {"region": "NSW1"},
            "2026-05-14 10:00", "2026-05-14 10:00",
            "records", only_latest=False
        )
        assert len(resp.records) >= 1
        # interval_start/end echo back the requested range
        assert resp.interval_start == "2026-05-14 10:00"
    finally:
        await client.aclose()


def test_parse_period_invalid_raises_fetch_error():
    with pytest.raises(FetchError):
        _parse_period("totally garbage")
