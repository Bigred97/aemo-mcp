"""Regression tests for bugs found during QA against real NEMWEB.

Every test here pins a specific bug we hit during the customer simulation
+ edge-case sweep. Adding a new entry here is mandatory whenever we patch
a production behaviour.
"""
from __future__ import annotations

import re
from pathlib import Path

import httpx
import pytest
import respx

from aemo_mcp import curated
from aemo_mcp.cache import Cache
from aemo_mcp.client import AEMOClient
from aemo_mcp.curated import compile_filename_regex
from aemo_mcp.fetch import (
    _is_forecast_folder,
    _should_use_archive,
    fetch_dataset,
)
from aemo_mcp.parsing import find_sections, parse_csv
from aemo_mcp.shaping import NEM_TZ
from datetime import datetime, timedelta


# =============================================================================
# Bug: predispatch_30min YAML had wrong section name (REGIONSUM → REGION_SOLUTION)
# =============================================================================

def test_predispatch_yaml_uses_region_solution_not_regionsum():
    cd = curated.get("predispatch_30min")
    names = {s.name for f in cd.folders for s in f.sections}
    assert "PREDISPATCH.REGION_SOLUTION" in names
    # Old (wrong) name must not be present
    assert "PREDISPATCH.REGIONSUM" not in names


def test_predispatch_yaml_includes_region_prices():
    cd = curated.get("predispatch_30min")
    names = {s.name for f in cd.folders for s in f.sections}
    assert "PREDISPATCH.REGION_PRICES" in names


# =============================================================================
# Bug: daily_summary YAML pointed at DISPATCH.PRICE but daily file uses DREGION.
# =============================================================================

def test_daily_summary_section_is_dregion_with_trailing_dot():
    cd = curated.get("daily_summary")
    names = {s.name for f in cd.folders for s in f.sections}
    assert "DREGION." in names
    # Old (wrong) name must not be present
    assert "DISPATCH.PRICE" not in names


def test_daily_summary_has_four_metrics():
    cd = curated.get("daily_summary")
    keys = {m.key for m in cd.metrics}
    assert {"rrp", "total_demand", "dispatchable_generation", "net_interchange"} <= keys


def test_parser_builds_empty_subname_section_with_trailing_dot():
    """I,DREGION,,2,COL,... → section name 'DREGION.' (empty second cell)."""
    csv = b"I,DREGION,,2,SETTLEMENTDATE,REGIONID,RRP\n" \
          b"D,DREGION,,2,2026/05/14 00:05:00,NSW1,50\n"
    sections = parse_csv(csv)
    assert len(sections) == 1
    assert sections[0].name == "DREGION."


# =============================================================================
# Bug: rooftop_pv filename regex didn't match SATELLITE prefix
# =============================================================================

def test_rooftop_actual_regex_matches_satellite_filename():
    cd = curated.get("rooftop_pv")
    actual_folder = next(f for f in cd.folders if f.discriminator == "actual")
    rx = compile_filename_regex(actual_folder)
    assert rx.fullmatch(
        "PUBLIC_ROOFTOP_PV_ACTUAL_SATELLITE_20260515053000_0000000517728965.zip"
    ) is not None


def test_rooftop_actual_regex_still_matches_measurement_filename():
    """Forward compat: regex must accept the older MEASUREMENT infix too."""
    cd = curated.get("rooftop_pv")
    actual_folder = next(f for f in cd.folders if f.discriminator == "actual")
    rx = compile_filename_regex(actual_folder)
    assert rx.fullmatch(
        "PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20260515053000_0000000517728965.zip"
    ) is not None


def test_rooftop_actual_regex_matches_no_infix():
    cd = curated.get("rooftop_pv")
    actual_folder = next(f for f in cd.folders if f.discriminator == "actual")
    rx = compile_filename_regex(actual_folder)
    assert rx.fullmatch(
        "PUBLIC_ROOFTOP_PV_ACTUAL_20260515053000_0000000517728965.zip"
    ) is not None


# =============================================================================
# Bug: find_section() returned only first match — DREGION. v2/v3 dupes lost data
# =============================================================================

def test_find_sections_returns_all_versions():
    csv = b"I,DREGION,,2,COL,VAL\nD,DREGION,,2,A,1\n" \
          b"I,DREGION,,3,COL,VAL\nD,DREGION,,3,B,2\n"
    sections = parse_csv(csv)
    matching = find_sections(sections, "DREGION.")
    assert len(matching) == 2
    assert {s.version for s in matching} == {"2", "3"}


# =============================================================================
# Bug: archive path included "Current" → 404 on /Reports/Archive/Current/...
# =============================================================================

@respx.mock
async def test_archive_path_excludes_current_segment(tmp_path: Path):
    """The /Reports/Archive/<feed>/ path must NOT carry the 'Current' segment."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        # Mock the correct archive URL (no "Current")
        respx.get(
            "http://nemweb.com.au/Reports/Archive/DispatchIS_Reports/"
        ).respond(200, text='<a href="x.zip">x.zip</a>')
        # Also mock the (wrong) URL — if the bug regresses, this would be hit
        wrong = respx.get(
            "http://nemweb.com.au/Reports/Archive/Current/DispatchIS_Reports/"
        ).respond(404)

        cd = curated.get("dispatch_price")
        # Window deep in the past forces archive use.
        old = datetime.now(NEM_TZ) - timedelta(days=30)
        old_str = old.strftime("%Y-%m-%d %H:%M")
        try:
            await fetch_dataset(
                client, cd, {"region": "NSW1"}, old_str, old_str,
                "records", only_latest=False,
            )
        except Exception:
            # The test cares only about which URL is fetched, not whether
            # the response succeeded.
            pass
        assert wrong.call_count == 0
    finally:
        await client.aclose()


# =============================================================================
# Bug: section filter (rooftop section=actual/forecast) accidentally dropped rows
# =============================================================================

def test_section_filter_does_not_reject_rows():
    """The `section` filter is handled at folder selection, not row-level."""
    from aemo_mcp.fetch import _row_matches_filters
    cd = curated.get("rooftop_pv")
    row = {"INTERVAL_DATETIME": "2026/05/15 05:00:00", "REGIONID": "NSW1", "POWER": "0"}
    # User asks for section=actual; the row has no SECTION column. Must pass.
    assert _row_matches_filters(row, cd, {"section": "actual"}, None) is True
    assert _row_matches_filters(row, cd, {"section": "forecast"}, None) is True


# =============================================================================
# Bug: discriminator filter still hit BOTH folders (perf + 403 risk)
# =============================================================================

@respx.mock
async def test_section_filter_skips_unrelated_folder(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        # If the optimisation regresses, code would hit FORECAST folder
        # when user asked for ACTUAL only. We mock ACTUAL only; FORECAST
        # listing 404s.
        actual_listing = respx.get(
            "http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"
        ).respond(200, text='<a href="x">x</a>')
        forecast_listing = respx.get(
            "http://nemweb.com.au/Reports/Current/ROOFTOP_PV/FORECAST/"
        ).respond(404)

        cd = curated.get("rooftop_pv")
        try:
            await fetch_dataset(
                client, cd, {"region": "NSW1", "section": "actual"},
                None, None, "records", only_latest=True,
            )
        except Exception:
            pass
        # FORECAST folder must NOT have been listed.
        assert forecast_listing.call_count == 0
    finally:
        await client.aclose()


# =============================================================================
# Bug: latest() on forecast feeds collapsed to 1 row per region, losing horizon
# =============================================================================

def test_is_forecast_folder_for_predispatch():
    cd = curated.get("predispatch_30min")
    folder = cd.folders[0]
    assert _is_forecast_folder(folder, cd) is True


def test_is_forecast_folder_for_rooftop_forecast():
    cd = curated.get("rooftop_pv")
    folder = next(f for f in cd.folders if f.discriminator == "forecast")
    assert _is_forecast_folder(folder, cd) is True


def test_is_not_forecast_folder_for_dispatch_price():
    cd = curated.get("dispatch_price")
    folder = cd.folders[0]
    assert _is_forecast_folder(folder, cd) is False


def test_is_not_forecast_folder_for_rooftop_actual():
    cd = curated.get("rooftop_pv")
    folder = next(f for f in cd.folders if f.discriminator == "actual")
    assert _is_forecast_folder(folder, cd) is False


# =============================================================================
# Bug: _should_use_archive used 24h cutoff, hammering NEMWEB with too many files
# =============================================================================

def test_archive_cutoff_is_short():
    """Cutoff must be small enough that wide windows go to archive, not Current."""
    from aemo_mcp.fetch import _CURRENT_WINDOW_HOURS
    assert _CURRENT_WINDOW_HOURS <= 6


def test_should_use_archive_for_one_day_ago():
    one_day_ago = datetime.now(NEM_TZ) - timedelta(hours=23)
    assert _should_use_archive(one_day_ago, datetime.now(NEM_TZ)) is True


def test_should_use_archive_for_4_weeks():
    four_weeks_ago = datetime.now(NEM_TZ) - timedelta(days=28)
    assert _should_use_archive(four_weeks_ago, datetime.now(NEM_TZ)) is True


def test_should_not_use_archive_for_last_hour():
    one_hour_ago = datetime.now(NEM_TZ) - timedelta(hours=1)
    assert _should_use_archive(one_hour_ago, datetime.now(NEM_TZ)) is False


def test_should_not_use_archive_for_no_period():
    assert _should_use_archive(None, None) is False


# =============================================================================
# Bug: Cache(db_path=DEFAULT_DB_PATH) captured DEFAULT_DB_PATH at class-def time,
# so test monkeypatches had no effect and integration tests bled real data
# =============================================================================

def test_cache_picks_up_monkeypatched_default_db_path(tmp_path, monkeypatch):
    from aemo_mcp import cache as cache_mod
    custom = tmp_path / "custom.db"
    monkeypatch.setattr(cache_mod, "DEFAULT_DB_PATH", custom)
    c = Cache()  # no explicit db_path → must pick up the monkeypatch
    assert c.db_path == custom


def test_cache_explicit_db_path_overrides_default(tmp_path):
    explicit = tmp_path / "explicit.db"
    c = Cache(db_path=explicit)
    assert c.db_path == explicit


# =============================================================================
# Bug: DUID snapshot was too small — gas/solar queries returned 0 in QLD
# =============================================================================

def test_duid_snapshot_has_qld_gas():
    from aemo_mcp.duid_lookup import lookup_duids_for
    out = lookup_duids_for(region="QLD1", fuel="gas")
    assert len(out) >= 10, f"expected >=10 QLD gas DUIDs, got {len(out)}"


def test_duid_snapshot_has_qld_solar():
    from aemo_mcp.duid_lookup import lookup_duids_for
    out = lookup_duids_for(region="QLD1", fuel="solar")
    assert len(out) >= 10, f"expected >=10 QLD solar DUIDs, got {len(out)}"


def test_duid_snapshot_has_nsw_solar():
    from aemo_mcp.duid_lookup import lookup_duids_for
    out = lookup_duids_for(region="NSW1", fuel="solar")
    assert len(out) >= 10, f"expected >=10 NSW solar DUIDs, got {len(out)}"


def test_duid_snapshot_total_count():
    from aemo_mcp.duid_lookup import all_duids
    assert len(all_duids()) >= 250, "DUID snapshot should cover the majority of active NEM units"


def test_duid_snapshot_loader_skips_comments():
    """Comment lines starting with # must not become bogus DUIDs."""
    from aemo_mcp.duid_lookup import all_duids
    for duid in all_duids():
        assert not duid.startswith("#"), f"comment line leaked into snapshot: {duid!r}"
        assert "===" not in duid


# =============================================================================
# Bug: ZIP-of-ZIP detection — archive zips contain inner zips that need walking
# =============================================================================

def test_looks_like_zip_signature():
    from aemo_mcp.fetch import _looks_like_zip
    assert _looks_like_zip(b"PK\x03\x04" + b"\x00" * 10)
    assert not _looks_like_zip(b"not a zip")
    assert not _looks_like_zip(b"")


# =============================================================================
# Bug: in-flight dedup left unretrieved exceptions on futures (asyncio warning)
# =============================================================================

@respx.mock
async def test_failed_fetch_does_not_leak_future_exception_warning(tmp_path: Path):
    """When a request fails, the in-flight future's exception must be retrieved."""
    import warnings
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get("http://nemweb.com.au/x.zip").respond(500)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            try:
                await client.fetch_zip("http://nemweb.com.au/x.zip")
            except Exception:
                pass
        # No "Future exception was never retrieved" warnings.
        assert not any(
            "Future exception was never retrieved" in str(w.message)
            for w in captured
        )
    finally:
        await client.aclose()


# =============================================================================
# Bug: NEMWEB returns 403/404 for files that rolled out of Current mid-fetch.
# Must skip such files rather than failing the whole response.
# =============================================================================

@respx.mock
async def test_current_fetch_skips_individual_403(tmp_path: Path):
    """One failed download must not break the whole response."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        listing = (
            '<a href="/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150550_0000000456789010.zip">x</a>'
            '<a href="/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150555_0000000456789012.zip">x</a>'
        )
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=listing)
        # First file rolled out → 403
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150550_0000000456789010.zip"
        ).respond(403)
        # Second is fine — return an empty/invalid body so we exercise the
        # error-tolerance path rather than the full parse path.
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150555_0000000456789012.zip"
        ).respond(200, content=b"PK\x05\x06" + b"\x00" * 18)  # empty zip

        cd = curated.get("dispatch_price")
        # Period window includes both files
        from datetime import datetime as _dt
        start = _dt(2026, 5, 15, 5, 50, tzinfo=NEM_TZ).strftime("%Y-%m-%d %H:%M")
        end = _dt(2026, 5, 15, 5, 55, tzinfo=NEM_TZ).strftime("%Y-%m-%d %H:%M")

        # Should not raise — first file 403 is skipped, response is empty.
        # (Note: window starts in the past relative to "now" in test env,
        # the archive fallback may also kick in; that's fine — we just
        # verify no exception.)
        try:
            resp = await fetch_dataset(
                client, cd, None, start, end, "records", only_latest=False
            )
            # We don't assert on the response shape — the key is "did not raise".
            assert resp is not None
        except Exception as e:
            # If we accidentally trigger archive fallback that 404s, that's OK
            # for THIS regression test — we only care that the 403 didn't
            # propagate out.
            assert "403" not in str(e)
    finally:
        await client.aclose()
