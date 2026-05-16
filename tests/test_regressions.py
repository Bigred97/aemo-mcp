"""Regression tests for bugs found during QA against real NEMWEB.

Every test here pins a specific bug we hit during the customer simulation
+ edge-case sweep. Adding a new entry here is mandatory whenever we patch
a production behaviour.
"""
from __future__ import annotations

from pathlib import Path

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
            "https://www.nemweb.com.au/Reports/Archive/DispatchIS_Reports/"
        ).respond(200, text='<a href="x.zip">x.zip</a>')
        # Also mock the (wrong) URL — if the bug regresses, this would be hit
        wrong = respx.get(
            "https://www.nemweb.com.au/Reports/Archive/Current/DispatchIS_Reports/"
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
            "https://www.nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"
        ).respond(200, text='<a href="x">x</a>')
        forecast_listing = respx.get(
            "https://www.nemweb.com.au/Reports/Current/ROOFTOP_PV/FORECAST/"
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
        respx.get("https://www.nemweb.com.au/x.zip").respond(500)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            try:
                await client.fetch_zip("https://www.nemweb.com.au/x.zip")
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
            "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=listing)
        # First file rolled out → 403
        respx.get(
            "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150550_0000000456789010.zip"
        ).respond(403)
        # Second is fine — return an empty/invalid body so we exercise the
        # error-tolerance path rather than the full parse path.
        respx.get(
            "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150555_0000000456789012.zip"
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


# =============================================================================
# Bug: generation_scada stamped DUID into the region + fuel dimensions instead
# of joining against the bundled DUID master. Repro at 0.4.5:
#   latest(dataset_id='generation_scada') →
#       {'duid': 'BW01', 'region': 'BW01', 'fuel': 'BW01', 'metric': 'scada_mw'}
# Customers couldn't filter or group by region/fuel even though both were
# advertised in describe_dataset. Fix lives in shaping._resolve_dim_value +
# the `lookup:` field on the region/fuel filters in generation_scada.yaml.
# =============================================================================

def test_generation_scada_dims_resolve_region_and_fuel_not_duid():
    """Smoking gun: the bug stamped DUID into region/fuel. After the fix,
    region/fuel must be the looked-up values, not the DUID code.
    """
    from aemo_mcp.shaping import records_to_observations

    cd = curated.get("generation_scada")
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "DUID": "BW01", "SCADAVALUE": "600"},
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "DUID": "LY_W1", "SCADAVALUE": "520"},
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "DUID": "COOPGWF1", "SCADAVALUE": "180"},
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "DUID": "HPRG1", "SCADAVALUE": "90"},
    ]
    obs = records_to_observations(rows, cd)
    by_duid = {o.dimensions["duid"]: o.dimensions for o in obs}

    # Bayswater (NSW1 black coal) — the canonical sanity check
    assert by_duid["BW01"]["region"] == "NSW1"
    assert by_duid["BW01"]["fuel"] == "black_coal"
    # Loy Yang A (VIC1 brown coal) — unambiguous since brown coal is VIC-only
    assert by_duid["LY_W1"]["region"] == "VIC1"
    assert by_duid["LY_W1"]["fuel"] == "brown_coal"
    # Coopers Gap (QLD1 wind)
    assert by_duid["COOPGWF1"]["region"] == "QLD1"
    assert by_duid["COOPGWF1"]["fuel"] == "wind"
    # Hornsdale Power Reserve (SA1 battery)
    assert by_duid["HPRG1"]["region"] == "SA1"
    assert by_duid["HPRG1"]["fuel"] == "battery"

    # The bug had region/fuel == the DUID code — guard against regression.
    for dims in by_duid.values():
        assert dims["region"] != dims["duid"]
        assert dims["fuel"] != dims["duid"]


def test_generation_scada_unknown_duid_omits_region_and_fuel():
    """When a DUID isn't in the bundled snapshot (older snapshot / newer unit),
    we omit region/fuel rather than stamp the DUID code. Customer code that
    branches on `'region' in dims` then sees the truth instead of a misleading
    DUID echo.
    """
    from aemo_mcp.shaping import records_to_observations

    cd = curated.get("generation_scada")
    rows = [
        {
            "SETTLEMENTDATE": "2026/05/14 10:00:00",
            "DUID": "UNKNOWN_DUID_XYZ",
            "SCADAVALUE": "42",
        }
    ]
    obs = records_to_observations(rows, cd)
    assert len(obs) == 1
    dims = obs[0].dimensions
    assert dims["duid"] == "UNKNOWN_DUID_XYZ"
    # Region/fuel must NOT be the DUID code — either resolved or absent.
    assert dims.get("region") != "UNKNOWN_DUID_XYZ"
    assert dims.get("fuel") != "UNKNOWN_DUID_XYZ"


def test_generation_scada_describe_dataset_advertises_region_and_fuel_values():
    """The bug's secondary failure: describe_dataset claimed region/fuel were
    filterable, but the response dims used DUID for both. Verify the surface
    metadata stays consistent with the resolved dims.
    """
    cd = curated.get("generation_scada")
    detail = cd.to_detail()
    filters = {f.key: f for f in detail.filters}
    assert "region" in filters
    assert "fuel" in filters
    assert set(filters["region"].values) == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
    expected_fuels = {
        "black_coal", "brown_coal", "gas", "hydro",
        "wind", "solar", "battery", "biomass", "distillate",
    }
    assert set(filters["fuel"].values) >= expected_fuels


# =============================================================================
# Bug: invalid filter VALUES silently returned 0 records (no error).
# Repro at 0.4.6:
#   latest('dispatch_price', filters={'region': 'WA1'})    → 0 records, no error
#   latest('generation_scada', filters={'fuel': 'coal'})   → 0 records, no error
#   latest('interconnector_flows', filters={'interconnector': 'Basslink'}) → 0
# Agents read the empty list as "no data for this NEM region" rather than
# "wrong code". Fix lives in server._check_filter_values + _check_filter_keys.
# =============================================================================

import pytest
from aemo_mcp import server as _server


async def test_invalid_region_value_suggests_correction():
    """region='NSW' (missing the '1') used to return 0 records silently."""
    with pytest.raises(ValueError) as exc_info:
        await _server.latest("dispatch_price", filters={"region": "NSW"})
    msg = str(exc_info.value)
    assert "Did you mean 'NSW1'" in msg
    assert "NSW1" in msg and "QLD1" in msg  # full valid list inlined


async def test_invalid_fuel_value_suggests_correction():
    """fuel='coal' used to return 0 records silently; should suggest a near-match."""
    with pytest.raises(ValueError) as exc_info:
        await _server.latest("generation_scada", filters={"fuel": "coal"})
    msg = str(exc_info.value)
    # Either black_coal or brown_coal will be the closest match
    assert "Did you mean" in msg
    assert "coal" in msg
    assert "Valid fuel values" in msg


async def test_invalid_interconnector_value_lists_valid_options():
    """interconnector='Basslink' (common name) used to return 0 records silently."""
    with pytest.raises(ValueError) as exc_info:
        await _server.latest(
            "interconnector_flows", filters={"interconnector": "Basslink"}
        )
    msg = str(exc_info.value)
    # No close fuzzy match for 'Basslink', so no "Did you mean", but the valid
    # list must be inlined so the agent knows to use T-V-MNSP1.
    assert "T-V-MNSP1" in msg


async def test_invalid_filter_value_preserves_case_insensitive_match():
    """Lowercase region values must still work — the validation is value-
    correctness, not casing pedantry. 'nsw1' is canonically NSW1."""
    # Should NOT raise — case-insensitive equality with canonical NSW1
    resp = await _server.latest("dispatch_price", filters={"region": "nsw1"})
    assert resp is not None
    # And the dim is stamped with the canonical casing from the row, not the
    # user's casing — that's a separate guarantee but we sanity-check it here.


# =============================================================================
# Bug: stale=True with stale_reason=None was confusing.
# Repro at 0.4.6:
#   get_data(..., start_period='2099-01-01') → records: 0, stale: True,
#                                              stale_reason: None
# Fix lives in shaping.build_response — always populate stale_reason when
# stale is True (the cached-fallback path already set it; this covers the
# remaining branches: empty response + cadence-delay).
# =============================================================================

def test_stale_reason_set_for_empty_response():
    """Empty response with stale=True must carry an actionable reason."""
    from aemo_mcp.curated import get
    from aemo_mcp.shaping import build_response

    cd = get("dispatch_price")
    resp = build_response(
        dataset=cd,
        rows=[],
        sections_with_discriminator=None,
        fmt="records",
        user_query={"start_period": "2099-01-01"},
        source_url="http://x/",
        start_period="2099-01-01",
        end_period="2099-01-02",
    )
    assert resp.stale is True
    assert resp.stale_reason is not None
    # The reason should mention common causes the agent can act on.
    assert "future" in resp.stale_reason.lower() or "retention" in resp.stale_reason.lower()


def test_stale_reason_set_for_cadence_delay():
    """Records exist but are older than 2x cadence — stale_reason explains."""
    from aemo_mcp.curated import get
    from aemo_mcp.shaping import build_response

    cd = get("dispatch_price")
    rows = [
        {
            "SETTLEMENTDATE": "1990/01/01 00:00:00",
            "REGIONID": "NSW1",
            "RRP": "87.5",
        }
    ]
    resp = build_response(
        dataset=cd,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert resp.stale is True
    assert resp.stale_reason is not None
    assert "older" in resp.stale_reason or "delayed" in resp.stale_reason


# =============================================================================
# Bug: wide-window queries silently truncated at 31 days (the archive cap).
# Repro at 0.4.6:
#   get_data('dispatch_price', start='2026-01-01', end='2026-12-31')
#   → 44640 records spanning only Jan 1–Feb 1, with truncated_at=None,
#     stale_reason=None. Agent thought it got the whole year.
# Fix: when data range is narrower than user's requested end, set stale=True
# and explain in stale_reason so the agent knows to narrow the window.
# =============================================================================

def test_wide_window_truncation_surfaces_in_stale_reason():
    from aemo_mcp.curated import get
    from aemo_mcp.shaping import build_response

    cd = get("dispatch_price")
    # Data spans 2 days; user requested through end of year. The truncation
    # must show up in stale_reason so the agent can detect + narrow.
    rows = [
        {"SETTLEMENTDATE": "1990/01/01 00:00:00", "REGIONID": "NSW1", "RRP": "1"},
        {"SETTLEMENTDATE": "1990/01/02 00:00:00", "REGIONID": "NSW1", "RRP": "2"},
    ]
    resp = build_response(
        dataset=cd,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={"start_period": "1990-01-01", "end_period": "1990-12-31"},
        source_url="http://x/",
        start_period="1990-01-01",
        end_period="1990-12-31",
    )
    assert resp.stale is True
    assert resp.stale_reason is not None
    assert "1990-12-31" in resp.stale_reason  # mentions user's requested end
    assert "archive" in resp.stale_reason.lower() or "narrow" in resp.stale_reason.lower()


# =============================================================================
# Bug: error messages hard-coded "All 7 IDs" but portfolio is now 10.
# =============================================================================

async def test_unknown_dataset_error_uses_dynamic_count():
    """The 'All N IDs' count must match the curated registry, not be hard-coded."""
    from aemo_mcp import curated as _curated

    expected_count = len(_curated.list_ids())
    with pytest.raises(ValueError) as exc_info:
        await _server.describe_dataset("totally_not_a_dataset")
    msg = str(exc_info.value)
    assert f"All {expected_count} IDs" in msg
    # And the old hardcoded "7" must NOT appear (unless we happen to be at 7).
    if expected_count != 7:
        assert "All 7 IDs" not in msg
