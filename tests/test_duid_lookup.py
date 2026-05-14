"""DUID → region/fuel lookup tests."""
from __future__ import annotations

import pytest

from aemo_mcp import duid_lookup


@pytest.fixture(autouse=True)
def _reset():
    duid_lookup.reset_registry()
    yield
    duid_lookup.reset_registry()


def test_all_duids_nonempty():
    out = duid_lookup.all_duids()
    assert len(out) >= 50, f"expected at least 50 DUIDs in snapshot, got {len(out)}"


def test_all_regions_five():
    regions = duid_lookup.all_regions()
    assert set(regions) == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


def test_all_fuels_includes_major_buckets():
    fuels = set(duid_lookup.all_fuels())
    assert fuels >= {"black_coal", "brown_coal", "gas", "hydro", "wind", "solar", "battery"}


def test_lookup_bayswater_nsw_black_coal():
    out = duid_lookup.lookup_duids_for(region="NSW1", fuel="black_coal")
    assert "BW01" in out
    assert "BW04" in out
    assert "ER01" in out


def test_lookup_qld_wind():
    out = duid_lookup.lookup_duids_for(region="QLD1", fuel="wind")
    assert "COOPGWF1" in out


def test_lookup_sa_battery():
    out = duid_lookup.lookup_duids_for(region="SA1", fuel="battery")
    assert "HPRG1" in out
    assert "HPRL1" in out


def test_lookup_unknown_region_returns_empty():
    out = duid_lookup.lookup_duids_for(region="WA1")
    assert out == []


def test_lookup_unknown_fuel_returns_empty():
    out = duid_lookup.lookup_duids_for(fuel="nuclear")
    assert out == []


def test_lookup_no_filters_returns_all():
    out = duid_lookup.lookup_duids_for()
    assert len(out) >= 50


def test_lookup_only_region():
    out = duid_lookup.lookup_duids_for(region="VIC1")
    assert "LY_W1" in out
    assert "LOYYB1" in out
    assert "YWPS1" in out
    assert "BW01" not in out  # NSW1 unit


def test_lookup_only_fuel():
    out = duid_lookup.lookup_duids_for(fuel="brown_coal")
    # Brown coal is VIC-only
    info = {d: duid_lookup.duid_info(d) for d in out}
    assert all(i["region"] == "VIC1" for i in info.values() if i)


def test_lookup_region_case_insensitive():
    out_upper = duid_lookup.lookup_duids_for(region="NSW1")
    out_lower = duid_lookup.lookup_duids_for(region="nsw1")
    assert set(out_upper) == set(out_lower)


def test_lookup_fuel_case_insensitive():
    out_lower = duid_lookup.lookup_duids_for(fuel="wind")
    out_upper = duid_lookup.lookup_duids_for(fuel="WIND")
    assert set(out_lower) == set(out_upper)


def test_duid_info_returns_record():
    info = duid_lookup.duid_info("BW01")
    assert info is not None
    assert info["region"] == "NSW1"
    assert info["fuel"] == "black_coal"
    assert info["station"] == "Bayswater"


def test_duid_info_unknown_returns_none():
    assert duid_lookup.duid_info("FAKE_DUID_XYZ") is None


def test_aggregate_by_region():
    rows = [
        {"DUID": "BW01", "SCADAVALUE": "600"},
        {"DUID": "ER01", "SCADAVALUE": "650"},
        {"DUID": "LY_W1", "SCADAVALUE": "520"},
        {"DUID": "TORRA1", "SCADAVALUE": "100"},
    ]
    out = duid_lookup.aggregate_by(rows, "region")
    assert "NSW1" in out
    assert "VIC1" in out
    assert "SA1" in out
    assert set(out["NSW1"]) == {"BW01", "ER01"}


def test_aggregate_by_fuel():
    rows = [
        {"DUID": "BW01"},
        {"DUID": "LY_W1"},
        {"DUID": "COOPGWF1"},
        {"DUID": "HPRG1"},
    ]
    out = duid_lookup.aggregate_by(rows, "fuel")
    assert "black_coal" in out
    assert "brown_coal" in out
    assert "wind" in out
    assert "battery" in out


def test_aggregate_unknown_dimension_raises():
    with pytest.raises(ValueError, match="Unsupported aggregation dimension"):
        duid_lookup.aggregate_by([], "nonsense")


def test_aggregate_ignores_unknown_duids():
    rows = [{"DUID": "FAKE_XYZ"}, {"DUID": "BW01"}]
    out = duid_lookup.aggregate_by(rows, "region")
    assert "NSW1" in out
    # No bucket for unknown DUID
    flat = [d for v in out.values() for d in v]
    assert "FAKE_XYZ" not in flat
