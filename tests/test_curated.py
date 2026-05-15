"""Curated dataset YAML loading tests."""
from __future__ import annotations

import pytest

from aemo_mcp import curated


@pytest.fixture(autouse=True)
def _reset_registry():
    curated.reset_registry()
    yield
    curated.reset_registry()


def test_list_ids_returns_nine_datasets():
    ids = curated.list_ids()
    assert len(ids) == 9
    assert ids == sorted(ids)


def test_list_ids_specific_set():
    assert set(curated.list_ids()) == {
        "daily_summary",
        "dispatch_price",
        "dispatch_region",
        "fcas_prices",
        "generation_scada",
        "interconnector_flows",
        "predispatch_30min",
        "rooftop_pv",
        "trading_price",
    }


def test_get_dispatch_price():
    cd = curated.get("dispatch_price")
    assert cd is not None
    assert cd.id == "dispatch_price"
    assert cd.name.startswith("NEM Dispatch Price")
    assert cd.cadence == "5 min"


def test_get_case_insensitive():
    assert curated.get("DISPATCH_PRICE") is not None
    assert curated.get("Dispatch_Price") is not None
    assert curated.get("  dispatch_price  ") is not None


def test_get_unknown_returns_none():
    assert curated.get("nonsense") is None
    assert curated.get("") is None


def test_dispatch_price_has_one_folder():
    cd = curated.get("dispatch_price")
    assert len(cd.folders) == 1
    assert cd.folders[0].path == "/Reports/Current/DispatchIS_Reports/"


def test_dispatch_price_has_region_filter():
    cd = curated.get("dispatch_price")
    f = cd.get_filter("region")
    assert f is not None
    assert f.values == ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")
    assert f.column == "REGIONID"


def test_dispatch_price_has_rrp_metric():
    cd = curated.get("dispatch_price")
    m = cd.get_metric("rrp")
    assert m is not None
    assert m.source_column == "RRP"
    assert m.unit == "$/MWh"


def test_dispatch_region_has_four_metrics():
    cd = curated.get("dispatch_region")
    metric_keys = [m.key for m in cd.metrics]
    assert set(metric_keys) >= {
        "total_demand",
        "available_generation",
        "net_interchange",
    }


def test_interconnector_flows_has_six_interconnectors():
    cd = curated.get("interconnector_flows")
    f = cd.get_filter("interconnector")
    assert f is not None
    assert len(f.values) == 6
    assert "V-SA" in f.values
    assert "T-V-MNSP1" in f.values


def test_generation_scada_has_three_filters():
    cd = curated.get("generation_scada")
    keys = {f.key for f in cd.filters}
    assert keys == {"duid", "region", "fuel"}


def test_generation_scada_fuel_values():
    cd = curated.get("generation_scada")
    f = cd.get_filter("fuel")
    assert "black_coal" in f.values
    assert "wind" in f.values
    assert "battery" in f.values


def test_rooftop_pv_has_two_folders():
    cd = curated.get("rooftop_pv")
    assert len(cd.folders) == 2
    paths = {f.path for f in cd.folders}
    assert "/Reports/Current/ROOFTOP_PV/ACTUAL/" in paths
    assert "/Reports/Current/ROOFTOP_PV/FORECAST/" in paths


def test_rooftop_pv_has_section_discriminator():
    cd = curated.get("rooftop_pv")
    disc = {f.discriminator for f in cd.folders}
    assert disc == {"actual", "forecast"}


def test_predispatch_uses_datetime_column():
    cd = curated.get("predispatch_30min")
    assert cd.settlement_column == "DATETIME"


def test_daily_summary_cadence_daily():
    cd = curated.get("daily_summary")
    assert cd.cadence == "Daily"
    assert cd.cache_kind == "daily"


def test_to_detail_round_trip():
    cd = curated.get("dispatch_price")
    detail = cd.to_detail()
    assert detail.id == "dispatch_price"
    assert detail.is_curated is True
    assert "region" in {f.key for f in detail.filters}
    assert detail.units == {"rrp": "$/MWh"}


def test_to_detail_filters_include_values():
    cd = curated.get("dispatch_price")
    detail = cd.to_detail()
    region_f = next(f for f in detail.filters if f.key == "region")
    assert region_f.values == ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]


def test_to_detail_examples_present():
    cd = curated.get("dispatch_price")
    detail = cd.to_detail()
    assert len(detail.examples) >= 2
    assert any("NSW1" in ex for ex in detail.examples)


def test_all_datasets_have_at_least_one_metric():
    for ds_id in curated.list_ids():
        cd = curated.get(ds_id)
        assert len(cd.metrics) >= 1, f"{ds_id} has no metrics"


def test_all_datasets_have_search_keywords():
    for ds_id in curated.list_ids():
        cd = curated.get(ds_id)
        assert len(cd.search_keywords) >= 3, f"{ds_id} has too few search keywords"


def test_all_datasets_have_examples():
    for ds_id in curated.list_ids():
        cd = curated.get(ds_id)
        assert len(cd.examples) >= 1, f"{ds_id} has no examples"


def test_all_filter_columns_are_uppercase():
    """AEMO CSV columns are always uppercase. Filter `column` overrides too."""
    for ds_id in curated.list_ids():
        cd = curated.get(ds_id)
        for f in cd.filters:
            col = f.column or f.key.upper()
            assert col == col.upper(), f"{ds_id}.{f.key}: {col} not uppercase"


def test_cache_kind_valid_for_every_dataset():
    valid = {"live", "half_hour", "forecast", "daily", "archive", "listing"}
    for ds_id in curated.list_ids():
        cd = curated.get(ds_id)
        assert cd.cache_kind in valid, f"{ds_id}: invalid cache_kind {cd.cache_kind!r}"


def test_compile_filename_regex_matches_dispatch_filename():
    from aemo_mcp.curated import compile_filename_regex
    cd = curated.get("dispatch_price")
    rx = compile_filename_regex(cd.folders[0])
    assert rx.fullmatch("PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip") is not None


def test_compile_filename_regex_rejects_wrong_pattern():
    from aemo_mcp.curated import compile_filename_regex
    cd = curated.get("dispatch_price")
    rx = compile_filename_regex(cd.folders[0])
    assert rx.fullmatch("PUBLIC_DISPATCHSCADA_202605141000_0000000456789012.zip") is None
    assert rx.fullmatch("nope.zip") is None
