"""Dataset search ranker tests."""
from __future__ import annotations

import pytest

from aemo_mcp import curated, feeds


@pytest.fixture(autouse=True)
def _reset_registry():
    curated.reset_registry()
    yield
    curated.reset_registry()


def test_list_datasets_returns_seven():
    out = feeds.list_datasets()
    assert len(out) == 7


def test_list_datasets_all_curated():
    out = feeds.list_datasets()
    assert all(s.is_curated for s in out)


def test_get_dataset_returns_summary():
    s = feeds.get_dataset("dispatch_price")
    assert s is not None
    assert s.cadence == "5 min"


def test_get_dataset_case_insensitive():
    assert feeds.get_dataset("DISPATCH_PRICE") is not None


def test_get_dataset_unknown_returns_none():
    assert feeds.get_dataset("nonsense") is None


def test_search_spot_price_returns_dispatch_price_first():
    out = feeds.search_datasets("spot price")
    assert out[0].id == "dispatch_price"


def test_search_dispatch_price_returns_dispatch_price_first():
    out = feeds.search_datasets("dispatch price")
    assert out[0].id == "dispatch_price"


def test_search_rooftop_returns_rooftop_pv_in_top_three():
    out = feeds.search_datasets("rooftop")
    assert "rooftop_pv" in [s.id for s in out[:3]]


def test_search_interconnector_top_result():
    out = feeds.search_datasets("interconnector flows")
    assert "interconnector_flows" in [s.id for s in out[:2]]


def test_search_predispatch_routes_correctly():
    out = feeds.search_datasets("predispatch")
    assert "predispatch_30min" in [s.id for s in out[:3]]


def test_search_daily_summary_routes_correctly():
    out = feeds.search_datasets("daily summary")
    assert "daily_summary" in [s.id for s in out[:2]]


def test_search_generation_routes_to_scada():
    out = feeds.search_datasets("generation by fuel")
    assert "generation_scada" in [s.id for s in out[:3]]


def test_search_eraring_returns_scada():
    """Search must hit known DUIDs in keywords."""
    out = feeds.search_datasets("eraring")
    assert "generation_scada" in [s.id for s in out[:3]]


def test_search_negative_pricing_returns_dispatch_price():
    out = feeds.search_datasets("negative pricing")
    assert "dispatch_price" in [s.id for s in out[:3]]


def test_search_empty_string_raises():
    with pytest.raises(ValueError, match="query is required"):
        feeds.search_datasets("")


def test_search_whitespace_only_raises():
    with pytest.raises(ValueError, match="query is required"):
        feeds.search_datasets("   ")


def test_search_limit_respected():
    out = feeds.search_datasets("price", limit=2)
    assert len(out) == 2


def test_search_limit_larger_than_dataset_count():
    out = feeds.search_datasets("price", limit=100)
    assert len(out) <= 7


def test_search_returns_datasetsummary_objects():
    out = feeds.search_datasets("price")
    assert all(hasattr(s, "id") and hasattr(s, "name") for s in out)


def test_search_phrase_bonus_helps_exact_substring():
    """Datasets whose haystack contains the exact phrase should outrank fuzzy."""
    # "negative pricing" appears verbatim in dispatch_price keywords
    out = feeds.search_datasets("negative pricing")
    top = out[0]
    assert top.id == "dispatch_price"


def test_search_qld_routes_to_dispatch_datasets():
    """Region values are folded into haystack — 'QLD' should find region-aware datasets."""
    out = feeds.search_datasets("QLD demand")
    top_ids = [s.id for s in out[:3]]
    # Either dispatch_region or daily_summary should surface
    assert any(s in top_ids for s in ("dispatch_region", "dispatch_price", "daily_summary"))
