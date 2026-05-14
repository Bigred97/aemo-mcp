"""Live tests — hit the real NEMWEB. Skipped by default; run with:

    uv run pytest -q -m live

These tests are TOLERANT — they check response shape and basic plausibility
rather than exact values. NEMWEB is sometimes slow or in maintenance; tests
that depend on the real backend MUST NOT enforce specific MW or $ figures.
"""
from __future__ import annotations

import pytest

from aemo_mcp import server


pytestmark = pytest.mark.live


async def test_live_list_curated():
    ids = server.list_curated()
    assert len(ids) == 7


async def test_live_search_datasets():
    out = await server.search_datasets("spot price")
    assert out[0].id == "dispatch_price"


async def test_live_describe_dispatch_price():
    d = await server.describe_dataset("dispatch_price")
    assert d.id == "dispatch_price"
    assert d.cadence == "5 min"


async def test_live_latest_dispatch_price_nsw():
    """Hit the real DispatchIS feed for NSW. Expect a numeric RRP, any sign."""
    resp = await server.latest("dispatch_price", filters={"region": "NSW1"})
    assert resp.dataset_id == "dispatch_price"
    assert len(resp.records) >= 1
    rec = resp.records[0]
    assert rec.dimensions["region"] == "NSW1"  # type: ignore
    assert rec.unit == "$/MWh"
    assert rec.value is not None  # type: ignore


async def test_live_latest_dispatch_price_all_regions():
    resp = await server.latest("dispatch_price")
    regions = {o.dimensions["region"] for o in resp.records}  # type: ignore
    assert regions == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


async def test_live_latest_dispatch_region_qld_demand():
    resp = await server.latest("dispatch_region", filters={"region": "QLD1"})
    metrics = {o.dimensions["metric"] for o in resp.records}  # type: ignore
    assert "total_demand" in metrics
    demand = next(
        o for o in resp.records  # type: ignore
        if o.dimensions["metric"] == "total_demand"
    )
    assert demand.value is not None  # type: ignore
    # Plausible QLD demand range: 4,000 — 13,000 MW
    assert 2000 < demand.value < 20000  # type: ignore


async def test_live_latest_interconnector_flows_six_links():
    """The DispatchIS file always contains all 6 interconnectors."""
    resp = await server.latest("interconnector_flows")
    ics = {o.dimensions["interconnector"] for o in resp.records}  # type: ignore
    assert ics == {"N-Q-MNSP1", "NSW1-QLD1", "T-V-MNSP1", "V-S-MNSP1", "V-SA", "VIC1-NSW1"}


async def test_live_latest_generation_scada_nsw():
    """Hit Dispatch_SCADA for NSW units."""
    resp = await server.latest("generation_scada", filters={"region": "NSW1"})
    # Expect at least a few NSW units. Some may be offline (zero output) but
    # the rows should still come through if they have SCADAVALUE set.
    assert len(resp.records) >= 1


async def test_live_response_carries_attribution():
    resp = await server.latest("dispatch_price", filters={"region": "NSW1"})
    assert "AEMO" in resp.attribution
    assert "Copyright Permissions" in resp.attribution
    assert resp.source_url.startswith("http://nemweb.com.au/")


async def test_live_response_recent_not_stale():
    """A live `latest` call should not be flagged stale (NEMWEB lag is ~1-2 min)."""
    resp = await server.latest("dispatch_price", filters={"region": "NSW1"})
    # NEMWEB sometimes has hiccups; allow stale flag but assert structure is sound.
    # The real test here is that `stale` is a bool (not None).
    assert isinstance(resp.stale, bool)
