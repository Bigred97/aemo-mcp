"""Server-level integration tests with mocked NEMWEB."""
from __future__ import annotations

from pathlib import Path

import pytest
import respx

from aemo_mcp import cache as cache_mod
from aemo_mcp import server


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path: Path, monkeypatch):
    """Force the default cache into a per-test temp dir."""
    monkeypatch.setattr(cache_mod, "DEFAULT_DB_PATH", tmp_path / "cache.db")


@respx.mock
async def test_latest_dispatch_price_nsw(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.latest("dispatch_price", filters={"region": "NSW1"})
    assert resp.dataset_id == "dispatch_price"
    assert len(resp.records) == 1
    assert resp.records[0].value == 87.5  # type: ignore
    assert resp.records[0].dimensions["region"] == "NSW1"  # type: ignore


@respx.mock
async def test_latest_dispatch_price_negative_sa(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.latest("dispatch_price", filters={"region": "SA1"})
    assert resp.records[0].value == -15.40  # type: ignore


@respx.mock
async def test_get_data_all_regions_no_filter(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.get_data("dispatch_price")
    regions = {o.dimensions["region"] for o in resp.records}  # type: ignore
    assert regions == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


@respx.mock
async def test_get_data_csv_format(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.get_data(
        "dispatch_price", filters={"region": "NSW1"}, format="csv"
    )
    assert resp.csv is not None
    assert "NSW1" in resp.csv
    assert "period,value,unit" in resp.csv


@respx.mock
async def test_latest_interconnector_flow(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.latest(
        "interconnector_flows", filters={"interconnector": "V-SA"}
    )
    flow = next(
        o for o in resp.records  # type: ignore
        if o.dimensions["metric"] == "mw_flow"
    )
    assert flow.value == 250.0  # type: ignore


@respx.mock
async def test_latest_generation_scada_by_fuel(
    scada_listing_html, dispatch_scada_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/Dispatch_SCADA/"
    ).respond(200, text=scada_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_scada_zip)

    resp = await server.latest(
        "generation_scada", filters={"fuel": "wind"}
    )
    duids = {o.dimensions["duid"] for o in resp.records}  # type: ignore
    assert "COOPGWF1" in duids
    assert "BW01" not in duids


@respx.mock
async def test_latest_generation_scada_qld(
    scada_listing_html, dispatch_scada_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/Dispatch_SCADA/"
    ).respond(200, text=scada_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_scada_zip)

    resp = await server.latest(
        "generation_scada", filters={"region": "QLD1"}
    )
    duids = {o.dimensions["duid"] for o in resp.records}  # type: ignore
    # Only QLD DUIDs survive
    assert "COOPGWF1" in duids
    assert "BW01" not in duids


@respx.mock
async def test_attribution_present_on_every_response(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.latest("dispatch_price", filters={"region": "NSW1"})
    assert "AEMO" in resp.attribution
    assert "Copyright Permissions" in resp.attribution
    assert resp.source_url.startswith("https://www.nemweb.com.au/")
    assert resp.source == "Australian Energy Market Operator"


@respx.mock
async def test_server_version_carried_in_response(
    dispatchis_listing_html, dispatch_is_zip
):
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.latest("dispatch_price", filters={"region": "NSW1"})
    assert resp.server_version
    assert resp.server_version != ""


@respx.mock
async def test_search_then_describe_then_latest_flow(
    dispatchis_listing_html, dispatch_is_zip
):
    """End-to-end discovery flow: search → describe → fetch."""
    results = await server.search_datasets("spot price")
    top = results[0]
    assert top.id == "dispatch_price"

    detail = await server.describe_dataset(top.id)
    assert "region" in [f.key for f in detail.filters]

    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=dispatchis_listing_html)
    respx.get(
        "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    ).respond(200, content=dispatch_is_zip)

    resp = await server.latest(top.id, filters={"region": "NSW1"})
    assert resp.dataset_id == top.id
