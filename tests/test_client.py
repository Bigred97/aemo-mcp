"""HTTP client tests — directory listing, fetch, cache hit, in-flight dedup."""
from __future__ import annotations

import asyncio
import re
from pathlib import Path

import httpx
import pytest
import respx

from aemo_mcp.cache import Cache
from aemo_mcp.client import AEMOAPIError, AEMOClient


@respx.mock
async def test_fetch_directory_listing_basic(tmp_path: Path, dispatchis_listing_html):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)

        names = await client.fetch_directory_listing(
            "/Reports/Current/DispatchIS_Reports/",
            filename_regex=re.compile(r"PUBLIC_DISPATCHIS_\d{12}_\d+\.zip"),
        )
        assert len(names) == 3
        assert names[-1] == "PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    finally:
        await client.aclose()


@respx.mock
async def test_listing_lexicographic_max_is_latest(tmp_path: Path, dispatchis_listing_html):
    """The lex-max filename is the most recent — that's the contract `latest` relies on."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        names = await client.fetch_directory_listing(
            "/Reports/Current/DispatchIS_Reports/",
            filename_regex=re.compile(r"PUBLIC_DISPATCHIS_\d{12}_\d+\.zip"),
        )
        assert names == sorted(names)
    finally:
        await client.aclose()


@respx.mock
async def test_listing_excludes_parent_directory_link(tmp_path: Path, dispatchis_listing_html):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        ).respond(200, text=dispatchis_listing_html)
        names = await client.fetch_directory_listing(
            "/Reports/Current/DispatchIS_Reports/",
            filename_regex=re.compile(r".*\.zip"),
        )
        assert all("[To Parent Directory]" not in n for n in names)
        assert all(n.startswith("PUBLIC_") for n in names)
    finally:
        await client.aclose()


@respx.mock
async def test_listing_404_raises(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        respx.get(
            "http://nemweb.com.au/Reports/Current/NoSuch/"
        ).respond(404)
        with pytest.raises(AEMOAPIError, match="404"):
            await client.fetch_directory_listing("/Reports/Current/NoSuch/")
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_zip_returns_bytes(tmp_path: Path, dispatch_is_zip):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        respx.get(url).respond(200, content=dispatch_is_zip)
        body = await client.fetch_zip(url)
        assert body == dispatch_is_zip
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_zip_cached_on_second_call(tmp_path: Path, dispatch_is_zip):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
        route = respx.get(url).respond(200, content=dispatch_is_zip)
        await client.fetch_zip(url)
        await client.fetch_zip(url)
        # Both calls hit but only one should have actually fetched (cache hit)
        assert route.call_count == 1
    finally:
        await client.aclose()


@respx.mock
async def test_in_flight_dedup_one_http_call(tmp_path: Path, dispatch_is_zip):
    """Concurrent identical requests must share one HTTP call."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"

        async def slow_response(request):
            await asyncio.sleep(0.05)
            return httpx.Response(200, content=dispatch_is_zip)

        route = respx.get(url).mock(side_effect=slow_response)
        results = await asyncio.gather(*[client.fetch_zip(url) for _ in range(10)])
        assert all(r == dispatch_is_zip for r in results)
        assert route.call_count == 1
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_zip_500_raises_api_error(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/X.zip"
        respx.get(url).respond(500)
        with pytest.raises(AEMOAPIError, match="500"):
            await client.fetch_zip(url)
    finally:
        await client.aclose()


@respx.mock
async def test_fetch_zip_network_error_raises_api_error(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/X.zip"
        respx.get(url).mock(side_effect=httpx.ConnectError("network down"))
        with pytest.raises(AEMOAPIError, match="request failed"):
            await client.fetch_zip(url)
    finally:
        await client.aclose()


async def test_build_url_normalises_slashes(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        url = client.build_url("Reports/Current/DispatchIS_Reports", "X.zip")
        assert url == "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/X.zip"

        url = client.build_url("/Reports/Current/DispatchIS_Reports/", "X.zip")
        assert url == "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/X.zip"
    finally:
        await client.aclose()


@respx.mock
async def test_listing_filters_by_regex(tmp_path: Path):
    """Non-matching entries must be excluded from the returned list."""
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        html = '<a href="/foo/PUBLIC_DISPATCHIS_X.zip">x</a><a href="/foo/IRRELEVANT.txt">i</a>'
        respx.get("http://nemweb.com.au/foo/").respond(200, text=html)
        names = await client.fetch_directory_listing(
            "/foo/",
            filename_regex=re.compile(r"PUBLIC_DISPATCHIS_.*\.zip"),
        )
        assert names == ["PUBLIC_DISPATCHIS_X.zip"]
    finally:
        await client.aclose()


@respx.mock
async def test_client_sends_user_agent(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache, user_agent="aemo-mcp-test/1.0")
    captured_ua: list[str] = []
    try:
        async def capture(request):
            captured_ua.append(request.headers.get("User-Agent", ""))
            return httpx.Response(200, text="")

        respx.get("http://nemweb.com.au/foo/").mock(side_effect=capture)
        await client.fetch_directory_listing("/foo/")
        assert "aemo-mcp-test/1.0" in captured_ua[0]
    finally:
        await client.aclose()


async def test_client_context_manager(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "c.db")
    async with AEMOClient(cache=cache) as client:
        assert client is not None
