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


# ─── stale-fallback graceful degradation (CLAUDE.md quality dim #4) ──────


async def _prime_stale_cache(
    db_path: Path, url: str, payload: bytes, age_hours: float
) -> None:
    """Put `payload` into the cache as if it was fetched `age_hours` ago.

    Used to test the stale-fallback path: a regular cache.get() with a normal
    TTL will miss this row (because cached_at is older than the TTL window),
    but cache.get_stale() will still return it.

    The longest TTL in the aemo-mcp cache is `archive` (7 days) — fetch_zip
    defaults to that kind. So `age_hours` must be > 168 to ensure cache.get()
    misses and the stale-fallback path is exercised.
    """
    import time

    import aiosqlite

    cache = Cache(db_path=db_path)
    await cache._ensure_init()
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "INSERT INTO http_cache (cache_key, payload, cached_at, kind) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(cache_key) DO UPDATE SET "
            "payload=excluded.payload, cached_at=excluded.cached_at",
            (url, payload, time.time() - age_hours * 3600, "archive"),
        )
        await conn.commit()


@respx.mock
async def test_stale_fallback_serves_cached_payload_on_5xx(
    tmp_path: Path, dispatch_is_zip: bytes
):
    """When NEMWEB returns 5xx and we have a cached payload past its TTL,
    serve the cached payload and mark the response as stale. Agents continue
    reasoning rather than crashing."""
    from aemo_mcp.client import get_stale_signal, reset_stale_signal

    db_path = tmp_path / "c.db"
    url = (
        "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        "PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    )
    # Prime a 2h-old cache entry — past the 60s live TTL, so cache.get()
    # misses but cache.get_stale() will still return it.
    # 240h = 10 days, past the longest TTL (archive = 7 days), so a regular
    # cache.get() misses and the stale-fallback path is exercised.
    await _prime_stale_cache(db_path, url, dispatch_is_zip, age_hours=240.0)

    reset_stale_signal()
    cache = Cache(db_path=db_path)
    client = AEMOClient(cache=cache)
    try:
        respx.get(url).respond(503, text="Service Unavailable")
        body = await client.fetch_zip(url)
        assert body == dispatch_is_zip, "fallback payload must equal cached"
        stale, reason = get_stale_signal()
        assert stale is True, "stale flag must be set after 5xx fallback"
        assert reason and "503" in reason, (
            f"stale_reason should mention the 5xx: {reason}"
        )
        assert "minute" in reason.lower(), (
            f"stale_reason should report age: {reason}"
        )
    finally:
        await client.aclose()


@respx.mock
async def test_stale_fallback_serves_cached_on_request_error(
    tmp_path: Path, dispatch_is_zip: bytes
):
    """Same as 5xx test but for httpx.RequestError (DNS / connection refused
    / etc.)."""
    from aemo_mcp.client import get_stale_signal, reset_stale_signal

    db_path = tmp_path / "c.db"
    url = (
        "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        "PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    )
    # 240h = 10 days, past the longest TTL (archive = 7 days), so a regular
    # cache.get() misses and the stale-fallback path is exercised.
    await _prime_stale_cache(db_path, url, dispatch_is_zip, age_hours=240.0)

    reset_stale_signal()
    respx.get(url).mock(side_effect=httpx.ConnectError("simulated DNS failure"))

    cache = Cache(db_path=db_path)
    client = AEMOClient(cache=cache)
    try:
        body = await client.fetch_zip(url)
        assert body == dispatch_is_zip
        stale, reason = get_stale_signal()
        assert stale is True
        assert reason and "ConnectError" in reason, (
            f"stale_reason should mention ConnectError: {reason}"
        )
    finally:
        await client.aclose()


@respx.mock
async def test_raises_when_no_stale_cache_to_fall_back_to(tmp_path: Path):
    """Empty cache + upstream 5xx → still raises AEMOAPIError (original
    behaviour when there's nothing to gracefully degrade to)."""
    from aemo_mcp.client import reset_stale_signal

    reset_stale_signal()
    url = (
        "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
        "PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"
    )
    respx.get(url).respond(503, text="Service Unavailable")

    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        with pytest.raises(AEMOAPIError, match="503"):
            await client.fetch_zip(url)
    finally:
        await client.aclose()


async def test_cache_get_stale_returns_payload_and_timestamp(tmp_path: Path):
    """Cache.get_stale() returns (payload, cached_at) regardless of TTL —
    the building block for client's stale-fallback path."""
    from datetime import timedelta

    cache = Cache(db_path=tmp_path / "c.db")
    await cache.set("http://nemweb.com.au/x", b"hello", kind="live")
    # Normal `get` with a tiny TTL should miss
    fresh = await cache.get("http://nemweb.com.au/x", ttl=timedelta(seconds=0))
    assert fresh is None
    # `get_stale` should return regardless of TTL
    stale = await cache.get_stale("http://nemweb.com.au/x")
    assert stale is not None
    payload, cached_at = stale
    assert payload == b"hello"
    assert cached_at > 0
    # Non-existent key → None
    miss = await cache.get_stale("http://nemweb.com.au/missing")
    assert miss is None
