"""Async NEMWEB fetcher.

NEMWEB is a static IIS file server. There is no API — directory listings are
HTML; files are ZIPs containing CSVs.

Three responsibilities:

1. `fetch_directory_listing(folder)` — GET the IIS HTML and return the list
   of file names matching a regex.
2. `fetch_zip(url)` — GET a single ZIP file and return its bytes.
3. Cache + in-flight dedup — concurrent callers for the same URL share one
   HTTP request. Critical at 5-min cadence where 50 concurrent `latest()`
   calls would otherwise hammer NEMWEB.

NEMWEB's published files are immutable once written (filename embeds the
interval timestamp), so the file-body cache TTL is effectively infinite.
Only the directory listing has freshness sensitivity — that's the 30s TTL.
"""
from __future__ import annotations

import asyncio
import re
import time
from contextvars import ContextVar
from typing import Any

import httpx

from .cache import TTL, Cache, CacheKind


# ─── stale signal (graceful-degradation reporting per CLAUDE.md dim #4) ─
# When NEMWEB is unreachable, _fetch_cached falls back to the cached payload
# regardless of TTL and records the staleness in this ContextVar. Server-side
# tool wrappers read it after the request chain and set
# DataResponse.stale / .stale_reason. ContextVar (not instance attr) so
# concurrent MCP tool calls each see their own state.
_stale_signal: ContextVar[tuple[bool, str | None]] = ContextVar(
    "aemo_mcp_stale_signal", default=(False, None)
)


def reset_stale_signal() -> None:
    """Clear the stale state. Call once at the start of each tool call."""
    _stale_signal.set((False, None))


def get_stale_signal() -> tuple[bool, str | None]:
    """Return (stale, reason) for the most recent fetch chain in this context."""
    return _stale_signal.get()


def _mark_stale(reason: str) -> None:
    """Record that a stale-cache fallback was served this context.

    If multiple fetches in one chain are stale, we keep the FIRST reason
    (it's usually the most informative — the originating upstream failure).
    """
    cur_stale, _ = _stale_signal.get()
    if not cur_stale:
        _stale_signal.set((True, reason))

DEFAULT_BASE_URL = "https://www.nemweb.com.au"
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

# IIS directory listings render entries as <A HREF="path">name</A>.
# Capture the file name (last segment after the slash) — relative or absolute.
_HREF_PATTERN = re.compile(
    r'<a\s+href="([^"]+)"',
    re.IGNORECASE,
)


class AEMOAPIError(Exception):
    """Raised when NEMWEB returns a non-2xx response or the request fails."""


class AEMOClient:
    def __init__(
        self,
        cache: Cache | None = None,
        base_url: str = DEFAULT_BASE_URL,
        transport: httpx.AsyncBaseTransport | None = None,
        user_agent: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache = cache or Cache()
        ua = user_agent or "aemo-mcp/0.1 (+https://github.com/Bigred97/aemo-mcp)"
        self._http = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT,
            transport=transport,
            headers={"User-Agent": ua},
            follow_redirects=True,
        )
        self._in_flight: dict[str, asyncio.Future[bytes]] = {}
        self._in_flight_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AEMOClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def _fetch_cached(self, url: str, *, kind: CacheKind) -> bytes:
        """Generic cached + in-flight-deduped fetch.

        Concurrent callers for the same URL share one in-flight HTTP request.
        """
        cached = await self.cache.get(url, ttl=TTL[kind])
        if cached is not None:
            return cached

        # Race-safe in-flight registration.
        async with self._in_flight_lock:
            existing = self._in_flight.get(url)
            if existing is None:
                future: asyncio.Future[bytes] = (
                    asyncio.get_running_loop().create_future()
                )
                self._in_flight[url] = future

        if existing is not None:
            return await existing

        try:
            try:
                resp = await self._http.get(url)
                resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                # Graceful degradation: when NEMWEB is unreachable, fall back
                # to the most-recent cached payload (regardless of TTL) rather
                # than raising and breaking the agent's chain of reasoning.
                # The staleness is surfaced via the _stale_signal ContextVar
                # and ends up in DataResponse.stale / stale_reason.
                fallback = await self.cache.get_stale(url)
                if fallback is not None:
                    payload, cached_at = fallback
                    age_min = max(0, int((time.time() - cached_at) / 60))
                    if isinstance(e, httpx.HTTPStatusError):
                        upstream = (
                            f"AEMO/OpenNEM fetch returned "
                            f"{e.response.status_code}"
                        )
                    else:
                        upstream = (
                            f"AEMO/OpenNEM fetch unreachable "
                            f"({type(e).__name__})"
                        )
                    _mark_stale(
                        f"{upstream} for {url}; serving cached payload from "
                        f"~{age_min} minute(s) ago"
                    )
                    future.set_result(payload)
                    return payload
                # Genuinely no cache to fall back to — preserve original behaviour
                if isinstance(e, httpx.HTTPStatusError):
                    raise AEMOAPIError(
                        f"NEMWEB returned {e.response.status_code} for {url}"
                    ) from e
                raise AEMOAPIError(f"NEMWEB request failed: {e}") from e
            await self.cache.set(url, resp.content, kind=kind)
            future.set_result(resp.content)
            return resp.content
        except BaseException as e:
            if not future.done():
                future.set_exception(e)
            # When no other coroutine is awaiting this future, the
            # exception we just set would be GC'd unretrieved and Python
            # would log "Future exception was never retrieved". Mark it
            # retrieved here — the calling coroutine still gets `raise` so
            # the exception propagates normally.
            try:
                future.exception()
            except Exception:
                pass
            raise
        finally:
            async with self._in_flight_lock:
                self._in_flight.pop(url, None)

    async def fetch_directory_listing(
        self,
        folder: str,
        *,
        filename_regex: re.Pattern[str] | None = None,
        kind: CacheKind = "listing",
    ) -> list[str]:
        """Fetch a NEMWEB folder's HTML listing and return sorted file names.

        `folder` is a path under `base_url`, e.g.
        '/Reports/Current/DispatchIS_Reports/'. Trailing slash optional.

        If `filename_regex` is provided, only file names that fully match it
        are returned. The list is sorted ascending — for AEMO filename
        patterns (timestamp-prefixed) this means the LAST entry is the most
        recent.
        """
        if not folder.startswith("/"):
            folder = "/" + folder
        if not folder.endswith("/"):
            folder = folder + "/"
        url = f"{self.base_url}{folder}"
        body = await self._fetch_cached(url, kind=kind)
        text = body.decode("utf-8", errors="replace")

        names: list[str] = []
        for match in _HREF_PATTERN.finditer(text):
            href = match.group(1)
            # Normalise: drop everything up to the final slash.
            name = href.rsplit("/", 1)[-1]
            if not name or name in (".", ".."):
                continue
            if filename_regex is not None and not filename_regex.fullmatch(name):
                continue
            names.append(name)
        names.sort()
        return names

    async def fetch_zip(self, url: str, *, kind: CacheKind = "archive") -> bytes:
        """Fetch a single NEMWEB ZIP file. Returns raw bytes.

        Timestamped files are immutable — once a file with name
        `PUBLIC_DISPATCHIS_202605141000_X.zip` exists it never changes. Cache
        with `archive` TTL (7 days) by default. Caller can override for
        feeds where files might be republished.
        """
        return await self._fetch_cached(url, kind=kind)

    def build_url(self, folder: str, filename: str) -> str:
        if not folder.startswith("/"):
            folder = "/" + folder
        if not folder.endswith("/"):
            folder = folder + "/"
        return f"{self.base_url}{folder}{filename}"
