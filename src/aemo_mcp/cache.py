"""SQLite-backed HTTP cache with per-read TTL.

Ported from rba-mcp 0.1.9. Same single-table design — TTL is evaluated at
read time so one cached row can satisfy different freshness windows. The
`kind` column lets us run targeted invalidation later.

CacheKind values are tuned to AEMO cadences:
- "live"      → 60 seconds  (5-min dispatch feeds)
- "half_hour" → 5 minutes   (30-min feeds: rooftop PV, predispatch)
- "forecast"  → 1 hour      (longer-horizon forecast bundles)
- "daily"     → 24 hours    (daily rolled-up archives)
- "archive"   → 7 days      (MMSDM and other immutable historical files)
- "listing"   → 30 seconds  (NEMWEB directory HTML — drives latest-file detection)

NEMWEB's published files are immutable once written (filename embeds the
interval timestamp), so the file-body cache is effectively infinite — TTL
only matters for directory listings and "latest" calls.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import timedelta
from pathlib import Path
from typing import Literal

import aiosqlite

CacheKind = Literal["live", "half_hour", "forecast", "daily", "archive", "listing"]

DEFAULT_DB_PATH = Path.home() / ".aemo-mcp" / "cache.db"

TTL: dict[CacheKind, timedelta] = {
    "live": timedelta(seconds=60),
    "half_hour": timedelta(minutes=5),
    "forecast": timedelta(hours=1),
    "daily": timedelta(hours=24),
    "archive": timedelta(days=7),
    "listing": timedelta(seconds=30),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS http_cache (
    cache_key  TEXT PRIMARY KEY,
    payload    BLOB NOT NULL,
    cached_at  REAL NOT NULL,
    kind       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kind_cached_at ON http_cache(kind, cached_at);
"""


class Cache:
    def __init__(self, db_path: Path | None = None) -> None:
        # Resolve DEFAULT_DB_PATH at construction time (not class-def time)
        # so tests that monkeypatch the module-level constant take effect.
        # Default Path() args are evaluated once at class definition, which
        # is too early to be overridden — leaked across tests previously.
        import aemo_mcp.cache as _self_mod
        self.db_path = db_path or _self_mod.DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def _ensure_init(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            try:
                await self._init_schema()
            except sqlite3.DatabaseError:
                # Pre-existing cache.db is corrupt or has an incompatible
                # schema. The cache is a perf optimisation, not a source of
                # truth — drop and recreate is always safe.
                self.db_path.unlink(missing_ok=True)
                await self._init_schema()
            self._initialized = True

    async def _init_schema(self) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.executescript(_SCHEMA)
            await conn.commit()

    async def get(self, key: str, ttl: timedelta) -> bytes | None:
        await self._ensure_init()
        cutoff = time.time() - ttl.total_seconds()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT payload FROM http_cache WHERE cache_key = ? AND cached_at >= ?",
                (key, cutoff),
            ) as cur:
                row = await cur.fetchone()
        return row[0] if row else None

    async def get_stale(self, key: str) -> tuple[bytes, float] | None:
        """Return cached (payload, cached_at_epoch) regardless of TTL.

        Used by the client as a fallback when NEMWEB is unavailable —
        graceful degradation per CLAUDE.md quality dimension #4. The caller
        computes "how stale" from the timestamp and surfaces it in
        `DataResponse.stale_reason`.
        """
        await self._ensure_init()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT payload, cached_at FROM http_cache WHERE cache_key = ?",
                (key,),
            ) as cur:
                row = await cur.fetchone()
        return (row[0], row[1]) if row else None

    async def set(self, key: str, value: bytes, kind: CacheKind) -> None:
        await self._ensure_init()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                INSERT INTO http_cache (cache_key, payload, cached_at, kind)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload = excluded.payload,
                    cached_at = excluded.cached_at,
                    kind = excluded.kind
                """,
                (key, value, time.time(), kind),
            )
            await conn.commit()

    async def clear(self, kind: CacheKind | None = None) -> None:
        await self._ensure_init()
        async with aiosqlite.connect(self.db_path) as conn:
            if kind:
                await conn.execute("DELETE FROM http_cache WHERE kind = ?", (kind,))
            else:
                await conn.execute("DELETE FROM http_cache")
            await conn.commit()
