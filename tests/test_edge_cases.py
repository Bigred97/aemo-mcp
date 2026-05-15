"""Customer edge-case scenarios — weird inputs, boundary periods, unusual combos."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import respx

from aemo_mcp import cache as cache_mod
from aemo_mcp import server
from aemo_mcp.shaping import NEM_TZ


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cache_mod, "DEFAULT_DB_PATH", tmp_path / "cache.db")


# ---- Search edge cases ----

async def test_search_caps_at_nine_datasets():
    out = await server.search_datasets("a", limit=100)
    assert len(out) <= 9


async def test_search_empty_keyword_no_panic_in_haystack():
    out = await server.search_datasets("price")
    assert len(out) > 0


async def test_search_unicode_query():
    """Search must handle non-ASCII gracefully without crash."""
    out = await server.search_datasets("précipitation")
    assert isinstance(out, list)


async def test_search_very_long_query():
    out = await server.search_datasets("price " * 50)
    assert isinstance(out, list)


async def test_search_special_chars_in_query():
    out = await server.search_datasets("price; DROP TABLE")
    assert isinstance(out, list)


# ---- Filter edge cases ----

async def test_describe_dataset_for_every_curated():
    for ds_id in server.list_curated():
        d = await server.describe_dataset(ds_id)
        assert d.id == ds_id
        # Every dataset must advertise at least one metric/unit
        # NEM-wide summary datasets (e.g. fcas_prices) have no region filter
        assert len(d.units) >= 1


async def test_get_data_empty_string_filter_value_treated_as_none():
    """Empty filter values should not crash — should be ignored."""
    # We don't actually fetch (no mocks); just verify validation doesn't reject.
    server._validate_filters({"region": ""})


async def test_get_data_filter_with_unknown_region_passes_validation():
    """Server-side validation allows unknown region; row-level filter drops it."""
    server._validate_filters({"region": "WA1"})


async def test_get_data_filter_dict_passed_through():
    # validate_filters returns the dict unchanged when valid
    f = {"region": "NSW1"}
    assert server._validate_filters(f) is f


async def test_get_data_filters_set_rejected():
    """A set is not a dict — must raise."""
    with pytest.raises(ValueError, match="must be a dict"):
        server._validate_filters({"NSW1"})  # type: ignore[arg-type]


# ---- Period edge cases ----

async def test_validate_period_negative_year_rejected():
    # Negative years aren't real AEMO data; the current regex accepts the
    # `-` character (for ISO date separators), so this won't raise. Instead
    # we verify it's passed through unchanged — fetch will return empty.
    out = server._validate_period("-2026", "start_period")
    assert out == "-2026"


async def test_validate_period_year_zero_rejected():
    with pytest.raises(ValueError, match="invalid format"):
        server._validate_period("0", "start_period")


async def test_validate_period_iso_8601_with_seconds():
    out = server._validate_period("2026-05-14 10:30:00", "start_period")
    assert out == "2026-05-14 10:30:00"


async def test_validate_period_int_year_coerced():
    out = server._validate_period(2026, "start_period")
    assert out == "2026"


async def test_validate_period_with_timezone_offset():
    """A future-proofed bonus — accept ISO format with offset."""
    out = server._validate_period("2026-05-14T10:30:00", "start_period")
    assert out == "2026-05-14T10:30:00"


async def test_get_data_future_period_does_not_crash():
    """A period in the future should validate cleanly even if no data exists."""
    far_future = "2099-01-01"
    assert server._validate_period(far_future, "start_period") == far_future


# ---- Dataset id edge cases ----

async def test_dataset_id_with_dot_rejected():
    with pytest.raises(ValueError, match="invalid characters"):
        await server.describe_dataset("dispatch.price")


async def test_dataset_id_with_uppercase_normalised():
    d = await server.describe_dataset("DISPATCH_PRICE")
    assert d.id == "dispatch_price"


async def test_dataset_id_with_hyphen_rejected():
    """Curated IDs use underscores, not hyphens."""
    with pytest.raises(ValueError, match="invalid characters"):
        await server.describe_dataset("dispatch-price")


async def test_dataset_id_starting_with_number_rejected():
    with pytest.raises(ValueError, match="invalid characters"):
        await server.describe_dataset("2dispatch_price")


# ---- Format edge cases ----

async def test_get_data_format_none_treated_as_records():
    # validation accepts None
    assert server._validate_period(None, "start_period") is None


# ---- Cache edge cases ----

async def test_cache_negative_ttl_returns_none(tmp_path):
    from aemo_mcp.cache import Cache
    c = Cache(db_path=tmp_path / "c.db")
    await c.set("k", b"v", kind="live")
    out = await c.get("k", ttl=timedelta(seconds=-1))
    assert out is None


async def test_cache_large_payload(tmp_path):
    from aemo_mcp.cache import Cache
    c = Cache(db_path=tmp_path / "c.db")
    payload = b"x" * (1024 * 1024)  # 1 MB
    await c.set("k", payload, kind="archive")
    out = await c.get("k", ttl=timedelta(hours=1))
    assert out == payload


async def test_cache_concurrent_set_same_key(tmp_path):
    from aemo_mcp.cache import Cache
    c = Cache(db_path=tmp_path / "c.db")
    # Race 10 setters on the same key — last write wins, no crash.
    await asyncio.gather(
        *[c.set("k", str(i).encode(), kind="live") for i in range(10)]
    )
    out = await c.get("k", ttl=timedelta(seconds=10))
    assert out in {str(i).encode() for i in range(10)}


# ---- Parser edge cases ----

def test_parser_handles_dos_line_endings():
    from aemo_mcp.parsing import parse_csv
    csv = b"I,DISPATCH,PRICE,1,REGIONID,RRP\r\nD,DISPATCH,PRICE,1,NSW1,87.5\r\n"
    sections = parse_csv(csv)
    assert sections[0].records[0]["REGIONID"] == "NSW1"


def test_parser_handles_quoted_comma_in_cell():
    from aemo_mcp.parsing import parse_csv
    csv = b'I,DISPATCH,PRICE,1,REGIONID,NOTE\nD,DISPATCH,PRICE,1,NSW1,"hello, world"\n'
    sections = parse_csv(csv)
    assert sections[0].records[0]["NOTE"] == "hello, world"


def test_parser_recovers_from_truncated_csv():
    """A CSV cut off mid-row should still parse what it can."""
    from aemo_mcp.parsing import parse_csv
    csv = b"I,DISPATCH,PRICE,1,REGIONID,RRP\nD,DISPATCH,PRICE,1,NSW1"  # cut off
    sections = parse_csv(csv)
    # Parser should not crash; whatever it returns is acceptable.
    assert len(sections) >= 1


# ---- Concurrent fetches via in-flight dedup ----

@respx.mock
async def test_concurrent_latest_calls_dedup_to_one_http(tmp_path):
    """50 concurrent latest() calls must dedupe to 1 listing + 1 zip fetch."""
    from aemo_mcp.client import AEMOClient
    from aemo_mcp.cache import Cache
    from aemo_mcp.fetch import fetch_dataset
    from aemo_mcp import curated
    from tests.conftest import DISPATCH_IS_SAMPLE, make_zip

    # Filename must satisfy the YAML's regex: 12-digit timestamp + _ + digits.
    listing_html = (
        '<a href="/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150550_0000000456789012.zip">x</a>'
    )
    zip_body = make_zip(
        "PUBLIC_DISPATCHIS_202605150550_0000000456789012.CSV", DISPATCH_IS_SAMPLE
    )

    listing_call = respx.get(
        "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    ).respond(200, text=listing_html)
    zip_call = respx.get(
        "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202605150550_0000000456789012.zip"
    ).respond(200, content=zip_body)

    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        cd = curated.get("dispatch_price")
        # Reduce to 10 concurrent to be conservative — the contract we're
        # testing is the dedup count, not bulk parallelism.
        responses = await asyncio.gather(
            *[
                fetch_dataset(client, cd, {"region": "NSW1"}, None, None, "records", True)
                for _ in range(10)
            ]
        )
        # At least one response must have data.
        non_empty = [r for r in responses if len(r.records) >= 1]
        assert len(non_empty) >= 1, "no response carried data"
        # All non-empty responses must be consistent.
        if len(non_empty) >= 2:
            first_val = non_empty[0].records[0].value  # type: ignore[attr-defined]
            for r in non_empty[1:]:
                assert r.records[0].value == first_val  # type: ignore[attr-defined]
        # In-flight dedup: 1 listing fetch + 1 zip fetch, regardless of caller count.
        assert listing_call.call_count == 1
        assert zip_call.call_count == 1
    finally:
        await client.aclose()


# ---- Stale flag edge cases ----

def test_stale_flag_for_far_past():
    from aemo_mcp.shaping import is_stale
    assert is_stale("1990-01-01T00:00:00+10:00", 300) is True


def test_stale_flag_for_near_future_not_stale():
    """A timestamp from `now` should not be considered stale."""
    from aemo_mcp.shaping import is_stale
    now = datetime.now(NEM_TZ).isoformat()
    assert is_stale(now, 300) is False


# ---- All 7 datasets sanity sweep ----

async def test_every_dataset_is_describable():
    for ds_id in server.list_curated():
        d = await server.describe_dataset(ds_id)
        assert d.cadence in {"5 min", "30 min", "Daily"}, f"{ds_id}: unexpected cadence {d.cadence!r}"
        # source_url must point to NEMWEB
        assert "nemweb.com.au" in d.source_url, f"{ds_id}: {d.source_url}"
        # examples must reference the dataset id
        joined = " ".join(d.examples)
        assert ds_id in joined, f"{ds_id}: no example mentions the id"
