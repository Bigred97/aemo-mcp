"""Server-side input validation guards (offline — no network calls)."""
from __future__ import annotations

import pytest

from aemo_mcp import server


async def test_search_datasets_empty_query_raises():
    with pytest.raises(ValueError, match="query is required"):
        await server.search_datasets("")


async def test_search_datasets_whitespace_query_raises():
    with pytest.raises(ValueError, match="query is required"):
        await server.search_datasets("   ")


async def test_search_datasets_non_string_query():
    with pytest.raises(ValueError, match="must be a string"):
        await server.search_datasets(query=123)  # type: ignore[arg-type]


async def test_search_datasets_negative_limit():
    with pytest.raises(ValueError, match=">= 1"):
        await server.search_datasets("price", limit=0)


async def test_search_datasets_bool_limit_rejected():
    """bool is a subclass of int — must be rejected explicitly."""
    with pytest.raises(ValueError, match="positive integer"):
        await server.search_datasets("price", limit=True)  # type: ignore[arg-type]


async def test_search_datasets_negative_int_limit():
    with pytest.raises(ValueError, match=">= 1"):
        await server.search_datasets("price", limit=-5)


async def test_describe_dataset_unknown_raises():
    with pytest.raises(ValueError, match="not a known AEMO dataset"):
        await server.describe_dataset("xyz_nonsense")


async def test_describe_dataset_garbage_id_rejected():
    with pytest.raises(ValueError, match="invalid characters"):
        await server.describe_dataset("dispatch_price; DROP TABLE")


async def test_describe_dataset_empty_id():
    with pytest.raises(ValueError, match="empty"):
        await server.describe_dataset("")


async def test_describe_dataset_non_string():
    with pytest.raises(ValueError, match="must be a string"):
        await server.describe_dataset(dataset_id=42)  # type: ignore[arg-type]


async def test_describe_dataset_case_insensitive():
    detail = await server.describe_dataset("DISPATCH_PRICE")
    assert detail.id == "dispatch_price"


async def test_describe_dataset_all_seven():
    """Every curated dataset must describe cleanly."""
    for ds_id in server.list_curated():
        detail = await server.describe_dataset(ds_id)
        assert detail.id == ds_id
        assert detail.name
        assert detail.cadence


async def test_get_data_invalid_format_string():
    with pytest.raises(ValueError, match="Unknown format"):
        await server.get_data(
            "dispatch_price", filters={"region": "NSW1"}, format="JSON"  # type: ignore[arg-type]
        )


async def test_get_data_non_string_format():
    with pytest.raises(ValueError, match="must be a string"):
        await server.get_data(
            "dispatch_price", filters={"region": "NSW1"}, format=42  # type: ignore[arg-type]
        )


async def test_get_data_end_before_start():
    with pytest.raises(ValueError, match="end_period .* is before start_period"):
        await server.get_data(
            "dispatch_price", filters={"region": "NSW1"},
            start_period="2026-05-14", end_period="2026-05-13"
        )


async def test_get_data_garbage_period():
    with pytest.raises(ValueError, match="invalid format"):
        await server.get_data(
            "dispatch_price", filters={"region": "NSW1"},
            start_period="not a date"
        )


async def test_get_data_unknown_dataset():
    with pytest.raises(ValueError, match="not a known AEMO dataset"):
        await server.get_data("nonsense_dataset")


async def test_get_data_unknown_filter_key():
    with pytest.raises(ValueError, match="Unknown filter key"):
        await server.get_data(
            "dispatch_price", filters={"wibble": "NSW1"}
        )


async def test_get_data_non_dict_filters():
    with pytest.raises(ValueError, match="filters must be a dict"):
        await server.get_data(
            "dispatch_price", filters=["NSW1"]  # type: ignore[arg-type]
        )


async def test_get_data_int_period_coerced_to_string():
    """An int year must be coerced to a string at the validation boundary.

    Verify by checking the validator directly — we don't want to hit the
    network just to test type coercion.
    """
    out = server._validate_period(2026, "start_period")
    assert out == "2026"


async def test_get_data_int_period_zero_returns_none_safely():
    # Currently treated as a year-zero attempt — coerce to "0" then reject
    # on regex (too few digits).
    with pytest.raises(ValueError, match="invalid format"):
        server._validate_period(0, "start_period")


async def test_get_data_bool_period_rejected():
    with pytest.raises(ValueError, match="must be a string"):
        await server.get_data(
            "dispatch_price", filters={"region": "NSW1"}, start_period=True  # type: ignore[arg-type]
        )


async def test_latest_unknown_dataset():
    with pytest.raises(ValueError, match="not a known AEMO dataset"):
        await server.latest("nonsense_dataset")


async def test_latest_garbage_id():
    with pytest.raises(ValueError, match="invalid characters"):
        await server.latest("dispatch; DROP")


async def test_latest_unknown_filter_key():
    with pytest.raises(ValueError, match="Unknown filter key"):
        await server.latest("dispatch_price", filters={"bogus": "NSW1"})


async def test_list_curated_returns_seven():
    ids = server.list_curated()
    assert len(ids) == 7
    assert set(ids) == {
        "daily_summary",
        "dispatch_price",
        "dispatch_region",
        "generation_scada",
        "interconnector_flows",
        "predispatch_30min",
        "rooftop_pv",
    }


async def test_list_curated_sorted():
    ids = server.list_curated()
    assert ids == sorted(ids)


async def test_get_data_non_string_dataset_id():
    with pytest.raises(ValueError, match="must be a string"):
        await server.get_data(dataset_id=42)  # type: ignore[arg-type]


async def test_dataset_id_lowercase_normalized():
    """Mixed case dataset IDs must be normalised."""
    detail = await server.describe_dataset("Dispatch_Price")
    assert detail.id == "dispatch_price"


async def test_dataset_id_whitespace_trimmed():
    detail = await server.describe_dataset("  dispatch_price  ")
    assert detail.id == "dispatch_price"
