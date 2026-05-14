"""FastMCP tool registration tests — verifies the MCP surface is wired correctly."""
from __future__ import annotations

import pytest


def test_server_module_imports():
    from aemo_mcp import server
    assert server.mcp is not None
    assert server.mcp.name == "aemo-mcp"


def test_search_datasets_is_registered():
    from aemo_mcp.server import search_datasets
    # FastMCP wraps the function; the original is callable.
    assert callable(search_datasets)


def test_describe_dataset_is_registered():
    from aemo_mcp.server import describe_dataset
    assert callable(describe_dataset)


def test_get_data_is_registered():
    from aemo_mcp.server import get_data
    assert callable(get_data)


def test_latest_is_registered():
    from aemo_mcp.server import latest
    assert callable(latest)


def test_list_curated_is_registered():
    from aemo_mcp.server import list_curated
    assert callable(list_curated)


def test_main_entrypoint_exists():
    from aemo_mcp.server import main
    assert callable(main)


async def test_list_curated_callable_async_safe():
    """list_curated is synchronous on this server (mirrors abs/rba)."""
    from aemo_mcp.server import list_curated
    out = list_curated()
    assert isinstance(out, list)
    assert len(out) == 7


def test_version_string_exposed():
    import aemo_mcp
    assert aemo_mcp.__version__ != ""


def test_dataset_summary_pydantic_shape():
    from aemo_mcp.models import DatasetSummary
    s = DatasetSummary(id="x", name="n", cadence="5 min")
    assert s.is_curated is True


def test_dataresponse_default_source():
    from aemo_mcp.models import DataResponse
    from datetime import datetime, timezone
    r = DataResponse(
        dataset_id="x", dataset_name="X",
        source_url="http://x/",
        retrieved_at=datetime.now(timezone.utc),
    )
    assert r.source == "Australian Energy Market Operator"
    assert "AEMO" in r.attribution


def test_observation_pydantic_shape():
    from aemo_mcp.models import Observation
    o = Observation(period="t", value=1.0, dimensions={"x": "y"}, unit="MW")
    assert o.value == 1.0
