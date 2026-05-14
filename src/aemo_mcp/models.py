"""Pydantic v2 response models for aemo-mcp.

Mirrors the rba-mcp / abs-mcp / ato-mcp shape so a downstream agent that uses
multiple sibling MCPs gets a uniform response. AEMO-specific additions:
- DatasetSummary, DatasetDetail, DatasetFilter (NEM-flavoured dataset metadata)
- DataResponse.interval_start / interval_end (5-min cadence makes these
  much more salient than "period start/end" for daily-cadence siblings)
- DataResponse.stale (True if latest interval is older than 2x cadence)
- AEMO Copyright Permissions attribution string on every response.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# Verbatim attribution per RESEARCH.md / AEMO Copyright Permissions
_AEMO_ATTRIBUTION = (
    "Source: Australian Energy Market Operator (AEMO), NEMWEB. "
    "Used under AEMO's Copyright Permissions (general permission for any "
    "purpose with accurate attribution). "
    "https://aemo.com.au/privacy-and-legal-notices/copyright-permissions"
)


class DatasetFilter(BaseModel):
    """One filter dimension on a dataset (e.g. region, interconnector, fuel)."""
    key: str                                 # filter key, e.g. "region"
    description: str                         # plain-English description
    values: list[str] = Field(default_factory=list)  # allowed values
    required: bool = False


class DatasetSummary(BaseModel):
    """Compact entry returned by search_datasets()."""
    id: str                                  # dataset_id, e.g. "dispatch_price"
    name: str                                # plain-English name
    description: str | None = None
    cadence: str | None = None               # "5 min" / "30 min" / "Daily"
    is_curated: bool = True                  # always True for v0 — all 7 are curated


class DatasetDetail(BaseModel):
    """Full dataset metadata returned by describe_dataset()."""
    id: str
    name: str
    description: str
    is_curated: bool
    cadence: str | None = None
    filters: list[DatasetFilter] = Field(default_factory=list)
    units: dict[str, str] = Field(default_factory=dict)   # metric → unit
    source_url: str
    examples: list[str] = Field(default_factory=list)


class Observation(BaseModel):
    """One observation: a (period, dimensions) → value tuple."""
    period: str                              # ISO-8601 datetime in AEMO market time
    value: float | None
    dimensions: dict[str, str]               # e.g. {"region": "NSW1", "metric": "rrp"}
    unit: str | None = None


class DataResponse(BaseModel):
    """Uniform response shape across all sibling MCPs."""
    dataset_id: str
    dataset_name: str
    query: dict[str, Any] = Field(default_factory=dict)
    interval_start: str | None = None       # ISO-8601, AEMO market time (UTC+10)
    interval_end: str | None = None         # ISO-8601, AEMO market time (UTC+10)
    unit: str | None = None
    records: list[Observation] | list[dict[str, Any]] = Field(default_factory=list)
    csv: str | None = None
    source: str = "Australian Energy Market Operator"
    attribution: str = _AEMO_ATTRIBUTION
    source_url: str
    retrieved_at: datetime
    # True if the most recent observation is older than 2x the feed cadence —
    # signal to the LLM that NEMWEB is delayed or the feed has stopped.
    stale: bool = False
    server_version: str = Field(default_factory=lambda: _get_server_version())


def _get_server_version() -> str:
    try:
        from importlib.metadata import version
        return version("aemo-mcp")
    except Exception:
        return "0.0.0+unknown"
