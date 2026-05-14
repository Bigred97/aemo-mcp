"""Hand-curated metadata for the 7 NEM datasets.

Each YAML in `data/curated/` describes one dataset:
- backend folder + filename pattern (regex)
- multi-section CSV section name(s) to extract
- filter dimensions (region, interconnector, metric, ...)
- cadence + cache TTL kind
- units + plain-English search keywords

Loaded once at import time and stored in a frozen dataclass registry.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import yaml

from .cache import CacheKind
from .models import DatasetDetail, DatasetFilter


@dataclass(frozen=True)
class CuratedFilter:
    key: str
    description: str
    values: tuple[str, ...] = ()
    required: bool = False
    # Maps user-facing filter values → CSV row predicates. Each predicate is
    # a dict {column: value}. Most filters map 1:1 to a single column.
    column: str | None = None


@dataclass(frozen=True)
class CuratedMetric:
    """One numeric metric extracted from the CSV section."""
    key: str                                # response-side key (e.g. "rrp")
    source_column: str                      # column header in the CSV section
    description: str                        # plain-English
    unit: str                               # e.g. "$/MWh"


@dataclass(frozen=True)
class CuratedSection:
    """One CSV section to extract from the fetched ZIP."""
    name: str                               # e.g. "DISPATCH.PRICE"
    # When a single dataset stitches together multiple sections (eg.
    # `rooftop_pv` combines ACTUAL + FORECAST from two different folders),
    # `discriminator` distinguishes them in the output dimensions.
    discriminator: str | None = None


@dataclass(frozen=True)
class CuratedFolder:
    """One NEMWEB folder this dataset fetches from."""
    path: str                               # e.g. "/Reports/Current/DispatchIS_Reports/"
    filename_regex: str                     # e.g. "PUBLIC_DISPATCHIS_.*\\.zip"
    sections: tuple[CuratedSection, ...] = ()
    discriminator: str | None = None         # see CuratedSection.discriminator


@dataclass(frozen=True)
class CuratedDataset:
    id: str
    name: str
    description: str
    cadence: str                            # plain-English: "5 min" / "30 min" / "Daily"
    cache_kind: CacheKind
    folders: tuple[CuratedFolder, ...]
    filters: tuple[CuratedFilter, ...] = ()
    metrics: tuple[CuratedMetric, ...] = ()
    settlement_column: str = "SETTLEMENTDATE"  # AEMO-standard period column
    source_url: str = "http://nemweb.com.au/Reports/Current/"
    search_keywords: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()

    def to_detail(self) -> DatasetDetail:
        return DatasetDetail(
            id=self.id,
            name=self.name,
            description=self.description,
            is_curated=True,
            cadence=self.cadence,
            filters=[
                DatasetFilter(
                    key=f.key,
                    description=f.description,
                    values=list(f.values),
                    required=f.required,
                )
                for f in self.filters
            ],
            units={m.key: m.unit for m in self.metrics},
            source_url=self.source_url,
            examples=list(self.examples),
        )

    def get_filter(self, key: str) -> CuratedFilter | None:
        for f in self.filters:
            if f.key == key:
                return f
        return None

    def get_metric(self, key: str) -> CuratedMetric | None:
        for m in self.metrics:
            if m.key == key:
                return m
        return None


_REGISTRY: dict[str, CuratedDataset] | None = None


def _yaml_dir() -> Path:
    try:
        ref = resources.files("aemo_mcp").joinpath("data/curated")
        if ref.is_dir():
            return Path(str(ref))
    except (ModuleNotFoundError, AttributeError):
        pass
    here = Path(__file__).resolve().parent / "data" / "curated"
    if here.is_dir():
        return here
    raise FileNotFoundError("Could not locate aemo_mcp/data/curated/")


def _parse_filter(raw: dict[str, Any]) -> CuratedFilter:
    return CuratedFilter(
        key=str(raw["key"]),
        description=str(raw.get("description", "")),
        values=tuple(raw.get("values") or ()),
        required=bool(raw.get("required", False)),
        column=raw.get("column"),
    )


def _parse_metric(raw: dict[str, Any]) -> CuratedMetric:
    return CuratedMetric(
        key=str(raw["key"]),
        source_column=str(raw["source_column"]),
        description=str(raw.get("description", "")),
        unit=str(raw.get("unit", "")),
    )


def _parse_section(raw: dict[str, Any] | str) -> CuratedSection:
    if isinstance(raw, str):
        return CuratedSection(name=raw)
    return CuratedSection(
        name=str(raw["name"]),
        discriminator=raw.get("discriminator"),
    )


def _parse_folder(raw: dict[str, Any]) -> CuratedFolder:
    sections_raw = raw.get("sections") or []
    sections = tuple(_parse_section(s) for s in sections_raw)
    return CuratedFolder(
        path=str(raw["path"]),
        filename_regex=str(raw["filename_regex"]),
        sections=sections,
        discriminator=raw.get("discriminator"),
    )


def _load_one(path: Path) -> CuratedDataset:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    folders = tuple(_parse_folder(f) for f in (raw.get("folders") or []))
    if not folders:
        raise ValueError(f"Curated YAML {path.name} has no folders")
    filters = tuple(_parse_filter(f) for f in (raw.get("filters") or []))
    metrics = tuple(_parse_metric(m) for m in (raw.get("metrics") or []))
    return CuratedDataset(
        id=str(raw["id"]),
        name=str(raw["name"]),
        description=str(raw.get("description", "")),
        cadence=str(raw.get("cadence", "")),
        cache_kind=str(raw.get("cache_kind", "live")),  # type: ignore[arg-type]
        folders=folders,
        filters=filters,
        metrics=metrics,
        settlement_column=str(raw.get("settlement_column", "SETTLEMENTDATE")),
        source_url=str(raw.get("source_url", "http://nemweb.com.au/Reports/Current/")),
        search_keywords=tuple(raw.get("search_keywords") or ()),
        examples=tuple(raw.get("examples") or ()),
    )


def _load_all() -> dict[str, CuratedDataset]:
    out: dict[str, CuratedDataset] = {}
    for path in sorted(_yaml_dir().glob("*.yaml")):
        cd = _load_one(path)
        out[cd.id] = cd
    return out


def _registry() -> dict[str, CuratedDataset]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _load_all()
    return _REGISTRY


def get(dataset_id: str) -> CuratedDataset | None:
    return _registry().get(dataset_id.strip().lower())


def list_ids() -> list[str]:
    return sorted(_registry().keys())


def list_all() -> list[CuratedDataset]:
    return [_registry()[k] for k in list_ids()]


def reset_registry() -> None:
    global _REGISTRY
    _REGISTRY = None


def compile_filename_regex(folder: CuratedFolder) -> re.Pattern[str]:
    """Compiled regex for filenames in this folder (cached on the dataclass would be nicer but it's frozen)."""
    return re.compile(folder.filename_regex)
