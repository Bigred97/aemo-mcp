"""Registry of curated NEM datasets + fuzzy search.

Replaces rba-mcp's `tables.py`. The 7 curated datasets are defined in
`data/curated/*.yaml`; this module loads them into `DatasetSummary` shapes
and runs fuzzy search across name + description + keywords + filter values.

Curated datasets all get +25 search score (every dataset is curated in v0 —
the boost is reserved for future non-curated additions).
"""
from __future__ import annotations

from importlib import resources
from pathlib import Path

from rapidfuzz import fuzz, process

from . import curated as curated_mod
from .models import DatasetSummary


def _data_dir() -> Path:
    """Locate aemo_mcp/data/ both during dev and after install."""
    try:
        ref = resources.files("aemo_mcp").joinpath("data")
        if ref.is_dir():
            return Path(str(ref))
    except (ModuleNotFoundError, AttributeError):
        pass
    here = Path(__file__).resolve().parent / "data"
    if here.is_dir():
        return here
    raise FileNotFoundError("Could not locate aemo_mcp/data/")


def list_datasets() -> list[DatasetSummary]:
    """Return all curated datasets as DatasetSummary objects."""
    out: list[DatasetSummary] = []
    for cd in curated_mod.list_all():
        # Build a rich description: name + summary + filter keys + region
        # values + keywords (repeated 3x to boost their fuzzy weight) so the
        # haystack catches "QLD" / "interconnector" / "negative price"
        # queries even when those terms only appear in filter metadata.
        keywords = " ".join(cd.search_keywords)
        weighted_keywords = " ".join([keywords] * 3)  # 3x weight
        filter_keys = " ".join(f.key for f in cd.filters)
        filter_values = " ".join(
            v for f in cd.filters for v in f.values
        )
        description = " ".join(
            filter(
                None,
                [cd.name, cd.description, weighted_keywords, filter_keys, filter_values],
            )
        )
        out.append(
            DatasetSummary(
                id=cd.id,
                name=cd.name,
                description=description,
                cadence=cd.cadence,
                is_curated=True,
            )
        )
    return out


def _keyword_match_bonus(
    summary: DatasetSummary, query: str
) -> int:
    """If the query matches any of the dataset's curated keywords, bonus."""
    cd = curated_mod.get(summary.id)
    if cd is None:
        return 0
    q = query.strip().lower()
    if not q:
        return 0
    BONUS_EXACT = 40    # exact keyword match
    BONUS_PARTIAL = 15  # query tokens overlap with keyword tokens
    for kw in cd.search_keywords:
        kw_l = kw.lower()
        if q == kw_l:
            return BONUS_EXACT
    # Partial: if every word in the query appears in some keyword
    q_tokens = set(q.split())
    if not q_tokens:
        return 0
    kw_text = " ".join(cd.search_keywords).lower()
    kw_tokens = set(kw_text.split())
    if q_tokens.issubset(kw_tokens):
        return BONUS_PARTIAL
    return 0


def get_dataset(dataset_id: str) -> DatasetSummary | None:
    norm = dataset_id.strip().lower()
    for s in list_datasets():
        if s.id.lower() == norm:
            return s
    return None


def search_in_memory(
    summaries: list[DatasetSummary], query: str, limit: int = 10
) -> list[DatasetSummary]:
    """Fuzzy-search a list of dataset summaries.

    Scoring layers (cumulative):
      - WRatio fuzzy score against the haystack (id + name + description +
        keywords + filter values)
      - +50 if the query matches the dataset ID exactly (with underscores
        normalised to spaces). Strongest signal — when the user types the
        canonical id verbatim they want THAT dataset.
      - +35 if the query phrase appears in the dataset NAME (not the longer
        description). This catches "dispatch price" → dispatch_price.
      - +20 if the query phrase appears anywhere in the haystack (the older
        "phrase-match bonus" from rba-mcp).
    """
    if not query.strip():
        raise ValueError(
            "query is required. Try 'spot price', 'demand', 'rooftop pv', "
            "'interconnector', or any other NEM topic."
        )
    haystack = {
        i: f"{s.id} {s.name} {s.description or ''}" for i, s in enumerate(summaries)
    }
    pool_size = max(limit * 4, 30)
    matches = process.extract(query, haystack, scorer=fuzz.WRatio, limit=pool_size)
    ID_BONUS = 50
    NAME_BONUS = 35
    PHRASE_BONUS = 20
    q_lower = query.strip().lower()
    q_normalised = q_lower.replace("_", " ").replace("-", " ")
    rescored = []
    for _hay, score, idx in matches:
        bonus = 0
        s = summaries[idx]
        id_normalised = s.id.lower().replace("_", " ")
        if q_normalised == id_normalised or q_lower == s.id.lower():
            bonus += ID_BONUS
        if q_lower and q_lower in s.name.lower():
            bonus += NAME_BONUS
        elif q_normalised and q_normalised in s.name.lower():
            bonus += NAME_BONUS
        if q_lower and q_lower in haystack[idx].lower():
            bonus += PHRASE_BONUS
        bonus += _keyword_match_bonus(s, query)
        rescored.append((score + bonus, score, idx))
    rescored.sort(key=lambda t: (-t[0], -t[1]))
    return [summaries[idx] for _adj, _score, idx in rescored[:limit]]


def search_datasets(query: str, limit: int = 10) -> list[DatasetSummary]:
    return search_in_memory(list_datasets(), query, limit)
