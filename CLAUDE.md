# aemo-mcp

Sister MCP in the Australian Public Data stack. See `../CLAUDE.md` for
portfolio-wide conventions; this file captures repo-specific details
plus the cross-sister discipline.

## Source

| | |
|--|--|
| Source agency | Australian Energy Market Operator (AEMO) |
| Source URL | http://nemweb.com.au/Reports/Current/ |
| Data format | Multi-section CSV (AEMO `C,/I,/D,` rows) packed in ZIP files, served from NEMWEB (IIS static file server). Directory listings are HTML. |
| Licence | AEMO Copyright Permissions (general permission for any purpose with attribution; commercial use allowed) |
| Licence URL | https://aemo.com.au/privacy-and-legal-notices/copyright-permissions |
| Python module | `aemo_mcp` |
| PyPI package | `aemo-mcp` |
| GitHub | https://github.com/Bigred97/aemo-mcp |

Note: AEMO's data is NOT CC-BY. Their Copyright Permissions policy is similar
in effect (general permission with attribution) but the canonical attribution
string differs — see `models._AEMO_ATTRIBUTION`.

## Curated datasets (9)

dispatch_price · dispatch_region · interconnector_flows · generation_scada ·
rooftop_pv · predispatch_30min · daily_summary · trading_price · fcas_prices

All cover the NEM (NSW1, QLD1, SA1, TAS1, VIC1). Western Australia (WEM) and
the Northern Territory are not on the NEM and are out of scope.

## Repo-specific module set

Required (every sister): `server.py`, `models.py`, `curated.py`, `client.py`, `cache.py`, `shaping.py`, `data/curated/*.yaml`

Repo-specific extras:
- `fetch.py` — orchestration layer between server.py and the HTTP/parsing
  stack. Resolves curated dataset → folder(s) → file selection (Current vs
  Archive) → ZIP fetch → CSV parse → filter → shape. Sits where most
  sisters' `server.py` would have inline orchestration logic — pulled out
  because NEM file selection (5-min vs 30-min, Current vs Archive, latest
  vs window, forecast vs actual) is non-trivial.
- `feeds.py` — dataset search ranking + DatasetSummary projection. Replaces
  rba-mcp's `tables.py` / abs-mcp's `catalog.py`.
- `parsing.py` — AEMO multi-section CSV parser + ZIP unpacker. Each NEMWEB
  ZIP holds one CSV with one or more `I,/D,` sections (DISPATCH.PRICE,
  DISPATCH.REGIONSUM, DISPATCH.INTERCONNECTORRES, ...). Stdlib `csv` +
  `zipfile` only; no pandas.
- `duid_lookup.py` — DUID → region/fuel join table for `generation_scada`.
  Static snapshot in `data/duid_snapshot.csv` (DUIDs change infrequently);
  refreshed periodically. Used to translate `region`/`fuel` filters into a
  DUID allow-set before filtering DISPATCH.UNIT_SCADA rows.

## Repo-specific gotchas

- **5-min cadence drives cache TTLs.** `live` = 60s, `half_hour` = 5min,
  `forecast` = 1h, `daily` = 24h, `archive` = 7d, `listing` = 30s. Timestamped
  NEMWEB files are immutable once written (filename embeds the interval), so
  the file-body cache is effectively infinite. Only the directory listing
  has freshness sensitivity.
- **AEMO market time is UTC+10, no DST.** NEM is Brisbane-aligned year-round.
  All NEMWEB timestamps in this code are tz-aware in NEM time (`NEM_TZ`).
- **`/Reports/Current/` vs `/Reports/Archive/`.** Current holds ~24-48h of
  5-min files; older intervals roll into daily ZIP-of-ZIPs compendia at
  `/Reports/Archive/<feed>/PUBLIC_<feed>_YYYYMMDD.zip`. `fetch.py` auto-pivots
  to Archive for windows older than `_CURRENT_WINDOW_HOURS` (4h). Archive
  fallback unpacks two ZIP levels.
- **In-flight request deduplication is mandatory.** At 5-min cadence with
  many concurrent users, naive caching would hammer NEMWEB. `AEMOClient._in_flight`
  shares one HTTP call across concurrent identical URLs.
- **Latest-file detection is purely lexicographic.** AEMO embeds the interval
  timestamp (`YYYYMMDDHHmm`) as the first 12-digit group in every filename,
  so `max(filenames)` is the most recent. No HEAD requests needed.
- **Forecast feeds use `latest()` differently.** For `rooftop_pv` forecast and
  `predispatch_30min`, `latest()` returns the FULL forward curve from the
  most-recent run, not a single collapsed row per dim. `_is_forecast_folder`
  controls this.
- **AEMO `C,/I,/D,` CSV format.** `C` = comment, `I` = schema row (opens a
  new section), `D` = data row. Section name is `col1.col2` of the I-row;
  data rows positionally map cells 4+ to the I-row's column names. One ZIP
  can hold many sections, and one file can hold the same section twice in
  two schema versions (`DREGION.` v2 + v3 in Daily_Reports) — `find_sections`
  returns all; the caller dedupes.
- **NEMWEB rolls files in/out continuously.** A filename present in the
  directory listing may have moved to `/Archive/` between the listing GET
  and the per-file GET. Individual 403/404 must NOT fail the whole response
  — `_fetch_current_zips` skips and continues.
- **`stale` field has dual meaning.** Set True if EITHER the latest observation
  is older than 2× the feed cadence (NEM-side delay) OR a cached-fallback was
  served because NEMWEB returned a non-2xx (graceful degradation). `stale_reason`
  disambiguates.

## Cache kinds (aemo-specific, not portable to other sisters)

```
live      60s   — 5-min dispatch feeds
half_hour 5min  — 30-min feeds: rooftop PV actual, predispatch
forecast  1h    — longer-horizon forecast bundles
daily     24h   — daily rolled-up archives
archive   7d    — immutable historical files (could be infinite)
listing   30s   — NEMWEB directory HTML
```

---

## The core 5-tool surface (uniform across sisters — mandatory)

The 5 below are the uniform brand. Additional tools (e.g. `top_n`, `stats`) are
allowed where the data shape genuinely needs them — they must use the same
`Annotated[Field]` discipline and `DataResponse` envelope as the core 5.

1. `search_datasets(query, limit)` — fuzzy-search the 7 curated NEM feeds
2. `describe_dataset(dataset_id)` — schema + filters + cadence + source URL
3. `get_data(dataset_id, filters, start_period, end_period, format)` — query
4. `latest(dataset_id, filters)` — most recent 5-min / 30-min / daily interval
5. `list_curated()` — enumerate supported IDs

Every parameter uses `Annotated[Type, Field(description=..., examples=[...])]`.
This is the Glama Tool Definition Quality requirement — non-negotiable.

## Trust contract (every DataResponse carries)

```
source             "Australian Energy Market Operator"
source_url         the NEMWEB folder the data came from
attribution        full AEMO Copyright Permissions attribution string
retrieved_at       UTC timestamp
server_version     importlib.metadata.version("aemo-mcp")
interval_start     ISO-8601 in AEMO market time (UTC+10)
interval_end       ISO-8601 in AEMO market time (UTC+10)
stale              True when the feed is delayed OR cached fallback was served
stale_reason       human-readable when stale=True (e.g. "AEMO/OpenNEM fetch returned 503 ...")
truncated_at       int | None — set when latest() caps a large response
```

## The 5 quality dimensions (audit every release against these)

1. **Semantic Clarity** — verb-noun tool names, Annotated[Field] with examples, rich docstrings (Examples + Returns blocks), `pattern=` constraints on dataset IDs and region codes
2. **Data Pruning** — <10k tokens for typical responses, `latest()` returns the most-recent interval(s) for the filter dims rather than the whole file, no leaked AEMO row metadata in observations
3. **Cross-Agency Joining** — AEMO market time uniformly UTC+10; region codes (NSW1/QLD1/SA1/TAS1/VIC1) match the canonical AEMO IDs that other sisters can join against; periods accept the shared YYYY / YYYY-MM / YYYY-MM-DD / YYYY-MM-DD HH:MM grammar
4. **Reliability + Caching** — TTLs tuned per AEMO cadence (60s / 5min / 1h / 24h / 7d), self-heal on `sqlite3.DatabaseError`, **graceful degradation**: when NEMWEB returns 5xx or is unreachable, fall back to last cached payload via `Cache.get_stale()` and set `stale=True, stale_reason="..."` rather than raising
5. **Deterministic Error Handling** — every `ValueError` carries a "Try X" / "Did you mean X?" / "Valid options: ..." hint that suggests the correction, not just describes the rejection

## Test taxonomy

Required: `test_cache.py`, `test_curated.py`, `test_server_validation.py`, `test_shaping.py`, `test_integration.py` (live, `@pytest.mark.live`)
Recommended: `test_client.py`, `test_fetch.py`, `test_feeds.py`, `test_parsing.py`, `test_duid_lookup.py`, `test_mcp_protocol.py`, `test_regressions.py`, `test_edge_cases.py`, `test_live.py`

Zero-flake bar: full unit suite must run 10× consecutively green before tagging a release.

## Release workflow (Trusted Publishing via OIDC, no API tokens in CI)

```
1. Bump version in pyproject.toml (semver)
2. Update CHANGELOG.md (latest entry at top, semver headings)
3. uv run pytest × 10 — zero flakes
4. git commit -am "X.Y.Z: <one-line reason>"
5. git tag -a vX.Y.Z -m "X.Y.Z: <reason>"
6. git push origin main vX.Y.Z
7. release.yml fires → builds → OIDC publish → PyPI
```

PyPI new-project rate limit: 5/day per account; not an issue for existing
projects (only counts NEW package names). `aemo-mcp` is already published.

## Anti-patterns — DO NOT do these

- Don't add tools that duplicate or rename the core 5; their names/shapes are fixed. Extras are allowed only where the data shape genuinely needs them (e.g. `top_n`, `stats`) and must follow the same `Annotated[Field]` + `DataResponse` discipline
- Don't add new top-level dependencies beyond what other sisters use (httpx, pydantic, fastmcp, aiosqlite, rapidfuzz, pyyaml)
- Don't introduce pandas at the parsing layer — stdlib `csv` + `zipfile` is enough
- Don't bundle large NEMWEB archives in the wheel; cache at runtime
- Don't ship without 10 consecutive zero-flake pytest runs
- Don't echo PyPI tokens / PATs in tool output, commit messages, or CHANGELOG
- Don't classify a slow source as a bug — NEMWEB cold fetches take 1-3s, only flag >10s or actual errors
- Don't hammer NEMWEB — in-flight dedup + cache are mandatory at 5-min cadence
- Don't widen scope mid-audit-loop; loops are fix-only

## Common operations

```bash
cd .                                                       # in the repo
uv sync --extra dev                                        # install deps
uv run pytest                                              # unit tests
uv run pytest -m live                                      # live tests too
uvx --refresh --from aemo-mcp==<ver> python -c "..."        # smoke a published wheel
```
