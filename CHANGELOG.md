# Changelog

## [0.4.8] - 2026-05-17

### Improved — transport-agnostic Field descriptions

Three `Field(description=...)` strings in `server.py` referenced
MCP-tool-name (`search_datasets()`, `list_curated()`). These
descriptions become part of the parameter schema, so REST-gateway
customers hitting `/v1/{describe,get-data,latest}/...` saw "Use
search_datasets() to discover" — confusing because they're not calling
a Python function. Rewrote to the "{endpoint or tool}" form. Matches
the ato 0.8.7 / rba 0.7.5 portfolio guard. No runtime behaviour change.

## [0.4.7] - 2026-05-17

### Fixed — silent zero-result failures + missing `stale_reason`

A friction audit found four real bugs that left agents misreading a
"no signal" response as legitimate empty data:

1. **Invalid filter values silently returned 0 records** — e.g.
   `filters={"region": "NSW"}` (missing the `1` suffix),
   `filters={"fuel": "coal"}` (vs `black_coal` / `brown_coal`),
   `filters={"interconnector": "Basslink"}` (vs the formal `T-V-MNSP1`),
   `filters={"region": "WA1"}` (WEM, not on the NEM). All four returned
   `[]` with no error. Agents read the empty list as "no NEM data" and
   moved on. `server._check_filter_values` now validates every supplied
   filter value against the curated `values` enumeration (case-insensitive),
   raising `ValueError` with a `Did you mean 'X'?` hint plus the full
   valid list inlined. Filters with empty `values` (open-ended like DUID)
   stay permissive.

2. **`stale=True` with `stale_reason=None`** — empty / future-period queries
   set `stale=True` but left the reason unset, so agents saw the flag
   without an explanation. `shaping.build_response` now always populates
   `stale_reason` whenever `stale=True`: distinct messages for the
   no-data branch (suggesting future period / retention / over-filtering)
   and the cadence-delay branch (quoting the latest observation timestamp).
   The cached-fallback path in `server._fetch_with_stale_signal` was
   already setting the reason and is unchanged.

3. **Wide-window queries silently truncated at the 31-day archive cap** —
   `get_data(start_period="2026-01-01", end_period="2026-12-31")` returned
   only Jan 1 – Feb 1 of data but reported `interval_end="2026-12-31"`,
   `truncated_at=None`, and `stale_reason=None`. Customers thought they
   got the whole year. `shaping.build_response` now compares the user's
   requested `end_period` against the actual `interval_end`; when the
   request is wider, it sets `stale=True` and populates `stale_reason`
   with "Returned data covers X to Y; your request was A to B. NEMWEB
   archive fetches cap at ~31 days per call — narrow the window for
   the full range." `interval_start/end` semantics are unchanged
   (preserves backwards compat).

4. **"All 7 IDs" in error messages** — three error sites in `server.py`
   hard-coded the dataset count at 7, but the portfolio is now 10.
   Switched to `len(ids)` so the count stays accurate as the registry
   grows.

### Deferred (audit polish, not bugs)

- "Use ..." → "Try ..." wording consistency across `_validate_period`
- `AEMOParseError` retry hints
- Basslink / Heywood → formal interconnector ID auto-resolution (feature,
  not a fix — `Basslink` now raises with the valid list, so the agent
  can self-correct)
- `duid_snapshot.csv` audit beyond known errors — see spawned follow-up

### Tests

- 8 new regression tests pinning each of the four bugs above (3 for
  filter-value validation, 2 for `stale_reason`, 1 for the wide-window
  signal, 1 for dynamic dataset count, plus 1 for case-insensitivity
  preserved). 323 unit tests, 10x zero-flake.

## [0.4.6] - 2026-05-17

### Changed — NEMWEB base URL hardened to `https://www.nemweb.com.au`

`client.DEFAULT_BASE_URL` and every curated YAML's `source_url` now point at
the canonical HTTPS host. NEMWEB has long served both `http://nemweb.com.au`
and `https://www.nemweb.com.au` but only the latter carries a valid cert
chain and survives modern HSTS preload — agents that block plain-http
fetches stop seeing `SSLError` / `MaxRetryError` on first call.

### Fixed — `generation_scada` region/fuel dims stamped DUID instead of resolving

Bug repro at 0.4.5:
```
latest(dataset_id='generation_scada') →
  records[0].dimensions = {
    'duid': 'BW01', 'region': 'BW01', 'fuel': 'BW01', 'metric': 'scada_mw'
  }
```

`DISPATCH.UNIT_SCADA` only carries `DUID` + `SCADAVALUE`, so the region and
fuel filter columns in `generation_scada.yaml` aliased to `column: DUID`.
At filter time we joined against the bundled DUID master to translate
`region`/`fuel` into a DUID allow-set (already worked since 0.1.0). But
at shaping time the same alias caused `shaping.records_to_observations`
to stamp the raw DUID value into the region + fuel dims — customers
couldn't group or join on either dimension even though `describe_dataset`
advertised both as filterable.

- `CuratedFilter` now carries an optional `lookup: str | None` field.
  YAML filters set `lookup: duid_to_region` or `lookup: duid_to_fuel`
  when the dim should be resolved via `duid_lookup.duid_info()` rather
  than read verbatim from the row column.
- `shaping._resolve_dim_value()` is the single place that consults the
  lookup table — `records_to_observations` calls it for every filter
  the dataset declares.
- `generation_scada.yaml`: region and fuel filters now declare their
  lookup. Other curated datasets are unchanged (no DUID-join semantics).
- Unknown DUIDs (newer units not yet in the bundled snapshot) resolve
  to `None` — the dim is omitted from the observation rather than
  stamped with the DUID code. Customer code checking
  `'region' in dimensions` gets the truth.
- Wheel size unchanged — no new bundled fixtures. The fix is logic-only
  on top of the existing `data/duid_snapshot.csv`.

### Tests

- `test_shaping.py`: 5 new tests covering `_resolve_dim_value` pass-through,
  DUID-to-region/fuel lookup, unknown-DUID omission, and missing columns.
- `test_regressions.py`: 3 new tests pinning the bug — region/fuel must
  resolve via the lookup for known DUIDs (BW01, LY_W1, COOPGWF1, HPRG1),
  unknown DUIDs must not stamp the DUID code into region/fuel, and
  `describe_dataset('generation_scada')` continues to advertise the
  filterable values.
- 315 unit tests, 10x zero-flake.

## [0.4.5] - 2026-05-16

### Verified — no text-field bloat (portfolio playbook item #5)

AEMO data is numeric — RRP, demand, SCADA megawatts, mwflow — and the
non-numeric fields are short codes (region IDs are 4 chars; DUIDs cap at
12 chars; CONSTRAINTID names cap around 30 chars; period timestamps are 25
chars ISO-8601). Spot-check across `dispatch_price`, `dispatch_region`,
`dispatch_constraints` confirmed max observation field length is 25
characters — well under the 200-char "consider capping" threshold the
playbook flags. No `cap_long_text` / `_truncate_field` helper needed.

### Skipped — playbook item #6 (default-series ambiguity)

AEMO datasets all carry explicit `dataset_id` discrimination (no
default-series fallback semantics like rba `latest()` had); item #6
is N/A for this sister.

## [0.4.4] - 2026-05-16

### Changed — sanitise user-facing error and schema strings

Portfolio playbook item #3: strip implementation details from the strings
agents see when something goes wrong.

- `get_data` / `latest` AEMOAPIError message no longer name-drops the
  `/Reports/Current/` NEMWEB path — replaced with the agent-actionable
  "rolls files between current and archive" wording.
- `_check_filter_keys` no longer suggests `Try describe_dataset('X')` —
  the error now points to the valid-filters list directly so the agent
  can correct itself without an extra tool call.
- `get_data.filters` `Field(description=...)` no longer references
  `describe_dataset(dataset_id)` by tool name. The filter-key surface is
  described in terms of the dataset's own metadata.
- `_fetch_current_zips` FetchError no longer leaks the internal
  `folder.path` (e.g. `/Reports/Current/DispatchIS_Reports/`) — it
  reports "Could not list NEMWEB directory for this dataset" instead.

### Tests

- `test_unknown_filter_key_suggests_close_match` updated to assert the
  new wording. Other 306 tests unchanged. 307 unit tests, 10x zero-flake.
- Source comments + dataclass docstrings (e.g. `# e.g. "DISPATCH.PRICE"`)
  retain structural references — those are not user-facing.

## [0.4.3] - 2026-05-16

### Performance — streaming row filter for high-cadence feeds

The 5-minute NEM dispatch feeds are the highest-volume datasets in the
portfolio. Pre-0.4.3, `fetch_dataset` would call `parse_csv` to materialise
the full sections list (including DISPATCH.REGIONSUM + INTERCONNECTORRES
sections that `dispatch_price` doesn't even need) before filtering rows in
Python. A multi-day archive window would spike peak RSS above 100MB.

- Added `parsing.iter_csv_rows(body, target_section=...)` — streaming
  iterator that yields one `(section_name, version, row_dict)` tuple at a
  time without ever holding the full sections list in memory. `target_section`
  short-circuits D-row construction for unwanted sections.
- Added `fetch._stream_filtered_rows` — applies row filter + period bounds +
  resolved-DUID allow-set inline so peak memory is O(keepers), not O(file
  rows).
- Wired the high-cadence single-section feeds (`dispatch_price`,
  `dispatch_region`, `interconnector_flows`, `generation_scada`,
  `dispatch_constraints`, `trading_price`, `fcas_prices`) through the
  streaming path. The eager `parse_csv` branch is retained for daily
  archives (DREGION. v2+v3 dedup) and multi-section folders (predispatch
  fan-out).

### Tests

- New `tests/test_resilience.py` (6 tests) with explicit time + peak-memory
  bounds, mocked NEMWEB via respx. Empirical numbers on the dev machine:
  - `latest('dispatch_price', filters={'region': 'NSW1'})`:
    140ms / 2.1MB peak (bound: <3s / <50MB).
  - `get_data('generation_scada', filters={'region': 'NSW1'}, period=10min)`:
    940ms / 9.7MB peak (bound: <10s / <100MB).
- 307 unit tests passing (was 301).

## [0.4.2] - 2026-05-16

### Fixed — JSON-string `filters` parameter (portfolio-wide)

The MCP protocol JSON-encodes dict parameters before they reach the
server. `_validate_filters` was checking `isinstance(filters, dict)`
before parsing the JSON string, so every call of the form
`get_data(filters={"region":"NSW1"})` from a real MCP client was
rejected. Fix: decode JSON-string filters before the type check.
Coordinated patch across the portfolio (abs 0.9.2, ato 0.8.2, apra 0.8.2,
asic 0.6.1, aihw 0.4.2, wgea 0.5.1, aemo 0.4.2).

## [0.4.1] - 2026-05-16

### Added
- `DataResponse.row_count`: number of observation rows. Closes the
  last portfolio-uniformity gap — every sister's DataResponse now
  carries the canonical row_count field.

## [0.4.0] - 2026-05-16

### Added — dispatch_constraints (DISPATCH.CONSTRAINT)

- **`dispatch_constraints` curated dataset.** 5-minute snapshot of every
  active network/security constraint the AEMO dispatch engine evaluates.
  Each interval reports ~200-1000 constraints; most are non-binding
  (marginal_value = 0). The few binding ones are what drive intra-NEM
  price separation and regional price spikes.
- Closes the audit gap on "why did the price spike?" — energy traders,
  retail desks, renewable developers (curtailment tracking), network
  planners and consultants explaining price events can now query the
  shadow-price data directly.
- Filters: `constraint_id` (substring matching against names like
  `C_V::N_NIL_RB` or `F_I+NIL_APD_TL_L60`), and `duid` for
  generator-specific constraints.
- Metrics: `rhs` (RHS limit), `marginal_value` (shadow price $/MW — the
  headline), `violation_degree` (extent of constraint violation),
  `lhs` (LHS computed value).
- Source: NEMWEB `DispatchIS_Reports`, same folder as `dispatch_price`,
  section `DISPATCH.CONSTRAINT` (13 columns). Uses the existing
  AEMO multi-section ZIP+CSV parser — no new code, YAML-only addition.

### Customer-value validation (live NEMWEB fetch, 2026-05-16 14:10)

- Latest 5-min interval: 982 active constraints × 4 metrics = 3,928
  observations. 18 constraints binding (marginal_value ≠ 0).
- Binding examples in this interval: `F_I+BIP_ML_L1`, `F_I+NIL_APD_TL_L5`,
  `F_I+NIL_MG_R1` — all FCAS-related with small shadow prices ($0.01-
  0.03/MW).
- Search routing: "dispatch constraint", "binding constraint",
  "shadow price", "price spike", "qni constraint" all hit
  `dispatch_constraints` at #1.

### Tests

- 300 unit tests passing (was 300). 10× zero-flake gauntlet. Ruff clean.
- 9→10 count assertions updated across test_curated, test_feeds,
  test_edge_cases, test_mcp_protocol, test_server_validation, test_live.

## [0.3.1] - 2026-05-16

### Fixed

- `test_live_list_curated` updated to expect 9 datasets (was 7).
- CLAUDE.md curated dataset list updated to all 9 NEM feeds.

## [0.3.0] - 2026-05-16

### Added

- **`trading_price` dataset**: 30-minute NEM regional reference price from
  TradingIS (TRADING.PRICE section). Distinct from the 5-minute dispatch price
  (`dispatch_price`) — this is the settlement price used for financial
  settlement of generators and retailers. Also exposes FCAS contingency and
  regulation prices at the trading interval for all 8 FCAS services.
- **`fcas_prices` dataset**: daily volume-weighted average (VWA) prices for all
  8 FCAS markets (raise/lower × 6-second/60-second/5-minute/regulation) from
  Vwa_Fcas_Prices (TRADING.VWAFCASPRICES). Includes cleared volume and revenue
  per service. Published once daily for the prior trading day.

## [0.2.0] - 2026-05-15

### Added
- **DataResponse.period**: canonical `{"start", "end"}` dict populated alongside the
  aemo-specific `interval_start` / `interval_end`. Cross-sister consumers can now read
  `resp.period["start"]` / `resp.period["end"]` uniformly across the Australian Public
  Data MCP portfolio. The legacy `interval_start` / `interval_end` fields are preserved
  unchanged.

## 0.1.2 (2026-05-15)

Portfolio parity — stale-cache fallback + error-message sweep + dependabot +
CLAUDE.md. 297 unit tests (was 288) + 10 live tests, 3× zero-flake green.

- **Add: stale-cache fallback (graceful degradation).** When NEMWEB returns
  5xx or is unreachable (`httpx.RequestError`), `AEMOClient._fetch_cached`
  now falls back to the most-recent cached payload (regardless of TTL) via
  the new `Cache.get_stale()` and records the staleness on a `_stale_signal`
  ContextVar. Server-side tool wrappers (`get_data`, `latest`) read the
  signal after the fetch chain and surface it on the response via
  `DataResponse.stale=True, stale_reason="AEMO/OpenNEM fetch returned X
  for Y; serving cached payload from ~N minute(s) ago"`. Mirrors abs-mcp
  0.2.13 / rba-mcp 0.1.10 patterns. Empty-cache fallback preserves the
  original `AEMOAPIError` behaviour.
- **Add: `DataResponse.stale_reason` and `truncated_at` fields.** Aligns the
  envelope with the rest of the portfolio (abs / rba / ato / apra / aihw /
  asic). `stale` retains its dual meaning: True when the latest NEM
  observation is older than 2x cadence OR a cached fallback was served.
- **Error-message sweep.** Every weak `ValueError` rewritten to suggest the
  correction via stdlib `difflib.get_close_matches`. Unknown dataset IDs
  now emit `Did you mean 'dispatch_price'?` for close typos; unknown filter
  keys emit `Did you mean 'region'?`; unknown formats emit `Did you mean
  'records'?`. Period errors now show a worked example
  (`'2026-05-14' or '2026-05-14 09:00'`). `Unsupported aggregation
  dimension` in duid_lookup now lists valid options. No new top-level
  dependencies.
- **Add: `CLAUDE.md`.** Repo-specific conventions auto-loaded by Claude
  Code, mirroring the rest of the portfolio. Calls out the AEMO-specific
  module set (`fetch.py`, `feeds.py`, `parsing.py`, `duid_lookup.py`),
  the dual-meaning `stale` flag, the 5-min cadence cache-TTL ladder, and
  the `/Reports/Current/` vs `/Reports/Archive/` pivot.
- **Add: `.github/dependabot.yml`.** Weekly minor + patch update PRs for
  pip + GitHub Actions, grouped, Mon 10:00 Sydney. Verbatim from the
  sister repos.
- **Tests: +4 stale-fallback regressions + 5 error-message-suggestion
  regressions.** New tests cover: 5xx + stale cache → fallback + signal;
  ConnectError + stale cache → same; 5xx + empty cache → still raises
  `AEMOAPIError`; `Cache.get_stale()` round-trip + TTL bypass; "Did you
  mean" hint for dataset / filter-key / format typos; period worked
  example.

## 0.1.1 (2026-05-15)

Customer-simulation hardening pass. Hammered every dataset against the live
NEMWEB feed; fixed every bug surfaced. 288 unit tests (was 225) + 10 live
tests, 10× zero-flake green.

- **Fix: `daily_summary` section name** — Daily_Reports publishes regional
  data under `DREGION.` (with trailing dot — second cell of I-row is empty),
  not `DISPATCH.PRICE`. YAML updated + 4 metrics (rrp, total_demand,
  dispatchable_generation, net_interchange) now resolve correctly. The
  parser already builds the empty-subname name; added a regression test.
- **Fix: `predispatch_30min` section names** — the actual NEMWEB sections
  are `PREDISPATCH.REGION_SOLUTION` (demand/generation) and
  `PREDISPATCH.REGION_PRICES` (RRP); the YAML pointed at the non-existent
  `PREDISPATCH.REGIONSUM`. Forward curves now return 135+ records per
  region per run (instead of 1).
- **Fix: `rooftop_pv` filename regex** — AEMO renamed the ACTUAL infix
  from `MEASUREMENT` to `SATELLITE` and now also publishes some files
  with no infix at all. Regex accepts all three forms.
- **Fix: archive fallback** — added `/Reports/Archive/<feed>/` (one ZIP-of-
  ZIPs per day) fetch for windows older than 4 hours. Daily archive zips
  are unpacked recursively. Demo 3 ("did SA hit negative pricing in the
  last 24h?") and Demo 4 ("weekly avg dispatch price for VIC, last 4
  weeks") now succeed where they previously hit 403 on rolled-out
  /Current/ files. Capped at 31 days per response.
- **Fix: archive path** — the path constructor was including the literal
  `Current` segment, producing `/Reports/Archive/Current/...` instead of
  `/Reports/Archive/...`. Now strips correctly.
- **Fix: 5-min feeds: skip rolled-out files instead of failing the whole
  response** — NEMWEB rolls files in/out of /Current/ continuously; a
  file present in the directory listing may have moved to /Archive/ by
  the time we GET it. Individual 403/404s now skip silently; only a
  fully-empty result surfaces an error.
- **Fix: `latest()` on forecast feeds returns the full forward curve**,
  not a single row collapsed to the furthest-out horizon. `rooftop_pv`
  forecast and `predispatch_30min` now behave correctly.
- **Fix: section filter at folder level** — `filters={"section": "actual"}`
  now skips fetching the FORECAST folder entirely, cutting one HTTP round
  trip + sidestepping flaky listings.
- **Fix: section filter row-level skip** — the synthesised `section`
  filter is no longer treated as a row column (which would reject every
  row since rows have no SECTION cell).
- **Fix: section dedup** — when AEMO emits the same section twice in one
  file with different versions (e.g. `DREGION.` v2 + v3 in Daily_Reports),
  we now combine and dedupe by (settlement_column, filter columns)
  instead of taking only the first match.
- **Fix: `Cache(db_path=DEFAULT_DB_PATH)` honors monkeypatches** — the
  default value was captured at class-definition time, so test
  monkeypatches of `DEFAULT_DB_PATH` had no effect. Now resolved at
  construction time. Fixes flaky integration test where live NEMWEB data
  bled through respx mocks.
- **Fix: in-flight dedup future exception leaks** — failed fetches no
  longer log "Future exception was never retrieved" warnings.
- **Expand: DUID snapshot from 128 → 350 entries**, covering the majority
  of active NEM units across all 5 regions and 7 fuel buckets. Generation-
  by-fuel queries (QLD gas, NSW solar, SA battery, etc.) now return non-
  empty results.
- **Tests: +63 regressions + edge cases.** Tests now cover: every bug above
  + unicode queries, very long queries, special-char queries, negative
  TTL, concurrent 10x dedup, DOS line endings, truncated CSV, quoted
  commas, ZIP-bomb defence, every-dataset describe sweep, fuzzy ranker
  invariants, and DUID coverage thresholds per fuel/region.

## 0.1.0 (2026-05-14)

Initial release. MCP server wrapping AEMO NEMWEB feeds with 5 plain-English
tools and 7 curated datasets.

- **5 tools** mirroring abs-mcp / rba-mcp / ato-mcp: `search_datasets`,
  `describe_dataset`, `get_data`, `latest`, `list_curated`.
- **7 curated feeds** covering ~95% of typical NEM analytic queries:
  - `dispatch_price` — 5-min regional spot price (RRP) per NEM region
  - `dispatch_region` — 5-min total demand, scheduled + semi-scheduled generation, net interchange
  - `interconnector_flows` — 5-min MW flow across the 6 NEM interconnectors
  - `generation_scada` — 5-min DUID-level SCADA MW (every generating unit)
  - `rooftop_pv` — 30-min regional rooftop solar (actual + forecast)
  - `predispatch_30min` — 30-min half-hourly forecast, ~40h horizon
  - `daily_summary` — daily rolled-up compendium of yesterday's price + demand + dispatch
- **Trust contract** on every `DataResponse`: `source`, `attribution`,
  `source_url`, `retrieved_at`, `interval_start`, `interval_end`, `stale`.
- **Live-fetch only**, no pre-bundled NEMWEB archives in the wheel.
- **Cache TTLs tuned per cadence**: 60s for 5-min feeds, 5min for 30-min
  feeds, 1h forecasts, 24h daily, immutable for archived timestamped files.
- **In-flight request deduplication** — concurrent callers share one HTTP
  request per URL (critical at 5-min cadence with many users).
- **AEMO Copyright Permissions** attribution string in every response.

Licence: AEMO grants general permission to use AEMO Material for any purpose
with attribution. See https://aemo.com.au/privacy-and-legal-notices/copyright-permissions.
