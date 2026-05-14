# Changelog

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
