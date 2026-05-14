# aemo-mcp — Phase 1 Research

**Status:** Phase 1 complete. **PAUSED for review** before any code.
**Date:** 2026-05-14
**Author:** Harry Vass

---

## TL;DR — Build verdict

**GREENLIGHT.** AEMO's Copyright Permissions policy grants general permission to use NEMWEB content "for any purpose" (commercial included), with attribution as the sole condition. No paperwork, no rate-limit clause, no anti-caching clause. Pure-NEMWEB backend; ~7 feeds; ship in ~10 working days following the rba-mcp + ato-mcp template.

**Market gap is real.** No AEMO MCP exists on PyPI, GitHub, or the MCP registries. Competing Python tooling (`nemosis`, `nempy`) is pandas/numpy-heavy and analyst-oriented. OpenElectricity has a clean JSON API but its data licence is **CC-BY-NC** — fatal for any commercial/hosted tier. NEMWEB-direct is the only commercially-licensable backend.

---

## 1. Licence — verdict: **REDISTRIBUTION-PERMITTED**

**Source:** https://aemo.com.au/privacy-and-legal-notices/copyright-permissions
**Confidence:** High on controlling text; medium on "no buried overlay" (origin returned 403 to non-browser UAs; verbatim from archive.org snapshot 2025-12-28).

### Verbatim controlling text

> **Copyright Permissions**
>
> AEMO Material comprises documents, reports, sound and video recordings and any other material created by or on behalf of AEMO and made publicly available by AEMO.
>
> All AEMO Material is protected by copyright under Australian law. A publication will be protected even if it does not display the © symbol.
>
> In addition to the uses permitted under copyright laws, AEMO confirms its general permission for anyone to use AEMO Material for any purpose, but only with accurate and appropriate attribution of the relevant AEMO Material and AEMO as its author.
>
> You do not need to obtain specific permission to use AEMO Material in this way.
>
> To be clear, confidential documents and any reports commissioned by another person or body who may own the copyright in them are not AEMO Material, and these permissions do not apply to those documents.

### What this allows

| Use case | Permitted? |
|---|---|
| Fetch NEMWEB CSV/ZIP files | ✅ Yes |
| Cache server-side (any TTL: 60s — 7d) | ✅ Yes (implicit in "any purpose") |
| Redistribute to MCP clients | ✅ Yes |
| Commercial / paid hosted tier | ✅ Yes (no NC clause) |
| Automated programmatic access | ✅ Not restricted (no robots.txt, no ToS) |

### Required attribution string

AEMO does not prescribe an exact form. Canonical attribution we will use on **every** `DataResponse`:

```
Source: Australian Energy Market Operator (AEMO), NEMWEB <report-name>, retrieved <retrieved_at>.
Used under AEMO's Copyright Permissions (general permission for any purpose with accurate attribution).
https://aemo.com.au/privacy-and-legal-notices/copyright-permissions
```

### Known artifacts (disclosed, not blocking)

- A 2009 NEMDE CD-ROM `Readme.htm` (e.g. http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/2017/NEMDE_2017_03/Readme.htm) says "not intended for commercial use" — but this predates the current site-wide Copyright Permissions policy and is contradicted by it. Will be noted in repo NOTICE.
- No first-party AEMO data on data.gov.au — AEMO is the sole upstream. (`q=AEMO` returns 45 results, all adjacent topology / GenCost / state govt reports. No NEMWEB feeds.)

### Operational courtesy

NEMWEB is a static IIS file server with no documented rate limit, but caching reduces load. We will:
- Use SQLite-backed disk cache with per-feed TTLs (60s dispatch, 5min regional, 1h forecasts, 7d archives).
- Set `User-Agent: aemo-mcp/<version> (+https://github.com/Bigred97/aemo-mcp)`.
- In-flight request deduplication (mandatory at 5-min cadence — many users → thundering herd).

---

## 2. Backend inventory — NEMWEB structure

NEMWEB is a flat IIS file server. No JSON API. Directory listings are plain HTML (`<A HREF=…>` per file). AEMO's `dev.aemo.com.au` developer portal exists but is **participant-gated** (NEM dispatch bidding, DER registration) and unusable as a public backend.

### Folder map

```
http://nemweb.com.au/
├── Reports/
│   ├── Current/                       ← rolling window, ~30d
│   │   ├── DispatchIS_Reports/        ← 5-min: price + demand + interconnector (workhorse)
│   │   ├── TradingIS_Reports/         ← 5-min file, 30-min content
│   │   ├── Dispatch_SCADA/            ← 5-min: DUID-level MW (gen by unit)
│   │   ├── ROOFTOP_PV/
│   │   │   ├── ACTUAL/                ← 30-min: regional rooftop PV
│   │   │   └── FORECAST/              ← 30-min: satellite forecast
│   │   ├── P5_Reports/                ← 5-min predispatch (~1h horizon)
│   │   ├── PredispatchIS_Reports/     ← 30-min predispatch (~40h horizon)
│   │   ├── Daily_Reports/             ← daily rolled-up compendium
│   │   ├── Public_Prices/             ← daily settlement prices (post-AP revisions)
│   │   ├── Next_Day_Dispatch/         ← daily DUID-level dispatch solution
│   │   ├── Operational_Demand/        ← ACTUAL_5MIN, HH, DAILY, FORECAST_HH
│   │   └── HistDemand/                ← 30-min regional demand + RRP
│   └── Archive/                       ← older copies of Current/ folders
└── Data_Archive/Wholesale_Electricity/MMSDM/<YYYY>/MMSDM_<YYYY>_<MM>/
                                        ← monthly historical bulk, 2012—present
```

### Feed comparison table

| Feed | Folder | Filename pattern | Cadence | Format | Size | Latency | Notes |
|---|---|---|---|---|---|---|---|
| **DispatchIS** | `/Reports/Current/DispatchIS_Reports/` | `PUBLIC_DISPATCHIS_YYYYMMDDHHmm_<seq>.zip` | 5 min | ZIP / multi-section CSV | ~22 KB | ~1-2 min | Sections: DISPATCHPRICE, DISPATCHREGIONSUM, DISPATCHINTERCONNECTORRES, DISPATCHCASESOLUTION. **Three feeds in one file.** |
| **Dispatch_SCADA** | `/Reports/Current/Dispatch_SCADA/` | `PUBLIC_DISPATCHSCADA_YYYYMMDDHHmm_<seq>.zip` | 5 min | ZIP / CSV | ~4 KB | ~1 min | DUID-level MW. Need static DUDETAILSUMMARY for fuel mapping. |
| **ROOFTOP_PV/ACTUAL** | `/Reports/Current/ROOFTOP_PV/ACTUAL/` | `PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_*.zip` | 30 min | ZIP / CSV | ~480 KB | within 30 min | Region-level satellite-derived actuals. |
| **ROOFTOP_PV/FORECAST** | `/Reports/Current/ROOFTOP_PV/FORECAST/` | `PUBLIC_ROOFTOP_PV_FORECAST_*.zip` | 30 min | ZIP / CSV | ~21 KB | within 30 min | Region-level forecast. |
| **P5MIN** | `/Reports/Current/P5_Reports/` | `PUBLIC_P5MIN_*.zip` | 5 min | ZIP / multi-section CSV | ~225 KB | ~1-2 min | 5-min predispatch, ~1h horizon. |
| **PredispatchIS** | `/Reports/Current/PredispatchIS_Reports/` | `PUBLIC_PREDISPATCHIS_*.zip` | 30 min | ZIP / multi-section CSV | ~1.1 MB | ~15-30 min | 30-min predispatch, ~40h horizon. |
| **Daily_Reports** | `/Reports/Current/Daily_Reports/` | `PUBLIC_DAILY_YYYYMMDD0000_*.zip` | Daily | ZIP / multi-section CSV | ~6.3 MB | next day ~04:10 AEST | Yesterday's full data in one drop. |

### MMSDM Historical Archive

Path: `/Data_Archive/Wholesale_Electricity/MMSDM/<YYYY>/MMSDM_<YYYY>_<MM>/MMSDM_Historical_Data_SQLLoader/DATA/`
- ~165 `PUBLIC_DVD_<TABLE>_YYYYMM010000.zip` files per month, 2012—present.
- Core price/region/SCADA tables: <100 MB per month.
- Bid tables (BIDPEROFFER) up to 2.5 GB per month.
- Won't bundle in wheel. Live-fetch only via the same `get_data()` plumbing with longer cache TTL.

### Identifying "the latest file"

Filenames are timestamp-prefixed (`YYYYMMDDHHmm` after the feed prefix). The latest file is the **lexicographically largest** entry in the directory listing. The trailing `_<seq>` integer is monotonic globally and breaks ties. **Do not** trust HTTP `Last-Modified` (occasionally rewritten on republish). Strategy: GET the IIS HTML, parse `<A HREF=` entries, `max()` by filename. One request, no per-file HEAD.

### Schema stability

Post-5MS (October 2021) schemas are stable. Pre-5MS files have different field counts on some sections — we will document a **2022-01-01 minimum recommended start** for `start_period` and gracefully tolerate older data via best-effort parsing.

---

## 3. Competitive landscape — confirmed gap

**No AEMO MCP server exists.** Searched PyPI, GitHub topic `aemo`, MCP servers registry — zero hits for `"aemo mcp"` or `"nemweb mcp"`.

Python ecosystem we are not depending on:

| Package | Backend | Footprint | Why we don't depend on it |
|---|---|---|---|
| `nemosis` 3.8.1 | NEMWEB direct | pandas + pyarrow (~300 MB tree) | Analyst-oriented, heavy install — incompatible with MCP "10 MB cold start" goal |
| `nempy` 3.0.3 | NEMWEB via nemosis | pandas + numpy + mip + scipy | Dispatch simulator, not a feed reader |
| `openelectricity` 0.10.1 | OpenElectricity JSON API | httpx-light | **Data licence is CC-BY-NC** — blocks commercial use |
| `nemseer` 1.0.7 | NEMWEB direct (forecasts only) | pandas + xarray | Stale (>2yr), forecast-only scope |

**Architectural takeaway:** Ship `httpx + stdlib csv + zipfile + aiosqlite`. Optional `pandas` only behind an extras flag for CSV chunking if needed. This is a real selling point — "10 MB install, sub-second cold start" vs. nemosis's 300 MB.

---

## 4. Recommended curated feeds for aemo-mcp (7 datasets)

Pulled from the 13 candidate feeds above. Each row is one `dataset_id` exposed by the MCP. Some share a backend file (DispatchIS = 3 datasets from 1 ZIP — we de-dupe at the cache layer):

| `dataset_id` | Description | Backend feed | Cadence | Cache TTL | Use case |
|---|---|---|---|---|---|
| **`dispatch_price`** | 5-min regional spot price (RRP) + 8 FCAS prices per NEM region | DispatchIS / DISPATCHPRICE | 5 min | 60 s | "Current NSW spot price"; negative-pricing detection |
| **`dispatch_region`** | 5-min total demand, scheduled + semi-scheduled generation, net interchange per region | DispatchIS / DISPATCHREGIONSUM | 5 min | 60 s | "Generation by fuel type now in QLD" (combined with SCADA); demand snapshots |
| **`interconnector_flows`** | 5-min MW flow + losses across the 6 interconnectors (NSW1-QLD1, V-SA, V-NSW, T-V-MNSP1, etc.) | DispatchIS / DISPATCHINTERCONNECTORRES | 5 min | 60 s | Interstate flow tracking |
| **`generation_scada`** | 5-min DUID-level SCADA MW (every generating unit in the NEM) | Dispatch_SCADA | 5 min | 60 s | Generation by unit / by fuel (with DUDETAILSUMMARY mapping) |
| **`rooftop_pv`** | 30-min regional rooftop solar — actual + forecast | ROOFTOP_PV/ACTUAL + FORECAST | 30 min | 5 min | "Rooftop PV forecast for tomorrow" |
| **`predispatch_30min`** | 30-min half-hourly forecast, ~40h horizon — price, demand, interconnector | PredispatchIS | 30 min | 1 h | Forward price curves; planning |
| **`daily_summary`** | Daily rolled-up compendium of yesterday's price + demand + dispatch | Daily_Reports | Daily | 24 h | Backfill; "weekly average dispatch price for VIC, last 4 weeks" |

This set covers ~95% of typical NEM analytic queries the user demo prompts target. Each feed maps to a YAML in `src/aemo_mcp/curated/` following the ato-mcp pattern.

### Why these and not the others

- **Trading price** (TRADINGIS) — post-5MS this is just the arithmetic mean of six dispatch prices. Redundant for price discovery; settlement consumers can derive it themselves from `dispatch_price`.
- **P5MIN** (5-min predispatch, ~1h horizon) — overlaps with PredispatchIS. Predispatch 30-min has wider value for forward-curve queries; P5MIN is mostly used by traders inside the gate-closure window.
- **Operational_Demand** — useful but duplicates `dispatch_region`'s demand field. Can add as v0.2 if customers ask.
- **MMSDM bulk archive** — not a curated dataset; exposed via `get_data(start_period=…)` on the live feeds. The cache TTL extends to 7d for any period older than 24h so archive queries are cheap on repeat.

---

## 5. Cadence + freshness

| Feed family | Pub latency | File rotation | Latest-file detection |
|---|---|---|---|
| 5-min dispatch (DispatchIS, SCADA) | ~1-2 min after interval close | New file every 5 min, timestamp embedded | Lexicographic `max()` on filename |
| 30-min (ROOFTOP_PV, PredispatchIS, Trading) | within 30 min of interval close | New file every 30 min | Same |
| Daily (Daily_Reports, Public_Prices) | ~04:10 AEST next day | One file per market day | Date in filename |

A `latest()` call on `dispatch_price` returns the most-recent 5-min interval (≤7 min stale at worst). The `stale` flag on `DataResponse` is set when `retrieved_at - interval_end > 2× cadence`.

---

## 6. Build estimate

Following the rba-mcp + ato-mcp template:

| Phase | Work | Est. effort |
|---|---|---|
| Project scaffold | pyproject, CI, .gitignore, LICENSE (MIT), NOTICE (AEMO attribution) | 0.5 d |
| Core infra | httpx client, aiosqlite cache, in-flight dedup, NEMWEB directory parser, multi-section CSV splitter, ZIP unpacker | 2 d |
| Curated YAMLs (×7) + Pydantic models | 7 dataset YAMLs, DUID→fuel static lookup, DataResponse model | 2 d |
| 5 MCP tools (search/describe/get_data/latest/list_curated) | Mirror abs-mcp/server.py Field-annotation pattern | 1.5 d |
| Tests — ≥120 unit + ≥8 live | unit (cache, parsing, in-flight, shaping, latest-file), live (one per dataset) | 2 d |
| Glama package | glama.json, badge, README, CHANGELOG, examples/ with 5 demo prompts | 1 d |
| Zero-flake validation | 10× pytest -m 'not live' green | 0.5 d |
| **Total** | | **~9.5 days** |

---

## 7. Open questions for review

1. **DUID → fuel mapping source.** Easiest: ship a static `DUDETAIL.csv` snapshot in the wheel (refreshed quarterly). Alternative: live-fetch DUDETAILSUMMARY from NEMWEB on cold start. Recommend **static snapshot + refresh script** — DUID changes are infrequent and a static file makes the `generation_scada` tool deterministic.
2. **Should `dispatch_price` include FCAS prices by default, or only RRP (energy price)?** Recommend **RRP only by default**, with FCAS exposed via `filters={"price_type": "fcas_raise_6sec"}` etc. Keeps default responses small.
3. **Timezone.** NEMWEB uses AEMO market time (UTC+10, no DST). Recommend storing/serving as `Australia/Brisbane`-equivalent (no DST), with `interval_start` / `interval_end` as ISO-8601 UTC for client portability. Both versions in the response.
4. **MMSDM live-fetch on request older than X.** When `start_period < today - 30d`, the implementation should pivot from `/Reports/Current/` to `/Data_Archive/.../MMSDM/`. Confirm cutover at 30 days vs 7 days vs "auto-detect missing".

---

## 8. Anti-patterns we will avoid

- **No 6th tool.** Stick to `search_datasets`, `describe_dataset`, `get_data`, `latest`, `list_curated`.
- **No pre-bundled NEMWEB archives in the wheel.** Live fetch with disk cache only.
- **No pandas at the tool surface.** Records returned as plain `list[dict]` / `list[Observation]`.
- **No PyPI publish without 10 zero-flake runs.**
- **No depending on nemosis, nempy, or openelectricity.**

---

## 9. References

- AEMO Copyright Permissions — https://aemo.com.au/privacy-and-legal-notices/copyright-permissions
- AEMO Copyright Permissions (archive.org 2025-12-28 snapshot, verbatim source) — https://web.archive.org/web/20251228101817/https://www.aemo.com.au/privacy-and-legal-notices/copyright-permissions
- AEMO NEMWEB market data page — https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb
- NEMWEB Reports/Current — http://nemweb.com.au/Reports/Current/
- NEMWEB Data Archive (MMSDM) — http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/
- AEMO 5MS programme background (Oct 2021 5-min settlement transition) — https://aemo.com.au/initiatives/major-programs/nem-five-minute-settlement-program-and-global-settlement
- data.gov.au CKAN search (q=AEMO, no first-party feeds) — https://data.gov.au/data/api/3/action/package_search?q=AEMO
- OpenElectricity licence (CC-BY-NC, why we don't proxy it) — https://platform.openelectricity.org.au/license

---

## Next step (awaiting approval)

If approved, Phase 2 begins by scaffolding the repo and implementing the DispatchIS path end-to-end as the first feed (`dispatch_price`, `dispatch_region`, `interconnector_flows` share one fetcher). 4-line update will follow first feed E2E.

**Decisions needed from Harry before I write code:**
1. Approve the 7-feed curated set above (or trim/swap).
2. Decide on the 4 open questions in §7 (DUID source, FCAS default, timezone, MMSDM cutover).
3. Confirm `aemo-mcp` / `aemo_mcp` / `Bigred97/aemo-mcp` identity is final.
