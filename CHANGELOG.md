# Changelog

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
