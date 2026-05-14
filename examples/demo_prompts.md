# aemo-mcp — Demo prompts

Five prompts you can ask any MCP-aware LLM (Claude Desktop, Cursor, etc.)
once `aemo-mcp` is installed.

## 1. Current NSW spot price

> What is the current NEM spot price in NSW?

The model calls `latest(dataset_id="dispatch_price", filters={"region": "NSW1"})`
and reports the most recent 5-minute RRP, the interval timestamp (AEMO market
time, UTC+10), and the source URL.

## 2. Generation by fuel type right now in QLD

> What is the current generation mix in Queensland by fuel type?

The model calls `latest(dataset_id="generation_scada", filters={"region": "QLD1"})`
and aggregates DUID-level MW into fuel buckets (Black coal, Gas, Hydro, Wind,
Solar, Battery) via the bundled `DUDETAILSUMMARY` snapshot.

## 3. Did SA hit negative pricing in the last 24 hours?

> Show me any 5-minute intervals in South Australia where the spot price went
> negative in the last 24 hours.

The model calls `get_data(dataset_id="dispatch_price", filters={"region": "SA1"},
start_period="<24h ago>")` and filters records where `value < 0`.

## 4. Weekly average dispatch price for VIC, last 4 weeks

> What has the weekly average dispatch price been for Victoria over the last
> four weeks?

The model calls `get_data(dataset_id="dispatch_price", filters={"region": "VIC1"},
start_period="<4 weeks ago>")` and computes weekly means client-side.

## 5. Rooftop PV forecast for tomorrow

> What's the rooftop solar forecast for tomorrow across the NEM?

The model calls `get_data(dataset_id="rooftop_pv", filters={"section": "forecast"},
start_period="<tomorrow>")` and summarises by region.

## Trust contract

Every response carries:

- `source = "Australian Energy Market Operator"`
- `attribution` — the canonical AEMO Copyright Permissions string
- `source_url` — the NEMWEB folder the data came from
- `retrieved_at` — UTC timestamp of the fetch
- `interval_start` / `interval_end` — period covered
- `stale` — `True` if the latest interval is older than 2x the feed cadence
