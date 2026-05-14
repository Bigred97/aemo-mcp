# aemo-mcp

[![tests](https://github.com/Bigred97/aemo-mcp/actions/workflows/test.yml/badge.svg)](https://github.com/Bigred97/aemo-mcp/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/aemo-mcp.svg)](https://pypi.org/project/aemo-mcp/)
[![Python](https://img.shields.io/pypi/pyversions/aemo-mcp.svg)](https://pypi.org/project/aemo-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Glama MCP server quality](https://glama.ai/mcp/servers/Bigred97/aemo-mcp/badges/score.svg)](https://glama.ai/mcp/servers/Bigred97/aemo-mcp)

**Ask Claude about Australia's electricity market and get real, current
numbers** — 5-minute spot prices, regional demand, generation by fuel,
interconnector flows, rooftop solar — not "I don't have access to that data."
This MCP server gives Claude (and other MCP clients like Cursor) live access
to the [Australian Energy Market Operator (AEMO) NEMWEB](http://nemweb.com.au/Reports/Current/)
feeds, with curated mappings for the most-asked indicators.

Companion to [abs-mcp](https://github.com/Bigred97/abs-mcp) (ABS macro stats),
[rba-mcp](https://github.com/Bigred97/rba-mcp) (RBA interest + FX rates),
[ato-mcp](https://github.com/Bigred97/ato-mcp) (ATO tax + ACNC charity),
[apra-mcp](https://github.com/Bigred97/apra-mcp) (banking + superannuation),
[aihw-mcp](https://github.com/Bigred97/aihw-mcp) (health & welfare),
[asic-mcp](https://github.com/Bigred97/asic-mcp) (companies + financial advisers),
and [au-weather-mcp](https://github.com/Bigred97/au-weather-mcp) (Australian
weather) — together they cover the most-asked Australian official data.

## What you can ask

Once installed, your LLM can answer questions like:

| Question | What the tool does |
|---|---|
| What's the current NSW spot price? | `latest("dispatch_price", filters={"region":"NSW1"})` |
| Did SA hit negative pricing in the last 24 hours? | `get_data("dispatch_price", filters={"region":"SA1"}, start_period=…)` and filter `value < 0` |
| Generation by fuel type right now in QLD | `latest("generation_scada", filters={"region":"QLD1"})`, aggregated by fuel |
| Weekly average dispatch price for VIC, last 4 weeks | `get_data("dispatch_price", filters={"region":"VIC1"}, start_period=…)` |
| Rooftop PV forecast for tomorrow | `get_data("rooftop_pv", filters={"section":"forecast"}, start_period="<tomorrow>")` |
| What's the current flow across Heywood (VIC ↔ SA)? | `latest("interconnector_flows", filters={"interconnector":"V-SA"})` |
| Total NEM demand right now | `latest("dispatch_region")` |

Every answer comes with the interval timestamp (AEMO market time, UTC+10),
units (MW, $/MWh), and a link back to the NEMWEB source. The MCP wraps
NEMWEB's CSV/ZIP feeds and exposes them through 5 plain-English tools.

## Install

```bash
# After publish:
uvx --upgrade aemo-mcp

# Local dev:
uv pip install -e .
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "aemo": {
      "command": "uvx",
      "args": ["--upgrade", "aemo-mcp"]
    }
  }
}
```

> **Why `--upgrade`?** `uvx aemo-mcp` (without the flag) uses whatever wheel
> is cached and never adopts new PyPI releases on its own. `--upgrade` makes
> uvx check PyPI on each launch and pull a newer release if one exists.
> Recommended for everyone except offline-first / pinned-version workflows.
> To verify which version is currently serving you, look at the
> `server_version` field on any `DataResponse`.

If you also have `rba-mcp` / `abs-mcp` installed, all servers run side-by-side.
Claude disambiguates with the server prefix (`aemo:get_data`, `rba:get_data`,
`abs:get_data`).

For local dev (pre-PyPI):

```json
{
  "mcpServers": {
    "aemo": {
      "command": "uv",
      "args": ["run", "--directory", "/absolute/path/to/aemo-mcp", "aemo-mcp"]
    }
  }
}
```

### Cursor

Add to `~/.cursor/mcp.json` (or workspace `.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "aemo": {
      "command": "uvx",
      "args": ["--upgrade", "aemo-mcp"]
    }
  }
}
```

## Tools

| Tool | What it does |
|---|---|
| `search_datasets(query, limit=10)` | Fuzzy-search the 7 curated NEM datasets by name, topic, or region. |
| `describe_dataset(dataset_id)` | Schema, filters, source URL, cadence, and example queries for one dataset. |
| `get_data(dataset_id, filters, start_period, end_period, format)` | Query data. Records / series / csv format. |
| `latest(dataset_id, filters)` | Most recent 5-min or 30-min interval for time-series feeds. |
| `list_curated()` | The 7 curated dataset IDs. |

## Curated datasets

The 7 datasets cover ~95% of typical NEM analytic queries:

| `dataset_id` | Cadence | Source | Use case |
|---|---|---|---|
| **`dispatch_price`** | 5 min | DispatchIS / DISPATCHPRICE | Current spot price per region; negative-pricing detection |
| **`dispatch_region`** | 5 min | DispatchIS / DISPATCHREGIONSUM | Demand + scheduled + semi-scheduled gen + net interchange |
| **`interconnector_flows`** | 5 min | DispatchIS / DISPATCHINTERCONNECTORRES | MW flow + losses across NEM interconnectors |
| **`generation_scada`** | 5 min | Dispatch_SCADA | DUID-level MW (every unit), aggregable by fuel |
| **`rooftop_pv`** | 30 min | ROOFTOP_PV/ACTUAL + FORECAST | Regional rooftop solar (actual + forecast) |
| **`predispatch_30min`** | 30 min | PredispatchIS | 30-min forecast, ~40h horizon |
| **`daily_summary`** | Daily | Daily_Reports | Yesterday's full data in one drop |

Use `list_curated()` to enumerate, `describe_dataset(dataset_id)` to learn
the filters available on each.

## Regions

NSW1 (New South Wales), QLD1 (Queensland), SA1 (South Australia), TAS1
(Tasmania), VIC1 (Victoria). Western Australia (WEM) and the Northern
Territory are not on the NEM and are out of scope.

## Trust contract

Every `DataResponse` carries:

- `source = "Australian Energy Market Operator"`
- `attribution` — AEMO Copyright Permissions verbatim attribution string
- `source_url` — the NEMWEB folder the data came from
- `retrieved_at` — UTC timestamp of the fetch
- `interval_start` / `interval_end` — period covered
- `stale` — `True` if the latest interval is older than 2× the feed cadence
- `server_version` — the wheel that served the call

## Licence + attribution

This package is MIT-licensed (see [LICENSE](LICENSE)).

The **AEMO data** it fetches is published under AEMO's Copyright Permissions
policy: AEMO grants general permission to use AEMO Material for any purpose
(commercial included) on the sole condition of accurate attribution of the
relevant material and AEMO as its author. See
[https://aemo.com.au/privacy-and-legal-notices/copyright-permissions](https://aemo.com.au/privacy-and-legal-notices/copyright-permissions).

End-users redistributing data fetched via this server must credit AEMO.
The canonical attribution string is on every `DataResponse.attribution`.

## Worked examples

**"What's the current NSW spot price?"**

```
latest(dataset_id="dispatch_price", filters={"region": "NSW1"})
```

→ `{"records": [{"period": "2026-05-14T10:05:00+10:00", "value": 87.5,
"dimensions": {"region": "NSW1", "metric": "rrp"}, "unit": "$/MWh"}], ...}`

**"NSW spot price for the last 24 hours"**

```
get_data(dataset_id="dispatch_price", filters={"region": "NSW1"},
         start_period="2026-05-13", end_period="2026-05-14")
```

**"Did SA hit negative pricing in the last 24 hours?"**

```
get_data(dataset_id="dispatch_price", filters={"region": "SA1"},
         start_period="<24h ago>")
```

Then the LLM filters `value < 0` client-side.

**"Generation by fuel type right now in QLD"**

```
latest(dataset_id="generation_scada", filters={"region": "QLD1"})
```

→ DUID-level rows with fuel attribution; the LLM aggregates by fuel.

## How it works

- **Live-fetch only.** No NEMWEB archives in the wheel. Every request goes
  through the cache.
- **Cache TTLs tuned per cadence.** 60s for 5-min feeds, 5min for 30-min
  feeds, 1h forecasts, 24h daily archive. Timestamped historical files are
  immutable in NEMWEB and cache effectively forever.
- **In-flight request deduplication.** Concurrent callers for the same URL
  share one HTTP request. Critical at 5-min cadence with many users.
- **Latest-file detection** is purely lexicographic on the NEMWEB directory
  listing — AEMO embeds the interval timestamp in every filename
  (`PUBLIC_DISPATCHIS_YYYYMMDDHHmm_<seq>.zip`), so `max()` is enough.
- **Multi-section CSV parser** handles AEMO's `I,/D,` row format where a
  single ZIP holds several tables (DISPATCHPRICE, DISPATCHREGIONSUM,
  DISPATCHINTERCONNECTORRES, etc.).

## Development

```bash
git clone https://github.com/Bigred97/aemo-mcp
cd aemo-mcp
uv sync --extra dev
uv pip install -e .
uv run pytest -q
```

Run live tests (hit NEMWEB):

```bash
uv run pytest -q -m live
```

Zero-flake validation:

```bash
for i in $(seq 1 10); do uv run pytest -q || break; done
```

## Companion MCPs

| MCP | Domain |
|---|---|
| [abs-mcp](https://github.com/Bigred97/abs-mcp) | ABS macroeconomic statistics (SDMX) |
| [rba-mcp](https://github.com/Bigred97/rba-mcp) | RBA interest + FX rates (F-tables) |
| [ato-mcp](https://github.com/Bigred97/ato-mcp) | ATO tax + ACNC charity data |
| [apra-mcp](https://github.com/Bigred97/apra-mcp) | APRA banking + superannuation statistics |
| [aihw-mcp](https://github.com/Bigred97/aihw-mcp) | AIHW health + welfare datasets |
| [asic-mcp](https://github.com/Bigred97/asic-mcp) | ASIC company + financial-adviser registers |
| [au-weather-mcp](https://github.com/Bigred97/au-weather-mcp) | Australian weather (Open-Meteo + BOM) |

## Author

Built by Harry Vass. Issues + PRs welcome at
[github.com/Bigred97/aemo-mcp](https://github.com/Bigred97/aemo-mcp).
