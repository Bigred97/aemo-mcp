# Contributing to aemo-mcp

Thanks for the interest. This MCP server is part of a family of Australian
open-data wrappers ([abs-mcp](https://github.com/Bigred97/abs-mcp),
[rba-mcp](https://github.com/Bigred97/rba-mcp),
[ato-mcp](https://github.com/Bigred97/ato-mcp),
[apra-mcp](https://github.com/Bigred97/apra-mcp),
[aihw-mcp](https://github.com/Bigred97/aihw-mcp),
[asic-mcp](https://github.com/Bigred97/asic-mcp),
[au-weather-mcp](https://github.com/Bigred97/au-weather-mcp)) and follows the
same architectural template.

## Quick start

```bash
git clone https://github.com/Bigred97/aemo-mcp
cd aemo-mcp
uv sync --extra dev
uv pip install -e .
uv run pytest -q
```

## What we accept

- **Bug fixes** with a regression test.
- **New curated feeds** for high-value NEMWEB reports. Add a YAML in
  `src/aemo_mcp/data/curated/`, extend `src/aemo_mcp/data/feeds.yaml`, and
  add at least one unit test against a fixture in `tests/fixtures/`.
- **Search-keyword expansions** to help LLMs route common queries — must be
  paired with a routing regression test in `test_feeds.py`.
- **Performance improvements** in parsing, caching, or in-flight dedup.

## What we don't accept (by design)

- **Tool count > 5.** We hold to the standard `search_datasets / describe_dataset / get_data / latest / list_curated` surface across all sibling MCPs.
- **Pre-bundled NEMWEB archives in the wheel.** Live fetch only.
- **Pandas at the tool surface.** Records must be plain `list[dict]` / `list[Observation]`. Pandas is fine internally for CSV parsing.
- **Dependencies on `nemosis`, `nempy`, `openelectricity`.** Stay light.

## Tests

```bash
# Unit tests (offline)
uv run pytest -q

# Live tests (hit NEMWEB)
uv run pytest -q -m live

# 10x zero-flake validation
for i in $(seq 1 10); do uv run pytest -q || break; done
```

## Release

Tag the commit:

```bash
git tag v0.1.x
git push origin v0.1.x
```

GitHub Actions builds the wheel and publishes to PyPI via Trusted Publishing
(OIDC, no token needed).

## Attribution

Every response carries the AEMO Copyright Permissions attribution string.
Keep that contract intact across changes.
