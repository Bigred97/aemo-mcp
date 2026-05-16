"""Resilience + memory smoke tests for the high-cadence streaming path.

The 5-minute NEM feeds (dispatch_price, dispatch_region, generation_scada,
interconnector_flows) are the highest-volume datasets in the portfolio.
Without row-level streaming + filter pushdown, a multi-day archive window
would hold the full parsed sections list in memory and peak RSS would spike
above 100MB.

Per the playbook (Item 1 acceptance criteria):
  - latest("dispatch_price", filters={"region": "NSW1"}) → <3s, <50MB peak
  - get_data("generation_scada", filters={"region": "NSW1"},
             start_period="...", end_period="...") → <10s, <100MB peak

These tests use respx to mock NEMWEB and synthesise a realistically-large
DISPATCH ZIP archive (288 intervals × 5 regions × 7 metrics) to exercise
the streaming filter. The mocks ensure we never hit the live CDN.
"""
from __future__ import annotations

import io
import time
import tracemalloc
import zipfile
from pathlib import Path

import pytest
import respx

from aemo_mcp.cache import Cache
from aemo_mcp.client import AEMOClient
from aemo_mcp.curated import get as get_curated
from aemo_mcp.fetch import fetch_dataset


# ── Synthetic AEMO CSV factories ──────────────────────────────────────


def _make_dispatch_is_csv(intervals: int = 288) -> bytes:
    """Generate a DISPATCHIS CSV with `intervals` 5-min intervals × 5 regions.

    288 intervals = one full trading day at 5-minute cadence. Includes
    DISPATCH.PRICE + DISPATCH.REGIONSUM + DISPATCH.INTERCONNECTORRES so the
    streaming filter has to skip the unwanted sections.
    """
    buf = io.StringIO()
    buf.write(
        "C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2026/05/14 23:55:00,"
        "0000000000,DISPATCHIS,0000000000\n"
    )
    buf.write(
        "I,DISPATCH,PRICE,1,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,"
        "INTERVENTION,RRP,EEP\n"
    )
    regions = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
    for i in range(intervals):
        h = i // 12
        m = (i % 12) * 5
        ts = f"2026/05/14 {h:02d}:{m:02d}:00"
        for r in regions:
            buf.write(
                f"D,DISPATCH,PRICE,1,{ts},1,{r},1,0,{80.0 + i * 0.1:.2f},0.00\n"
            )
    buf.write(
        "I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,RUNNO,REGIONID,"
        "DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,"
        "DISPATCHABLEGENERATION,NETINTERCHANGE\n"
    )
    for i in range(intervals):
        h = i // 12
        m = (i % 12) * 5
        ts = f"2026/05/14 {h:02d}:{m:02d}:00"
        for r in regions:
            buf.write(
                f"D,DISPATCH,REGIONSUM,1,{ts},1,{r},1,0,"
                f"{8000.0 + i:.1f},12000.0,11000.0,-100.0\n"
            )
    buf.write(
        "I,DISPATCH,INTERCONNECTORRES,1,SETTLEMENTDATE,RUNNO,"
        "INTERCONNECTORID,DISPATCHINTERVAL,INTERVENTION,MWFLOW,MWLOSSES\n"
    )
    intercon = ["N-Q-MNSP1", "NSW1-QLD1", "T-V-MNSP1", "V-S-MNSP1", "V-SA"]
    for i in range(intervals):
        h = i // 12
        m = (i % 12) * 5
        ts = f"2026/05/14 {h:02d}:{m:02d}:00"
        for ic in intercon:
            buf.write(
                f"D,DISPATCH,INTERCONNECTORRES,1,{ts},1,{ic},1,0,"
                f"{50.0 + i * 0.5:.1f},5.0\n"
            )
    buf.write("C,END OF REPORT,99\n")
    return buf.getvalue().encode("utf-8")


def _make_scada_csv(intervals: int = 288, duids: int = 100) -> bytes:
    """Generate a DISPATCH_SCADA CSV with `intervals` × `duids` rows.

    Default 288 × 100 = 28800 rows, simulating a full day of unit SCADA. The
    DUID list mixes 6 known DUIDs (so the duid_lookup snapshot maps them)
    with synthetic ones so the filter has work to do.
    """
    buf = io.StringIO()
    buf.write(
        "C,NEMP.WORLD,DISPATCH_SCADA,AEMO,PUBLIC,2026/05/14 23:55:00,"
        "0000000000,DISPATCH_SCADA,0000000000\n"
    )
    buf.write("I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,DUID,SCADAVALUE\n")
    # Known DUIDs from data/duid_snapshot.csv that map cleanly to regions.
    known = ["BW01", "BW02", "ER01", "LY_W1", "LY_W2", "COOPGWF1"]
    synthetic = [f"FAKE{n:03d}" for n in range(duids - len(known))]
    all_duids = known + synthetic
    for i in range(intervals):
        h = i // 12
        m = (i % 12) * 5
        ts = f"2026/05/14 {h:02d}:{m:02d}:00"
        for j, duid in enumerate(all_duids):
            buf.write(
                f"D,DISPATCH,UNIT_SCADA,1,{ts},{duid},{100.0 + j * 5.0:.1f}\n"
            )
    buf.write("C,END OF REPORT,99\n")
    return buf.getvalue().encode("utf-8")


def _zip_one(name: str, body: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, body)
    return buf.getvalue()


def _make_listing(folder: str, filenames: list[str]) -> str:
    """Mock IIS-style NEMWEB directory listing with given filenames."""
    rows = "\n".join(
        f'<A HREF="{folder}{fn}">{fn}</A>' for fn in filenames
    )
    return f"<html><body><pre>{rows}</pre></body></html>"


# ── Acceptance tests ──────────────────────────────────────────────────


@respx.mock
async def test_latest_dispatch_price_filtered_is_bounded(tmp_path: Path):
    """latest('dispatch_price', filters={'region': 'NSW1'}): <3s, <50MB peak.

    The mock returns a 288-interval (full-day) DISPATCHIS CSV containing
    three sections (PRICE + REGIONSUM + INTERCONNECTORRES, ~3000 rows total).
    The streaming filter must skip the unwanted sections and only retain NSW1.
    """
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        folder = "/Reports/Current/DispatchIS_Reports/"
        filenames = [
            "PUBLIC_DISPATCHIS_202605142355_0000000456789012.zip",
        ]
        respx.get(f"http://nemweb.com.au{folder}").respond(
            200, text=_make_listing(folder, filenames)
        )
        csv_body = _make_dispatch_is_csv(intervals=288)
        zip_body = _zip_one(
            "PUBLIC_DISPATCHIS_202605142355_0000000456789012.CSV", csv_body
        )
        respx.get(f"http://nemweb.com.au{folder}{filenames[0]}").respond(
            200, content=zip_body
        )

        cd = get_curated("dispatch_price")

        tracemalloc.start()
        start = time.perf_counter()
        resp = await fetch_dataset(
            client, cd, {"region": "NSW1"}, None, None, "records",
            only_latest=True,
        )
        elapsed = time.perf_counter() - start
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert resp.dataset_id == "dispatch_price"
        # latest collapses to one row per region; with NSW1 filter that's 1 row.
        assert len(resp.records) == 1
        # Performance bounds per playbook
        assert elapsed < 3.0, f"too slow: {elapsed:.2f}s"
        peak_mb = peak / 1024 / 1024
        assert peak_mb < 50.0, f"peak memory too high: {peak_mb:.1f}MB"
    finally:
        await client.aclose()


@respx.mock
async def test_get_data_generation_scada_window_is_bounded(tmp_path: Path):
    """get_data('generation_scada', region='NSW1', window): <10s, <100MB peak.

    The mock returns a 288-interval × 100-DUID DISPATCH_SCADA CSV
    (~28800 rows). The streaming filter must reject ~95% of rows (everything
    not in NSW1's DUID allow-set) before they ever land in a Python dict.
    """
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        folder = "/Reports/Current/Dispatch_SCADA/"
        # 12 files = 1h of 5-min data. Plenty to exercise the streaming
        # filter without crossing into the 4h archive cutover.
        # Filenames must match `PUBLIC_DISPATCHSCADA_\d{12}_\d+\.zip`.
        filenames = [
            f"PUBLIC_DISPATCHSCADA_2026051423{m:02d}_0000000456789012.zip"
            for m in [50, 55]
        ]
        respx.get(f"http://nemweb.com.au{folder}").respond(
            200, text=_make_listing(folder, filenames)
        )
        for fn in filenames:
            csv_body = _make_scada_csv(intervals=288, duids=100)
            respx.get(f"http://nemweb.com.au{folder}{fn}").respond(
                200,
                content=_zip_one(fn.replace(".zip", ".CSV"), csv_body),
            )

        cd = get_curated("generation_scada")

        # Recent window so the fetcher stays on /Current/.
        from datetime import datetime, timedelta

        from aemo_mcp.shaping import NEM_TZ
        now = datetime.now(NEM_TZ)
        start = (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M")
        end = now.strftime("%Y-%m-%d %H:%M")

        tracemalloc.start()
        t0 = time.perf_counter()
        resp = await fetch_dataset(
            client, cd, {"region": "NSW1"}, start, end, "records",
            only_latest=False,
        )
        elapsed = time.perf_counter() - t0
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert resp.dataset_id == "generation_scada"
        # Only NSW1 DUIDs from the known set are retained — BW01, BW02, ER01.
        duids = {o.dimensions.get("duid") for o in resp.records}  # type: ignore
        assert duids.issubset({"BW01", "BW02", "ER01"})
        # Performance bounds per playbook
        assert elapsed < 10.0, f"too slow: {elapsed:.2f}s"
        peak_mb = peak / 1024 / 1024
        assert peak_mb < 100.0, f"peak memory too high: {peak_mb:.1f}MB"
    finally:
        await client.aclose()


@respx.mock
async def test_streaming_filter_rejects_unwanted_sections(tmp_path: Path):
    """Regression: dispatch_price stream must NOT keep DISPATCH.REGIONSUM rows.

    The streaming iterator's target_section short-circuits sections it isn't
    looking for. If that's broken, dispatch_price would accidentally include
    demand rows + interconnector rows in its DataResponse.
    """
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        folder = "/Reports/Current/DispatchIS_Reports/"
        # Filename must match curated regex `PUBLIC_DISPATCHIS_\d{12}_\d+\.zip`
        filenames = ["PUBLIC_DISPATCHIS_202605142355_0000000456789012.zip"]
        respx.get(f"http://nemweb.com.au{folder}").respond(
            200, text=_make_listing(folder, filenames)
        )
        respx.get(f"http://nemweb.com.au{folder}{filenames[0]}").respond(
            200,
            content=_zip_one(
                filenames[0].replace(".zip", ".CSV"),
                _make_dispatch_is_csv(intervals=12),
            ),
        )

        cd = get_curated("dispatch_price")
        resp = await fetch_dataset(
            client, cd, None, None, None, "records", only_latest=True
        )
        # Every observation must carry the rrp metric (DISPATCH.PRICE) —
        # never the regionsum or interconnector metrics.
        metrics = {o.dimensions.get("metric") for o in resp.records}  # type: ignore
        assert metrics == {"rrp"}
    finally:
        await client.aclose()


@pytest.mark.parametrize(
    "dataset_id,filters",
    [
        ("dispatch_price", {"region": "NSW1"}),
        ("dispatch_region", {"region": "NSW1"}),
        ("interconnector_flows", {"interconnector": "V-SA"}),
    ],
)
@respx.mock
async def test_streaming_path_high_cadence_filtered(
    tmp_path: Path, dataset_id: str, filters: dict
):
    """Smoke test: the streaming path returns the same observations for
    every high-cadence single-section feed.
    """
    cache = Cache(db_path=tmp_path / "c.db")
    client = AEMOClient(cache=cache)
    try:
        folder = "/Reports/Current/DispatchIS_Reports/"
        filenames = ["PUBLIC_DISPATCHIS_202605141000_0000000456789012.zip"]
        respx.get(f"http://nemweb.com.au{folder}").respond(
            200, text=_make_listing(folder, filenames)
        )
        respx.get(f"http://nemweb.com.au{folder}{filenames[0]}").respond(
            200,
            content=_zip_one(
                filenames[0].replace(".zip", ".CSV"),
                _make_dispatch_is_csv(intervals=1),
            ),
        )

        cd = get_curated(dataset_id)
        resp = await fetch_dataset(
            client, cd, filters, None, None, "records", only_latest=True
        )
        assert resp.row_count >= 1
        # Every record must come from the requested filter dimension.
        key = next(iter(filters))
        vals = {o.dimensions.get(key) for o in resp.records}  # type: ignore
        assert vals == {filters[key]}
    finally:
        await client.aclose()
