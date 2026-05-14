"""Observation building + format conversion tests."""
from __future__ import annotations

from aemo_mcp.curated import (
    CuratedDataset,
    CuratedFilter,
    CuratedFolder,
    CuratedMetric,
    CuratedSection,
)
from aemo_mcp.models import Observation
from aemo_mcp.shaping import (
    NEM_TZ,
    _parse_aemo_datetime,
    _safe_float,
    build_response,
    is_stale,
    records_to_observations,
    to_csv,
    to_series,
)


def _fake_dispatch_price() -> CuratedDataset:
    return CuratedDataset(
        id="dispatch_price",
        name="NEM Dispatch Price",
        description="Test",
        cadence="5 min",
        cache_kind="live",
        folders=(
            CuratedFolder(
                path="/x/",
                filename_regex=r".*\.zip",
                sections=(CuratedSection(name="DISPATCH.PRICE"),),
            ),
        ),
        filters=(
            CuratedFilter(
                key="region",
                description="NEM region",
                values=("NSW1", "QLD1", "SA1", "TAS1", "VIC1"),
                column="REGIONID",
            ),
        ),
        metrics=(
            CuratedMetric(
                key="rrp",
                source_column="RRP",
                description="RRP",
                unit="$/MWh",
            ),
        ),
        settlement_column="SETTLEMENTDATE",
    )


def test_safe_float_handles_numbers():
    assert _safe_float("87.5") == 87.5
    assert _safe_float("0") == 0.0
    assert _safe_float("-15.40") == -15.4


def test_safe_float_handles_none_and_empty():
    assert _safe_float(None) is None
    assert _safe_float("") is None
    assert _safe_float("  ") is None


def test_safe_float_rejects_garbage():
    assert _safe_float("not a number") is None
    assert _safe_float("NaN") is None  # NaN rejected for JSON safety
    assert _safe_float([1, 2]) is None


def test_parse_aemo_datetime_slash_format():
    dt = _parse_aemo_datetime("2026/05/14 10:05:00")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 5
    assert dt.day == 14
    assert dt.hour == 10
    assert dt.minute == 5
    assert dt.tzinfo == NEM_TZ


def test_parse_aemo_datetime_dash_format():
    dt = _parse_aemo_datetime("2026-05-14 10:05:00")
    assert dt is not None


def test_parse_aemo_datetime_iso_format():
    dt = _parse_aemo_datetime("2026-05-14T10:05:00")
    assert dt is not None


def test_parse_aemo_datetime_quoted():
    dt = _parse_aemo_datetime('"2026/05/14 10:05:00"')
    assert dt is not None
    assert dt.hour == 10


def test_parse_aemo_datetime_returns_none_for_garbage():
    assert _parse_aemo_datetime("not a date") is None
    assert _parse_aemo_datetime("") is None
    assert _parse_aemo_datetime(None) is None


def test_records_to_observations_one_metric_per_row():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"},
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "QLD1", "RRP": "65.2"},
    ]
    obs = records_to_observations(rows, dataset)
    assert len(obs) == 2
    assert obs[0].value == 87.5
    assert obs[0].dimensions["region"] == "NSW1"
    assert obs[0].dimensions["metric"] == "rrp"
    assert obs[0].unit == "$/MWh"


def test_records_to_observations_skips_missing_metric():
    dataset = _fake_dispatch_price()
    rows = [{"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": ""}]
    obs = records_to_observations(rows, dataset)
    assert obs == []


def test_records_to_observations_negative_price_preserved():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "SA1", "RRP": "-15.40"}
    ]
    obs = records_to_observations(rows, dataset)
    assert obs[0].value == -15.40


def test_records_with_extra_dimensions():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"}
    ]
    obs = records_to_observations(rows, dataset, extra_dimensions={"section": "actual"})
    assert obs[0].dimensions["section"] == "actual"
    assert obs[0].dimensions["region"] == "NSW1"


def test_to_csv_emits_header_and_rows():
    obs = [
        Observation(
            period="2026-05-14T10:00:00+10:00",
            value=87.5,
            dimensions={"region": "NSW1", "metric": "rrp"},
            unit="$/MWh",
        )
    ]
    csv = to_csv(obs)
    assert "period,value,unit,region,metric" in csv
    assert "NSW1" in csv
    assert "87.5" in csv


def test_to_csv_handles_empty():
    assert to_csv([]) == ""


def test_to_csv_escapes_commas():
    obs = [
        Observation(
            period="2026-05-14T10:00:00",
            value=1.0,
            dimensions={"region": "Has, Comma"},
            unit="MW",
        )
    ]
    csv = to_csv(obs)
    assert '"Has, Comma"' in csv


def test_to_series_groups_by_dimensions():
    obs = [
        Observation(period="t1", value=1.0, dimensions={"region": "NSW1"}, unit="MW"),
        Observation(period="t2", value=2.0, dimensions={"region": "NSW1"}, unit="MW"),
        Observation(period="t1", value=3.0, dimensions={"region": "QLD1"}, unit="MW"),
    ]
    series = to_series(obs)
    assert len(series) == 2
    nsw = next(s for s in series if s["dimensions"]["region"] == "NSW1")
    assert len(nsw["observations"]) == 2


def test_is_stale_old_data_returns_true():
    """Period from 1990 must be stale for any reasonable cadence."""
    assert is_stale("1990-01-01T00:00:00+10:00", 300) is True


def test_is_stale_none_period_is_stale():
    assert is_stale(None, 300) is True
    assert is_stale("", 300) is True


def test_is_stale_garbage_period_is_stale():
    assert is_stale("not a date", 300) is True


def test_is_stale_recent_data_not_stale():
    """A future-ish period must not be stale."""
    from datetime import datetime
    near_future = datetime.now(NEM_TZ).isoformat()
    assert is_stale(near_future, 300) is False


def test_build_response_records_format():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"}
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={"filters": {"region": "NSW1"}},
        source_url="http://nemweb.com.au/test/",
        start_period=None,
        end_period=None,
    )
    assert resp.dataset_id == "dispatch_price"
    assert len(resp.records) == 1
    assert resp.records[0].value == 87.5  # type: ignore
    assert resp.source == "Australian Energy Market Operator"
    assert "AEMO" in resp.attribution
    assert resp.source_url == "http://nemweb.com.au/test/"
    assert resp.csv is None


def test_build_response_csv_format():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"}
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="csv",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert resp.csv is not None
    assert "NSW1" in resp.csv
    assert resp.records == []


def test_build_response_series_format():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"},
        {"SETTLEMENTDATE": "2026/05/14 10:05:00", "REGIONID": "NSW1", "RRP": "88.0"},
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="series",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert len(resp.records) == 1  # One series (NSW1)
    series = resp.records[0]
    assert "observations" in series  # type: ignore[operator]


def test_build_response_interval_bounds_derived_from_data():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"},
        {"SETTLEMENTDATE": "2026/05/14 10:05:00", "REGIONID": "NSW1", "RRP": "88.0"},
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert resp.interval_start is not None
    assert resp.interval_end is not None
    assert resp.interval_start <= resp.interval_end


def test_build_response_homogeneous_unit():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"}
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert resp.unit == "$/MWh"


def test_build_response_marks_stale_for_old_data():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "1990/01/01 00:00:00", "REGIONID": "NSW1", "RRP": "87.5"}
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert resp.stale is True


def test_build_response_carries_server_version():
    dataset = _fake_dispatch_price()
    resp = build_response(
        dataset=dataset,
        rows=[],
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert resp.server_version  # not empty


def test_build_response_attribution_mentions_aemo():
    dataset = _fake_dispatch_price()
    resp = build_response(
        dataset=dataset,
        rows=[],
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period=None,
        end_period=None,
    )
    assert "AEMO" in resp.attribution
    assert "Copyright Permissions" in resp.attribution


def test_build_response_explicit_period_bounds_preserved():
    dataset = _fake_dispatch_price()
    rows = [
        {"SETTLEMENTDATE": "2026/05/14 10:00:00", "REGIONID": "NSW1", "RRP": "87.5"}
    ]
    resp = build_response(
        dataset=dataset,
        rows=rows,
        sections_with_discriminator=None,
        fmt="records",
        user_query={},
        source_url="http://x/",
        start_period="2026-05-14",
        end_period="2026-05-15",
    )
    assert resp.interval_start == "2026-05-14"
    assert resp.interval_end == "2026-05-15"
