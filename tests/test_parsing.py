"""Multi-section AEMO CSV parser tests + ZIP unpack."""
from __future__ import annotations

import io
import zipfile

import pytest

from aemo_mcp.parsing import AEMOParseError, find_section, parse_csv, unzip
from tests.conftest import (
    DISPATCH_IS_SAMPLE,
    DISPATCH_SCADA_SAMPLE,
    ROOFTOP_ACTUAL_SAMPLE,
    make_zip,
)


def test_parse_returns_three_sections_for_dispatchis():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    names = {s.name for s in sections}
    assert names == {"DISPATCH.PRICE", "DISPATCH.REGIONSUM", "DISPATCH.INTERCONNECTORRES"}


def test_parse_dispatch_price_has_five_regions():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    price = find_section(sections, "DISPATCH.PRICE")
    assert price is not None
    assert len(price.records) == 5
    regions = {r["REGIONID"] for r in price.records}
    assert regions == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


def test_parse_dispatch_price_columns_match():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    price = find_section(sections, "DISPATCH.PRICE")
    assert price.columns == [
        "SETTLEMENTDATE",
        "RUNNO",
        "REGIONID",
        "DISPATCHINTERVAL",
        "INTERVENTION",
        "RRP",
        "EEP",
    ]


def test_parse_negative_price_preserved():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    price = find_section(sections, "DISPATCH.PRICE")
    sa = next(r for r in price.records if r["REGIONID"] == "SA1")
    assert sa["RRP"] == "-15.40"


def test_parse_regionsum_total_demand():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    rs = find_section(sections, "DISPATCH.REGIONSUM")
    assert rs is not None
    nsw = next(r for r in rs.records if r["REGIONID"] == "NSW1")
    assert nsw["TOTALDEMAND"] == "8500.0"


def test_parse_interconnector_six_links():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    ic = find_section(sections, "DISPATCH.INTERCONNECTORRES")
    assert ic is not None
    ids = {r["INTERCONNECTORID"] for r in ic.records}
    assert ids == {"N-Q-MNSP1", "NSW1-QLD1", "T-V-MNSP1", "V-S-MNSP1", "V-SA", "VIC1-NSW1"}


def test_parse_scada_returns_unit_scada():
    sections = parse_csv(DISPATCH_SCADA_SAMPLE)
    assert len(sections) == 1
    assert sections[0].name == "DISPATCH.UNIT_SCADA"
    assert len(sections[0].records) == 10


def test_parse_rooftop_actual_section_name():
    sections = parse_csv(ROOFTOP_ACTUAL_SAMPLE)
    assert sections[0].name == "ROOFTOP.ACTUAL"


def test_parse_empty_body_raises():
    with pytest.raises(AEMOParseError, match="empty"):
        parse_csv(b"")


def test_parse_d_before_i_raises():
    csv = b"D,DISPATCH,PRICE,1,fake,fake\n"
    with pytest.raises(AEMOParseError, match="before any I-row"):
        parse_csv(csv)


def test_parse_short_i_row_raises():
    csv = b"I,DISPATCH\n"
    with pytest.raises(AEMOParseError, match="fewer than 4 cells"):
        parse_csv(csv)


def test_parse_unknown_tag_silently_ignored():
    """Forward-compat: unknown row tags must not break parsing."""
    csv = b"""C,header
I,DISPATCH,PRICE,1,SETTLEMENTDATE,RRP
Z,DISPATCH,PRICE,1,2026/01/01 00:00:00,50
D,DISPATCH,PRICE,1,2026/01/01 00:00:00,60
"""
    sections = parse_csv(csv)
    assert len(sections) == 1
    assert len(sections[0].records) == 1
    assert sections[0].records[0]["RRP"] == "60"


def test_parse_short_d_row_pads_with_empty_strings():
    csv = b"""I,DISPATCH,PRICE,1,SETTLEMENTDATE,REGIONID,RRP
D,DISPATCH,PRICE,1,2026/01/01 00:00:00,NSW1
"""
    sections = parse_csv(csv)
    rec = sections[0].records[0]
    assert rec["SETTLEMENTDATE"] == "2026/01/01 00:00:00"
    assert rec["REGIONID"] == "NSW1"
    assert rec["RRP"] == ""


def test_find_section_case_insensitive():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    assert find_section(sections, "dispatch.price") is not None
    assert find_section(sections, "DISPATCH.PRICE") is not None
    assert find_section(sections, "Dispatch.Price") is not None


def test_find_section_unknown_returns_none():
    sections = parse_csv(DISPATCH_IS_SAMPLE)
    assert find_section(sections, "DISPATCH.NONESUCH") is None


def test_unzip_simple_csv():
    z = make_zip("inner.CSV", DISPATCH_IS_SAMPLE)
    out = unzip(z)
    assert "inner.CSV" in out
    assert out["inner.CSV"] == DISPATCH_IS_SAMPLE


def test_unzip_empty_bytes_raises():
    with pytest.raises(AEMOParseError, match="ZIP body is empty"):
        unzip(b"")


def test_unzip_invalid_zip_raises():
    with pytest.raises(AEMOParseError, match="Not a valid ZIP"):
        unzip(b"definitely not a zip file")


def test_unzip_skips_directory_entries():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("nested/", b"")
        zf.writestr("nested/inner.csv", DISPATCH_IS_SAMPLE)
    out = unzip(buf.getvalue())
    assert list(out.keys()) == ["nested/inner.csv"]


def test_unzip_zip_bomb_defence_triggers():
    """A pathological compression ratio must be rejected."""
    # Create a ZIP whose uncompressed size dwarfs its compressed size.
    payload = b"A" * (10 * 1024 * 1024)  # 10 MB of one byte → highly compressible
    z = make_zip("big.csv", payload)
    # The compression ratio for ~10MB of one byte is well over 1000:1.
    with pytest.raises(AEMOParseError, match="compression ratio"):
        unzip(z)


def test_parse_handles_quoted_dates():
    csv = b"""I,DISPATCH,PRICE,1,SETTLEMENTDATE,REGIONID,RRP
D,DISPATCH,PRICE,1,"2026/05/14 10:00:00",NSW1,87.5
"""
    sections = parse_csv(csv)
    rec = sections[0].records[0]
    assert rec["SETTLEMENTDATE"] == "2026/05/14 10:00:00"


def test_parse_strips_whitespace_in_cells():
    csv = b"""I,DISPATCH,PRICE,1,REGIONID,RRP
D,DISPATCH,PRICE,1,  NSW1  ,  87.5
"""
    sections = parse_csv(csv)
    rec = sections[0].records[0]
    assert rec["REGIONID"] == "NSW1"
    assert rec["RRP"] == "87.5"


def test_parse_handles_utf8_bom():
    csv = b"\xef\xbb\xbfI,DISPATCH,PRICE,1,REGIONID,RRP\nD,DISPATCH,PRICE,1,NSW1,87.5\n"
    sections = parse_csv(csv)
    assert sections[0].records[0]["REGIONID"] == "NSW1"


def test_parse_skips_comment_rows():
    csv = b"""C,comment 1
C,comment 2
I,DISPATCH,PRICE,1,REGIONID,RRP
D,DISPATCH,PRICE,1,NSW1,87.5
C,END OF REPORT,1
"""
    sections = parse_csv(csv)
    assert len(sections) == 1
    assert len(sections[0].records) == 1


def test_parse_section_version_captured():
    csv = b"I,DISPATCH,PRICE,3,REGIONID,RRP\nD,DISPATCH,PRICE,3,NSW1,87.5\n"
    sections = parse_csv(csv)
    assert sections[0].version == "3"
