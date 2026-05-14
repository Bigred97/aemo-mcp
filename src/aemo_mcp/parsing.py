"""AEMO multi-section CSV parser + ZIP unpacker.

AEMO publishes most NEMWEB reports as a ZIP containing one or more CSV files.
Each CSV uses AEMO's `C,/I,/D,` row-prefix format:

    C,...                                  ← comment header row
    I,DISPATCH,PRICE,1,SETTLEMENTDATE,...   ← schema row (section start)
    D,DISPATCH,PRICE,1,"2026-05-14 10:00:00",...  ← data row
    D,DISPATCH,PRICE,1,"2026-05-14 10:05:00",...
    I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,...     ← next section starts
    D,DISPATCH,REGIONSUM,1,"2026-05-14 10:00:00","NSW1",...
    ...
    C,END OF REPORT,...                    ← comment footer

The schema row tells you the column names for the rows that follow. The
section name is the concatenation of columns 1 + 2 of the schema row
(e.g. "DISPATCH" + "PRICE" → "DISPATCH.PRICE").

This module parses one CSV body into a list of (section_name, list[record])
tuples. Records are plain dicts keyed by the section's column names.

No pandas at this layer — stdlib `csv` + `zipfile` + `io.BytesIO` is enough.
"""
from __future__ import annotations

import csv
import zipfile
from dataclasses import dataclass, field
from io import BytesIO, StringIO


class AEMOParseError(Exception):
    """Raised when an AEMO CSV doesn't match the expected I/D row structure."""


@dataclass
class Section:
    """One I,/D, section of an AEMO multi-section CSV."""
    name: str                                 # e.g. "DISPATCH.PRICE"
    version: str                              # e.g. "1" (column 3 of the I row)
    columns: list[str] = field(default_factory=list)
    records: list[dict[str, str]] = field(default_factory=list)


def unzip(body: bytes) -> dict[str, bytes]:
    """Unpack a NEMWEB ZIP into {filename: bytes}.

    NEMWEB usually packs ONE CSV per ZIP. We return a dict so the caller can
    walk it in name order if a ZIP-of-CSVs ever appears.
    """
    if not body:
        raise AEMOParseError("ZIP body is empty")
    try:
        zf = zipfile.ZipFile(BytesIO(body))
    except zipfile.BadZipFile as e:
        raise AEMOParseError(f"Not a valid ZIP: {e}") from e
    out: dict[str, bytes] = {}
    for info in zf.infolist():
        if info.is_dir():
            continue
        # Belt-and-braces: refuse absurd compression ratios (ZIP bomb defence).
        if info.compress_size > 0 and info.file_size / info.compress_size > 1000:
            raise AEMOParseError(
                f"ZIP entry {info.filename} has suspicious compression ratio "
                f"({info.file_size / info.compress_size:.0f}x); refusing to unpack."
            )
        out[info.filename] = zf.read(info.filename)
    return out


def parse_csv(body: bytes) -> list[Section]:
    """Parse one AEMO multi-section CSV body into a list of sections.

    Rows starting with `C` are comments (skipped). Rows starting with `I`
    open a new section (their cells 4+ are the column names). Rows starting
    with `D` are data — their cells 4+ map positionally to the most-recent
    `I` row's columns.
    """
    if not body:
        raise AEMOParseError("CSV body is empty")
    text = body.decode("utf-8-sig", errors="replace")
    reader = csv.reader(StringIO(text))

    sections: list[Section] = []
    current: Section | None = None

    for row_idx, row in enumerate(reader):
        if not row:
            continue
        tag = row[0].strip()
        if tag == "C":
            continue
        if tag == "I":
            if len(row) < 4:
                raise AEMOParseError(
                    f"row {row_idx}: I-row has fewer than 4 cells: {row!r}"
                )
            name = f"{row[1].strip()}.{row[2].strip()}"
            version = row[3].strip()
            columns = [c.strip() for c in row[4:]]
            current = Section(name=name, version=version, columns=columns)
            sections.append(current)
            continue
        if tag == "D":
            if current is None:
                raise AEMOParseError(
                    f"row {row_idx}: D-row before any I-row: {row!r}"
                )
            # Skip the tag + section-name + sub-name + version cells.
            values = row[4:]
            record: dict[str, str] = {}
            for i, col in enumerate(current.columns):
                record[col] = values[i].strip() if i < len(values) else ""
            current.records.append(record)
            continue
        # Unknown tag — skip silently (forward-compat with AEMO additions).

    return sections


def find_section(sections: list[Section], name: str) -> Section | None:
    """Look up the first section matching a case-insensitive name."""
    norm = name.strip().upper()
    for s in sections:
        if s.name.upper() == norm:
            return s
    return None


def find_sections(sections: list[Section], name: str) -> list[Section]:
    """Return ALL sections matching a case-insensitive name.

    Some AEMO files (notably the Daily compendium) include two versions of
    the same section (e.g. DREGION. v2 and DREGION. v3 — old + new schema)
    with the same data. The caller is responsible for deduping records.
    """
    norm = name.strip().upper()
    return [s for s in sections if s.name.upper() == norm]
