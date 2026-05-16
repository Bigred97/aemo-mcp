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

This module exposes two parser shapes:

- `parse_csv(body)` — builds the full list of Sections in memory (eager).
  Used for small-volume datasets and the dedup-across-versions branch.
- `iter_csv_rows(body, target_section=...)` — streaming iterator that
  yields `(section_name, version, row_dict)` tuples one at a time without
  ever holding more than one row in memory. Optional `target_section`
  short-circuits parsing for unwanted sections — critical for the high-cadence
  5-minute feeds (`dispatch_price`, `generation_scada`, `interconnector_flows`)
  where peak RSS used to spike to >100MB on archive windows.

Records are plain dicts keyed by the section's column names.

No pandas at this layer — stdlib `csv` + `zipfile` + `io.BytesIO` is enough.
"""
from __future__ import annotations

import csv
import zipfile
from collections.abc import Iterator
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


def iter_csv_rows(
    body: bytes,
    target_section: str | None = None,
) -> Iterator[tuple[str, str, dict[str, str]]]:
    """Streaming iterator over an AEMO multi-section CSV body.

    Yields `(section_name, version, row_dict)` tuples one at a time without
    materialising the full sections list. The caller filters rows inline so
    peak memory stays O(1) rather than O(rows).

    `target_section` (case-insensitive) short-circuits parsing — D-rows for
    other sections are skipped without dict construction. The "DREGION."
    daily-archive dual-version case is fine here because matching is by name
    (both v2 and v3 yield the same section_name).

    Used by fetch.py for the high-cadence path (dispatch_price,
    generation_scada, interconnector_flows). For datasets that fan out across
    multiple sections in one file, pass `target_section=None` and the caller
    discriminates on the yielded section_name.
    """
    if not body:
        raise AEMOParseError("CSV body is empty")
    text = body.decode("utf-8-sig", errors="replace")
    reader = csv.reader(StringIO(text))
    target_norm = target_section.strip().upper() if target_section else None

    current_name: str | None = None
    current_version: str = ""
    current_columns: list[str] = []
    skip_d_rows = False  # True when current section isn't the target

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
            current_name = name
            current_version = row[3].strip()
            current_columns = [c.strip() for c in row[4:]]
            skip_d_rows = (
                target_norm is not None and name.upper() != target_norm
            )
            continue
        if tag == "D":
            if current_name is None:
                raise AEMOParseError(
                    f"row {row_idx}: D-row before any I-row: {row!r}"
                )
            if skip_d_rows:
                continue
            values = row[4:]
            record: dict[str, str] = {}
            n_cols = len(current_columns)
            for i in range(n_cols):
                record[current_columns[i]] = (
                    values[i].strip() if i < len(values) else ""
                )
            yield (current_name, current_version, record)
            continue
        # Unknown tag — skip (forward-compat with AEMO additions).


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
