"""Utilities for parsing and looking up CELEX identifiers.

The parser follows the structure described by EUR-Lex guidance:
Sector - Year - Descriptor (document type) - Number (+ optional suffixes).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any

SECTOR_LABELS = {
    "0": "Consolidated texts",
    "1": "Treaties",
    "2": "International agreements",
    "3": "Legal acts",
    "4": "Complementary legislation",
    "5": "Preparatory documents",
    "6": "EU case-law",
    "7": "National transposition",
    "8": "National case-law relating to EU law",
    "9": "Parliamentary questions",
    "C": "Other documents published in OJ C series",
    "E": "EFTA documents",
}

# Sector-specific descriptor mappings
DESCRIPTOR_LABELS = {
    # --- SECTOR 1: Treaties ---
    ("1", "K"): "Treaty establishing the ECSC",
    ("1", "E"): "Treaty establishing the EEC / EC",
    ("1", "M"): "Treaty on European Union (Maastricht)",
    ("1", "A"): "Amsterdam Treaty",
    ("1", "N"): "Nice Treaty",
    ("1", "L"): "Lisbon Treaty",

    # --- SECTOR 2: International agreements ---
    ("2", "A"): "Agreement",
    ("2", "D"): "Decision (agreement-related)",
    ("2", "P"): "Protocol",

    # --- SECTOR 3: Legal acts ---
    ("3", "R"): "Regulation",
    ("3", "L"): "Directive",
    ("3", "D"): "Decision",
    ("3", "F"): "Framework decision (pre-Lisbon)",
    ("3", "H"): "Recommendation",
    ("3", "A"): "Agreement (legal act form)",
    ("3", "B"): "Budget act",
    ("3", "C"): "Communication",
    ("3", "E"): "Common position",
    ("3", "G"): "Resolution",
    ("3", "J"): "Joint action",
    ("3", "K"): "Recommendation ECSC",
    ("3", "M"): "Decision ECSC",
    ("3", "P"): "Proposal for legal act",
    # --- SECTOR 4: Complementary legislation ---
    ("4", "X"): "Complementary legislation document",

    # --- SECTOR 5: Preparatory acts ---
    ("5", "PC"): "Commission proposal for legal act (COM proposal)",
    ("5", "DC"): "Commission document (Green paper, White paper, report, communication)",
    ("5", "SC"): "Commission staff working document (SWD / SEC)",
    ("5", "SWD"): "Staff working document",
    ("5", "SEC"): "Staff working document (older code)",
    ("5", "JC"): "JOIN document (Joint Communication)",
    ("5", "JOIN"): "Joint Communication",
    ("5", "IP"): "European Parliament resolution / preparatory act",
    ("5", "AR"): "Committee of the Regions opinion",
    ("5", "AE"): "European Economic and Social Committee opinion",
    ("5", "BP"): "European Central Bank opinion",
    ("5", "IR"): "Interinstitutional report",
    ("5", "XR"): "Court of Auditors opinion",
    ("5", "SA"): "Council statement",
    ("5", "AP"): "European Parliament legislative resolution",
    ("5", "DMA"): "Digital Markets Act preparatory document",
    ("5", "DSA"): "Digital Services Act preparatory document",
    # --- SECTOR 6: Case law ---
    ("6", "CJ"): "Judgment of the Court of Justice",
    ("6", "CC"): "Opinion of the Advocate General",
    ("6", "CO"): "Order of the Court",
    ("6", "CJ"): "Court of Justice judgment",
    ("6", "TJ"): "General Court judgment",
    ("6", "TC"): "Civil Service Tribunal judgment",
    ("6", "TR"): "Tribunal order",
    ("6", "TO"): "Order of the General Court",
    # --- SECTOR 7: National transposition ---
    ("7", "L"): "National implementing measure",
    # --- SECTOR 8: National case-law referring to EU law ---
    ("8", "CJ"): "National court decision citing EU law",
    # --- SECTOR 9: Parliamentary questions ---
    ("9", "E"): "Written question",
    ("9", "O"): "Oral question",
    ("9", "H"): "Question time",
    # --- OJ C series ---
    ("C", "A"): "Information notice",
    ("C", "C"): "Communication in OJ C series",
    ("C", "R"): "Resolution published in OJ C",
    # --- EFTA ---
    ("E", "J"): "EFTA Court judgment",
    ("E", "O"): "EFTA Surveillance Authority decision",
}

# fallback mapping if sector-specific mapping is unavailable
GENERIC_DESCRIPTOR_LABELS = {
    "A": "Agreement / Act / Notice",
    "AE": "EESC opinion",
    "AP": "Parliament resolution",
    "AR": "Committee of the Regions opinion",
    "B": "Budgetary act",
    "BP": "ECB opinion",
    "C": "Communication / Information",
    "CJ": "Court judgment",
    "CO": "Court order",
    "D": "Decision",
    "DC": "Commission document",
    "E": "Treaty / EEA related document",
    "F": "Framework decision",
    "G": "Resolution / Recommendation",
    "H": "Recommendation",
    "IP": "Parliament document",
    "IR": "Interinstitutional report",
    "J": "Joint action",
    "JC": "Joint communication",
    "JOIN": "Joint communication",
    "K": "ECSC related act",
    "L": "Directive",
    "M": "Decision (ECSC)",
    "N": "National measure",
    "O": "Order / Oral question",
    "P": "Proposal",
    "PC": "Commission proposal",
    "R": "Regulation",
    "S": "Staff document",
    "SC": "Staff working document",
    "SEC": "Staff working document",
    "SWD": "Staff working document",
    "T": "Tribunal case",
    "TC": "Civil Service Tribunal case",
    "TJ": "General Court judgment",
    "TO": "Tribunal order",
    "XR": "Court of Auditors opinion",
}

MAIN_RE = re.compile(
    r"^(?P<sector>[0-9CE])(?P<year>\d{4})(?P<descriptor>[A-Z]{1,4})(?P<doc_number>[A-Z0-9/.-]+?)(?P<tail>R\(\d{2}\))?$",
    re.I,
)

CORR_RE = re.compile(r"R\((?P<n>\d{2})\)$", re.I)

TRANSPO_RE = re.compile(
    r"^7(?P<base>[0-9CE]\d{4}[A-Z]{1,2}\d{4})(?P<country>[A-Z]{3})_(?P<uid>[0-9A-Z*]+)$",
    re.I,
)

CONSOL_RE = re.compile(
    r"^0(?P<base>[0-9CE]\d{4}[A-Z]{1,2}\d{4})(?:-(?P<appdate>\d{8}))?$",
    re.I,
)

CELEX_TOKEN_RE = re.compile(
    r"(?<![A-Z0-9])(?P<celex>[0-9CE]\d{4}[A-Z]{1,2}[A-Z0-9/.-]+(?:R\(\d{2}\))?)(?![A-Z0-9])",
    re.I,
)

@dataclass
class CelexInfo:
    raw: str
    normalized: str
    valid: bool
    sector: str | None = None
    sector_label: str | None = None
    year: int | None = None
    descriptor: str | None = None
    descriptor_label: str | None = None
    document_number: str | None = None
    is_corrigendum: bool = False
    corrigendum_number: int | None = None
    is_consolidated: bool = False
    consolidated_date: str | None = None
    transposed_base_celex: str | None = None
    transposition_country: str | None = None
    transposition_uid: str | None = None
    notes: str | None = None

def _normalize(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", "", str(value).upper())

def extract_celex_token(value: str | None) -> str | None:
    """Extract the first CELEX-like token from arbitrary text."""
    v = _normalize(value)
    if not v:
        return None
    m = CELEX_TOKEN_RE.search(v)
    return m.group("celex") if m else None

def lookup_descriptor(sector: str | None, descriptor: str | None) -> str | None:
    """
    Resolve descriptor label using:
    1. sector-specific mapping
    2. full descriptor fallback
    3. first-letter fallback
    """

    if not descriptor:
        return None

    d = descriptor.upper()

    # 1. sector-specific mapping
    if sector:
        s = sector.upper()
        label = DESCRIPTOR_LABELS.get((s, d))
        if label:
            return label

    # 2. generic full descriptor match (PC, DC, SC, AE, AR etc.)
    label = GENERIC_DESCRIPTOR_LABELS.get(d)
    if label:
        return label

    # 3. generic single-letter fallback
    label = GENERIC_DESCRIPTOR_LABELS.get(d[0])
    if label:
        return label

    return "Unknown descriptor"

def parse_celex(value: str | None) -> CelexInfo:
    raw = "" if value is None else str(value)
    norm = _normalize(raw)
    if not norm:
        return CelexInfo(raw=raw, normalized=norm, valid=False, notes="Empty input")

    # Sector 7: National transposition references the base legal act + country + SG id.
    m7 = TRANSPO_RE.match(norm)
    if m7:
        base = m7.group("base")
        info = CelexInfo(
            raw=raw,
            normalized=norm,
            valid=True,
            sector="7",
            sector_label=SECTOR_LABELS.get("7"),
            year=int(base[1:5]),
            descriptor=base[5:7] if base[6].isalpha() else base[5],
            descriptor_label=lookup_descriptor(base[0], base[5:7] if base[6].isalpha() else base[5]),
            document_number=base[7:] if base[6].isalpha() else base[6:],
            transposed_base_celex=base,
            transposition_country=m7.group("country"),
            transposition_uid=m7.group("uid"),
            notes="Sector 7 transposition CELEX",
        )
        return info

    # Sector 0: Consolidated text (base CELEX + optional application date).
    m0 = CONSOL_RE.match(norm)
    if m0:
        base = m0.group("base")
        appdate = m0.group("appdate")
        info = CelexInfo(
            raw=raw,
            normalized=norm,
            valid=True,
            sector="0",
            sector_label=SECTOR_LABELS.get("0"),
            year=int(base[1:5]),
            descriptor=base[5:7] if base[6].isalpha() else base[5],
            descriptor_label=lookup_descriptor(base[0], base[5:7] if base[6].isalpha() else base[5]),
            document_number=base[7:] if base[6].isalpha() else base[6:],
            is_consolidated=True,
            consolidated_date=appdate,
            notes="Sector 0 consolidated text",
        )
        return info

    # Generic CELEX parsing (includes corrigenda suffix).
    mg = MAIN_RE.match(norm)
    if not mg:
        return CelexInfo(raw=raw, normalized=norm, valid=False, notes="Pattern mismatch")

    sector = mg.group("sector").upper()
    year = int(mg.group("year"))
    descriptor = mg.group("descriptor").upper()
    doc_number = mg.group("doc_number")

    corr_n = None
    is_corr = False
    cm = CORR_RE.search(norm)
    if cm:
        is_corr = True
        corr_n = int(cm.group("n"))

    return CelexInfo(
        raw=raw,
        normalized=norm,
        valid=True,
        sector=sector,
        sector_label=SECTOR_LABELS.get(sector, "Unknown sector"),
        year=year,
        descriptor=descriptor,
        descriptor_label=lookup_descriptor(sector, descriptor),
        document_number=doc_number,
        is_corrigendum=is_corr,
        corrigendum_number=corr_n,
    )

def parse_celex_to_dict(value: str | None) -> dict[str, Any]:
    """Convenience wrapper for DataFrame `.apply` usage."""
    return asdict(parse_celex(value))
