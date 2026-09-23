"""Conservative case-law evidence rules, independent of retrieval and export."""
from dataclasses import dataclass
import re

from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.utils.celex import parse_celex


@dataclass(frozen=True)
class CaseLawDecision:
    is_case_law: bool
    category: str | None
    basis: str
    warning: str | None = None


def metadata_layers(document: NormalizedDocument) -> tuple[dict, ...]:
    """Read both normalized metadata and the adapter's preserved raw record."""
    raw = document.raw_metadata
    nested = raw.get("raw_record")
    return (raw, nested) if isinstance(nested, dict) else (raw,)


def classify_case_law(document: NormalizedDocument) -> CaseLawDecision:
    sectors = set()
    for layer in metadata_layers(document):
        sector = str(layer.get("celex_sector", "")).strip()
        if sector:
            sectors.add(sector)
        label = str(layer.get("celex_sector_label", "")).strip().lower()
        if label in {"consolidated texts", "treaties"}:
            sectors.add("0" if label == "consolidated texts" else "1")
        match = re.match(r"sector_([0-9])(?:_|$)", str(layer.get("celex_class", "")))
        if match:
            sectors.add(match[1])
        for key in ("celex", "celex_full"):
            info = parse_celex(layer.get(key))
            if info.valid:
                sectors.add(info.sector)
    info = parse_celex(document.source_document_id)
    if info.valid:
        sectors.add(info.sector)
    if sectors & {"0", "1"}:
        return CaseLawDecision(False, None, "excluded_celex_sector_0_or_1")
    title = re.sub(r"\s+", " ", document.title or "").strip().lower()
    # Exclude instrument titles, not judgments merely discussing a treaty.
    if re.match(
        r"^(?:treat(?:y|ies)\b|protocol\b|annex\b|"
        r"act concerning (?:the )?conditions of accession\b|act of accession\b|"
        r"accession.*\bannex\b|"
        r"consolidated (?:version|text|treaty)\b|amendments? to (?:the )?treaty\b)", title
    ):
        return CaseLawDecision(False, None, "excluded_instrument_title")
    jurisdiction = (document.jurisdiction or "").strip().lower()
    if jurisdiction not in {"", "eu", "european union"}:
        return CaseLawDecision(False, None, "unsupported_jurisdiction")
    if len(sectors) > 1:
        return CaseLawDecision(False, None, "conflicting_celex_metadata", "Conflicting CELEX sectors.")
    if sectors == {"6"}:
        return CaseLawDecision(True, "eu_case_law", "celex_sector_6")
    if sectors == {"8"}:
        return CaseLawDecision(True, "national_case_law_eu_reference", "celex_sector_8",
                               "Sector 8 is experimental and separate from EU court case law.")
    return CaseLawDecision(False, None, "no_reliable_case_law_evidence")
