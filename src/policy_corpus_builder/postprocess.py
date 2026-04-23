"""Post-processing helpers for normalized documents."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime

from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.schemas import NormalizationConfig


@dataclass(frozen=True, slots=True)
class DeduplicationResult:
    """Result of deterministic document deduplication."""

    documents: tuple[NormalizedDocument, ...]
    duplicates_removed: int


JURISDICTION_LABELS_BY_CODE = {
    "EU": "European Union",
    "UK": "United Kingdom",
    "CA": "Canada",
    "AUS": "Australia",
    "NZ": "New Zealand",
    "US": "United States",
}

JURISDICTION_ALIASES = {
    "australia": "Australia",
    "canada": "Canada",
    "eu": "European Union",
    "european union": "European Union",
    "new zealand": "New Zealand",
    "nz": "New Zealand",
    "uk": "United Kingdom",
    "united kingdom": "United Kingdom",
    "united states": "United States",
    "united states of america": "United States",
    "us": "United States",
    "usa": "United States",
}

DOCUMENT_TYPE_ALIASES = {
    "commission document (green paper, white paper, report, communication)": "eu_communication",
    "commission proposal for legal act (com proposal)": "eu_proposal",
    "commission staff working document (swd / sec)": "eu_staff_working_document",
    "communication / information": "eu_communication",
    "committee of the regions opinion": "eu_opinion",
    "decision": "eu_decision",
    "decision (agreement-related)": "eu_decision",
    "directive": "eu_directive",
    "eu_document": "eu_document",
    "eu_legal_document": "eu_document",
    "european parliament legislative resolution": "eu_resolution",
    "national_implementation_measure": "national_implementation_measure",
    "opinion": "eu_opinion",
    "policy_document": "policy_document",
    "regulation": "eu_regulation",
    "tribunal case": "eu_case_law",
    "unknown descriptor": "eu_document",
}


def clean_documents_for_downstream_analysis(
    documents: tuple[NormalizedDocument, ...],
    *,
    expected_jurisdiction_code: str | None = None,
) -> tuple[NormalizedDocument, ...]:
    """Apply conservative final-corpus cleanup for downstream analysis."""

    return tuple(
        clean_document_for_downstream_analysis(
            document,
            expected_jurisdiction_code=expected_jurisdiction_code,
        )
        for document in documents
    )


def clean_document_for_downstream_analysis(
    document: NormalizedDocument,
    *,
    expected_jurisdiction_code: str | None = None,
) -> NormalizedDocument:
    """Clean one normalized document without changing its identity."""

    raw_metadata = dict(document.raw_metadata)
    cleaned_title = _clean_title(document.title)
    cleaned_summary = _clean_optional_text(document.summary)
    cleaned_document_type = _harmonize_document_type(document.document_type)
    cleaned_language = _normalize_language(document.language)
    cleaned_jurisdiction = _normalize_jurisdiction(
        document.jurisdiction,
        expected_jurisdiction_code=expected_jurisdiction_code,
    )
    cleaned_publication_date, publication_precision = _normalize_date(document.publication_date)
    cleaned_effective_date, effective_precision = _normalize_date(document.effective_date)
    cleaned_full_text = _clean_full_text(document.full_text)

    _record_original(raw_metadata, "_original_title", document.title, cleaned_title)
    _record_original(
        raw_metadata,
        "_original_document_type",
        document.document_type,
        cleaned_document_type,
    )
    _record_original(raw_metadata, "_original_language", document.language, cleaned_language)
    _record_original(
        raw_metadata,
        "_original_jurisdiction",
        document.jurisdiction,
        cleaned_jurisdiction,
    )
    _record_original(
        raw_metadata,
        "_original_publication_date",
        document.publication_date,
        cleaned_publication_date,
    )
    _record_original(
        raw_metadata,
        "_original_effective_date",
        document.effective_date,
        cleaned_effective_date,
    )
    if publication_precision:
        raw_metadata["_publication_date_precision"] = publication_precision
    if effective_precision:
        raw_metadata["_effective_date_precision"] = effective_precision
    if document.full_text and cleaned_full_text is None:
        raw_metadata["_full_text_removed_reason"] = "source_boilerplate_or_empty_after_cleaning"

    return replace(
        document,
        title=cleaned_title,
        summary=cleaned_summary,
        document_type=cleaned_document_type,
        language=cleaned_language,
        jurisdiction=cleaned_jurisdiction,
        publication_date=cleaned_publication_date,
        effective_date=cleaned_effective_date,
        full_text=cleaned_full_text,
        raw_metadata=raw_metadata,
    )


def deduplicate_documents(
    documents: tuple[NormalizedDocument, ...],
    *,
    config: NormalizationConfig,
) -> DeduplicationResult:
    """Deduplicate normalized documents using configured normalized fields.

    The deduplication key is a tuple of `(field_name, field_value)` pairs in the
    configured field order. Missing values are represented as `None`. When two
    documents collide on the same key, the first document encountered is retained.
    """

    if not config.deduplicate:
        return DeduplicationResult(documents=documents, duplicates_removed=0)

    seen_keys: set[tuple[tuple[str, object], ...]] = set()
    unique_documents: list[NormalizedDocument] = []

    for document in documents:
        dedup_key = build_deduplication_key(document, config.deduplicate_fields)
        if dedup_key in seen_keys:
            continue

        seen_keys.add(dedup_key)
        unique_documents.append(document)

    return DeduplicationResult(
        documents=tuple(unique_documents),
        duplicates_removed=len(documents) - len(unique_documents),
    )


def build_deduplication_key(
    document: NormalizedDocument,
    field_names: tuple[str, ...],
) -> tuple[tuple[str, object], ...]:
    """Build a deterministic deduplication key from configured fields."""

    return tuple((field_name, getattr(document, field_name, None)) for field_name in field_names)


def _clean_title(value: str | None) -> str | None:
    cleaned = _clean_inline_text(value)
    if not cleaned:
        return None
    if cleaned.lower() in {"none", "n/a", "na", "null", "untitled"}:
        return None
    return cleaned


def _clean_optional_text(value: str | None) -> str | None:
    return _clean_inline_text(value)


def _clean_inline_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).replace("\ufeff", "").replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _harmonize_document_type(value: str | None) -> str | None:
    cleaned = _clean_inline_text(value)
    if not cleaned:
        return None
    alias = DOCUMENT_TYPE_ALIASES.get(cleaned.lower())
    if alias:
        return alias
    slug = re.sub(r"[^a-z0-9]+", "_", cleaned.lower()).strip("_")
    return slug or None


def _normalize_language(value: str | None) -> str | None:
    cleaned = _clean_inline_text(value)
    if not cleaned:
        return None
    return cleaned.lower()


def _normalize_jurisdiction(
    value: str | None,
    *,
    expected_jurisdiction_code: str | None,
) -> str | None:
    if expected_jurisdiction_code:
        label = JURISDICTION_LABELS_BY_CODE.get(expected_jurisdiction_code.upper())
        if label:
            return label
    cleaned = _clean_inline_text(value)
    if not cleaned:
        return None
    return JURISDICTION_ALIASES.get(cleaned.lower(), cleaned)


def _normalize_date(value: str | None) -> tuple[str | None, str | None]:
    cleaned = _clean_inline_text(value)
    if not cleaned:
        return None, None

    if re.fullmatch(r"\d{4}", cleaned):
        return f"{cleaned}-01-01", "year"
    if re.fullmatch(r"\d{4}-\d{2}", cleaned):
        candidate = f"{cleaned}-01"
        return (candidate, "month") if _is_valid_iso_date(candidate) else (cleaned, None)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", cleaned):
        return (cleaned, "day") if _is_valid_iso_date(cleaned) else (cleaned, None)

    slash_match = re.fullmatch(r"(\d{4})/(\d{1,2})/(\d{1,2})", cleaned)
    if slash_match:
        candidate = (
            f"{slash_match.group(1)}-"
            f"{int(slash_match.group(2)):02d}-"
            f"{int(slash_match.group(3)):02d}"
        )
        return (candidate, "day") if _is_valid_iso_date(candidate) else (cleaned, None)

    dotted_match = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", cleaned)
    if dotted_match:
        candidate = (
            f"{dotted_match.group(3)}-"
            f"{int(dotted_match.group(2)):02d}-"
            f"{int(dotted_match.group(1)):02d}"
        )
        return (candidate, "day") if _is_valid_iso_date(candidate) else (cleaned, None)

    return cleaned, None


def _is_valid_iso_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _clean_full_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).replace("\ufeff", "").replace("\xa0", " ")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if not cleaned:
        return None

    cleaned = _remove_eurlex_consolidated_text_boilerplate(cleaned)
    cleaned = _remove_email_warning_boilerplate(cleaned)
    if _is_schema_boilerplate_text(cleaned):
        return None

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned or None


def _remove_eurlex_consolidated_text_boilerplate(text: str) -> str:
    lines = text.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    if lines and lines[0].strip().lower().startswith("consolidated text:"):
        lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)
        if lines and re.fullmatch(r"0?[0-9A-Z]{8,}.*", lines[0].strip()):
            lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)

    text = "\n".join(lines).strip()
    text = re.sub(
        r"(?is)^This text is meant purely as a documentation tool and has no legal effect\."
        r"\s*The Union's institutions do not assume any liability for its contents\.\s*",
        "",
        text,
    )
    return text.strip()


def _remove_email_warning_boilerplate(text: str) -> str:
    return re.sub(
        r"(?is)^Caution:\s*This email originated from outside EPA,?\s*"
        r"please exercise additional caution when deciding whether to open attachments "
        r"or click links from this email\.\s*",
        "",
        text,
    ).strip()


def _is_schema_boilerplate_text(text: str) -> bool:
    compact = re.sub(r"\s+", " ", text).strip().lower()
    return compact.startswith("xml schema xml schema") and (
        "this document describes the xml schema" in compact
    )


def _record_original(
    raw_metadata: dict[str, object],
    key: str,
    original: object,
    cleaned: object,
) -> None:
    if original != cleaned and original is not None and key not in raw_metadata:
        raw_metadata[key] = original
