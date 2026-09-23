"""Deterministic, conservative case-law corpus and count tables."""
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import date
import csv
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from policy_corpus_builder.classification import classify_case_law
from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.postprocess import deduplicate_documents
from policy_corpus_builder.schemas import NormalizationConfig
from .jsonl import export_documents_jsonl

UNSUPPORTED_SOURCES = {
    "UK": "legislation.gov.uk legislation search",
    "CA": "publications.gc.ca publications search",
    "AUS": "legislation.gov.au legislation search",
    "NZ": "New Zealand legislation search",
    "US": "regulations.gov regulatory documents",
}


@dataclass(frozen=True)
class CaseLawExportResult:
    status: str
    document_count: int
    corpus_path: Path
    count_table_paths: dict[str, Path]
    overview_path: Path
    supported_jurisdictions: tuple[str, ...]
    unsupported_jurisdictions: tuple[str, ...]
    warnings: tuple[str, ...]


def _year(document):
    value = document.publication_date or ""
    try:
        return str(date.fromisoformat(value[:10]).year)
    except ValueError:
        return "unknown"


def export_case_law(documents: tuple[NormalizedDocument, ...], *, output_dir: Path,
                    jurisdictions: tuple[str, ...], fulltext: bool = False) -> CaseLawExportResult:
    """Filter ordinary results; no targeted retrieval or title-based inference."""
    output_dir.mkdir(parents=True, exist_ok=True)
    supported = tuple(j for j in jurisdictions if j == "EU")
    unsupported = tuple(j for j in jurisdictions if j != "EU")
    warnings = ["Case law found in the ordinary policy-query result surface; not a complete or targeted case-law corpus."]
    warnings.extend(f"{j}: case-law retrieval unsupported ({UNSUPPORTED_SOURCES.get(j, 'unverified source')}); no reliable provider court-decision signal. No case-law search or zero count is asserted." for j in unsupported)
    accepted, reasons = [], Counter()
    for document in documents:
        decision = classify_case_law(document)
        if not decision.is_case_law:
            reasons[decision.basis] += 1
            continue
        metadata = dict(document.raw_metadata)
        metadata.update(case_law_category=decision.category, case_law_basis=decision.basis)
        accepted.append(replace(document, raw_metadata=metadata,
                                document_type=decision.category,
                                full_text=document.full_text if fulltext else None,
                                content_path=document.content_path if fulltext else None))
        if decision.warning and decision.warning not in warnings:
            warnings.append(decision.warning)
    dedup = deduplicate_documents(tuple(accepted), config=NormalizationConfig(
        deduplicate=True, deduplicate_fields=("document_id",)))
    unique = tuple(sorted(dedup.documents, key=lambda d: d.document_id))
    status = ("written" if unique else "unsupported_jurisdictions" if not supported
              else "written_empty" if not documents else "no_reliable_records")
    groups_year, groups_type = defaultdict(list), defaultdict(list)
    for doc in unique:
        category = doc.raw_metadata["case_law_category"]
        jurisdiction = "EU"  # sector 8 remains an EU reference surface, not a country attribution
        groups_year[jurisdiction, _year(doc), category, doc.raw_metadata["case_law_basis"]].append(doc)
        groups_type[jurisdiction, doc.document_type or "unknown", category].append(doc)
    def counts(group):
        return {"case_count": len(group), "unique_source_document_count": len({
            (d.source_name, d.source_document_id or d.document_id) for d in group})}
    year_rows = [{**dict(zip(("jurisdiction", "year", "case_law_category", "classification_basis"), key)), **counts(group)}
                 for key, group in sorted(groups_year.items())]
    type_rows = [{**dict(zip(("jurisdiction", "document_type", "case_law_category"), key)), **counts(group)}
                 for key, group in sorted(groups_type.items())]
    table_paths = {
        "by_jurisdiction_year": output_dir / "case_law_counts_by_jurisdiction_year.csv",
        "by_document_type": output_dir / "case_law_counts_by_document_type.csv",
    }
    corpus_path = output_dir / "documents.jsonl"
    overview_path = output_dir / "case_law_overview.json"
    overview = {
        "schema_version": "1.0", "status": status,
        "classification_criteria": {"eu_case_law": "CELEX sector 6", "national_case_law_eu_reference": "CELEX sector 8",
            "exclusions": "Any sector 0/1 evidence, conflicting sectors, or treaty/accession/annex/protocol/consolidated instrument titles. Generic labels and title keywords never establish case law."},
        "supported_jurisdictions": list(supported), "unsupported_jurisdictions": list(unsupported),
        "supported_categories": ["eu_case_law"], "experimental_categories": ["national_case_law_eu_reference"],
        "input_record_count": len(documents), "accepted_record_count": len(accepted),
        "rejected_record_count": sum(reasons.values()), "rejection_counts_by_reason": dict(sorted(reasons.items())),
        "document_count": len(unique), "duplicates_removed": dedup.duplicates_removed,
        "missing_year_count": sum(_year(d) == "unknown" for d in unique),
        "missing_year_behavior": "Included in an unknown year row, never dropped; year uses normalized publication_date.",
        "count_unit": "Deduplicated normalized source documents, not distinct proceedings; judgments, orders and opinions count separately.",
        "deduplication_fields": ["document_id"], "unique_source_document_fields": ["source_name", "source_document_id (document_id fallback)"],
        "fulltext_requested": fulltext, "fulltext_document_count": sum(bool(d.full_text) for d in unique),
        "fulltext_behavior": "Reuse ordinary EUR-Lex text retrieval when requested; metadata survives missing/failed full text. Text and content_path omitted otherwise.",
        "retrieval_surface": "ordinary_policy_query_results", "targeted_retrieval": False,
        "limitations": ["Search terms, pagination and upstream retrieval failures constrain coverage.",
                        "An empty corpus is not evidence of no case law in a jurisdiction.",
                        "Sector 8 jurisdiction EU denotes the reference surface; national court country is not inferred."],
        "warnings": warnings,
        "output_paths": {"corpus": str(corpus_path), "overview": str(overview_path), **{k: str(v) for k,v in table_paths.items()}},
    }
    # Stage all artifacts before replacing destinations; overview is the final completion marker.
    with TemporaryDirectory(prefix=".case-law-", dir=output_dir) as tmp:
        staging = Path(tmp)
        export_documents_jsonl(unique, output_dir=staging)
        for key, rows, fields in (
            ("by_jurisdiction_year", year_rows, ["jurisdiction", "year", "case_law_category", "case_count", "unique_source_document_count", "classification_basis"]),
            ("by_document_type", type_rows, ["jurisdiction", "document_type", "case_law_category", "case_count", "unique_source_document_count"]),
        ):
            with (staging / table_paths[key].name).open("w", newline="", encoding="utf-8-sig") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
        (staging / overview_path.name).write_text(json.dumps(overview, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        for path in (corpus_path, *table_paths.values(), overview_path):
            (staging / path.name).replace(path)
    return CaseLawExportResult(status, len(unique), corpus_path, table_paths, overview_path,
                               supported, unsupported, tuple(warnings))
