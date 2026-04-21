"""Public top-level orchestration for multi-jurisdiction policy corpus builds."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from policy_corpus_builder.adapters import get_adapter
from policy_corpus_builder.exporters import export_documents_jsonl, export_run_manifest
from policy_corpus_builder.models import NormalizedDocument, Query
from policy_corpus_builder.pipeline import normalize_adapter_results
from policy_corpus_builder.postprocess import deduplicate_documents
from policy_corpus_builder.schemas import NormalizationConfig, SourceConfig

SUPPORTED_JURISDICTIONS = ("EU", "UK", "CA", "AUS", "NZ", "US")
MAIN_DEDUPLICATION_FIELDS = ("document_id",)
NIM_DEDUPLICATION_FIELDS = ("document_id",)
FINAL_CORPUS_SUBDIR = "final"
INTERMEDIATE_SUBDIR = "jurisdictions"
NIM_SUBDIR = "nim"
CACHE_SUBDIR = "cache"
RUN_MANIFEST_FILENAME = "run-manifest.json"

JURISDICTION_LABELS = {
    "EU": "European Union",
    "UK": "United Kingdom",
    "CA": "Canada",
    "AUS": "Australia",
    "NZ": "New Zealand",
    "US": "United States",
}

try:
    TOOL_VERSION = version("policy-corpus-builder")
except PackageNotFoundError:
    TOOL_VERSION = "0.1.0"


class CorpusBuildValidationError(ValueError):
    """Raised when the public corpus builder inputs are invalid."""


@dataclass(frozen=True, slots=True)
class PolicyCorpusBuildResult:
    """Summary of a completed top-level policy corpus build."""

    outputs_path: Path
    selected_jurisdictions: tuple[str, ...]
    intermediate_paths: dict[str, Path]
    final_corpus_path: Path
    nim_corpus_path: Path | None
    manifest_path: Path
    merged_document_count: int
    final_document_count: int
    duplicates_removed: int
    nim_document_count: int


def build_policy_corpus(
    query_terms: list[str],
    jurisdictions: list[str],
    outputs_path: str | Path,
    include_translations: bool = False,
    translated_terms: list[str] | None = None,
    include_nim: bool = False,
) -> PolicyCorpusBuildResult:
    """Build one normalized policy corpus across supported jurisdictions."""

    cleaned_query_terms = _clean_terms(query_terms, field_name="query_terms", required=True)
    cleaned_jurisdictions = _clean_jurisdictions(jurisdictions)
    cleaned_translated_terms = _clean_terms(
        translated_terms or [],
        field_name="translated_terms",
        required=False,
    )

    output_root = Path(outputs_path).expanduser().resolve()
    cache_root = output_root / CACHE_SUBDIR
    intermediate_root = output_root / INTERMEDIATE_SUBDIR
    final_root = output_root / FINAL_CORPUS_SUBDIR
    nim_root = output_root / NIM_SUBDIR

    cache_root.mkdir(parents=True, exist_ok=True)
    intermediate_root.mkdir(parents=True, exist_ok=True)
    final_root.mkdir(parents=True, exist_ok=True)

    jurisdiction_documents: dict[str, tuple[NormalizedDocument, ...]] = {}
    intermediate_paths: dict[str, Path] = {}

    for jurisdiction in cleaned_jurisdictions:
        documents = _run_jurisdiction(
            jurisdiction,
            query_terms=cleaned_query_terms,
            translated_terms=cleaned_translated_terms,
            include_translations=include_translations,
            output_root=output_root,
            cache_root=cache_root,
        )
        jurisdiction_documents[jurisdiction] = documents

        jurisdiction_output_dir = intermediate_root / jurisdiction.lower()
        path = export_documents_jsonl(documents, output_dir=jurisdiction_output_dir)
        intermediate_paths[jurisdiction] = path

    merged_documents: tuple[NormalizedDocument, ...] = tuple(
        document
        for jurisdiction in cleaned_jurisdictions
        for document in jurisdiction_documents[jurisdiction]
    )
    deduplication_result = deduplicate_documents(
        merged_documents,
        config=NormalizationConfig(
            deduplicate=True,
            deduplicate_fields=MAIN_DEDUPLICATION_FIELDS,
        ),
    )
    final_corpus_path = export_documents_jsonl(
        deduplication_result.documents,
        output_dir=final_root,
    )

    nim_corpus_path: Path | None = None
    nim_document_count = 0
    nim_documents: tuple[NormalizedDocument, ...] = tuple()
    if include_nim and "EU" in cleaned_jurisdictions:
        eu_celex_seeds = _extract_eu_celex_seeds(jurisdiction_documents.get("EU", tuple()))
        if eu_celex_seeds:
            nim_documents = _run_eu_nim(
                eu_celex_seeds,
                output_root=output_root,
                cache_root=cache_root,
            )
            nim_deduplication_result = deduplicate_documents(
                nim_documents,
                config=NormalizationConfig(
                    deduplicate=True,
                    deduplicate_fields=NIM_DEDUPLICATION_FIELDS,
                ),
            )
            nim_documents = nim_deduplication_result.documents
            nim_document_count = len(nim_documents)
            nim_root.mkdir(parents=True, exist_ok=True)
            nim_corpus_path = export_documents_jsonl(nim_documents, output_dir=nim_root)

    manifest_path = export_run_manifest(
        _build_manifest(
            outputs_path=output_root,
            selected_jurisdictions=cleaned_jurisdictions,
            query_terms=cleaned_query_terms,
            include_translations=include_translations,
            translated_terms=cleaned_translated_terms,
            include_nim=include_nim,
            intermediate_paths=intermediate_paths,
            jurisdiction_documents=jurisdiction_documents,
            final_corpus_path=final_corpus_path,
            nim_corpus_path=nim_corpus_path,
            merged_document_count=len(merged_documents),
            final_document_count=len(deduplication_result.documents),
            duplicates_removed=deduplication_result.duplicates_removed,
            nim_document_count=nim_document_count,
        ),
        output_dir=output_root,
    )

    return PolicyCorpusBuildResult(
        outputs_path=output_root,
        selected_jurisdictions=cleaned_jurisdictions,
        intermediate_paths=dict(intermediate_paths),
        final_corpus_path=final_corpus_path,
        nim_corpus_path=nim_corpus_path,
        manifest_path=manifest_path,
        merged_document_count=len(merged_documents),
        final_document_count=len(deduplication_result.documents),
        duplicates_removed=deduplication_result.duplicates_removed,
        nim_document_count=nim_document_count,
    )


def _run_jurisdiction(
    jurisdiction: str,
    *,
    query_terms: tuple[str, ...],
    translated_terms: tuple[str, ...],
    include_translations: bool,
    output_root: Path,
    cache_root: Path,
) -> tuple[NormalizedDocument, ...]:
    if jurisdiction == "EU":
        source = SourceConfig(
            name="eu-eurlex",
            adapter="eurlex",
            settings={
                "cache_dir": str((cache_root / "eu").resolve()),
            },
        )
        queries = list(_build_inline_queries(query_terms, origin="inline"))
        if include_translations:
            queries.extend(_build_inline_queries(translated_terms, origin="translated"))
        return _collect_normalized_documents(source, queries=queries, base_path=output_root)

    source = SourceConfig(
        name=f"{jurisdiction.lower()}-policy-source",
        adapter="non-eu",
        settings={"countries": [jurisdiction]},
    )
    queries = _build_inline_queries(query_terms, origin="inline")
    return _collect_normalized_documents(source, queries=queries, base_path=output_root)


def _run_eu_nim(
    celex_seeds: tuple[str, ...],
    *,
    output_root: Path,
    cache_root: Path,
) -> tuple[NormalizedDocument, ...]:
    source = SourceConfig(
        name="eu-nim",
        adapter="eurlex-nim",
        settings={
            "cache_dir": str((cache_root / "nim").resolve()),
        },
    )
    queries = _build_inline_queries(celex_seeds, origin="eu-celex-seed")
    return _collect_normalized_documents(source, queries=queries, base_path=output_root)


def _collect_normalized_documents(
    source: SourceConfig,
    *,
    queries: tuple[Query, ...],
    base_path: Path,
) -> tuple[NormalizedDocument, ...]:
    adapter = get_adapter(source.adapter)
    adapter.validate_source_config(source, base_path=base_path)

    documents: list[NormalizedDocument] = []
    loaded_source = None
    if getattr(adapter, "execution_mode", "query-aware") == "query-agnostic":
        loaded_source = adapter.load_source(source, base_path=base_path)

    for query in queries:
        raw_results = adapter.collect(
            source,
            query,
            base_path=base_path,
            loaded_source=loaded_source,
        )
        documents.extend(
            normalize_adapter_results(raw_results, source=source, query=query)
        )

    return tuple(documents)


def _build_inline_queries(terms: tuple[str, ...], *, origin: str) -> tuple[Query, ...]:
    return tuple(
        Query(
            text=term,
            query_id=f"{origin}-{index:03d}",
            origin=origin,
        )
        for index, term in enumerate(terms, start=1)
    )


def _extract_eu_celex_seeds(documents: tuple[NormalizedDocument, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for document in documents:
        candidate = (document.source_document_id or "").strip()
        if not candidate:
            raw_record = document.raw_metadata.get("raw_record")
            if isinstance(raw_record, dict):
                candidate = str(
                    raw_record.get("celex_full")
                    or raw_record.get("celex")
                    or ""
                ).strip()
        if candidate and candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return tuple(ordered)


def _build_manifest(
    *,
    outputs_path: Path,
    selected_jurisdictions: tuple[str, ...],
    query_terms: tuple[str, ...],
    include_translations: bool,
    translated_terms: tuple[str, ...],
    include_nim: bool,
    intermediate_paths: dict[str, Path],
    jurisdiction_documents: dict[str, tuple[NormalizedDocument, ...]],
    final_corpus_path: Path,
    nim_corpus_path: Path | None,
    merged_document_count: int,
    final_document_count: int,
    duplicates_removed: int,
    nim_document_count: int,
) -> dict[str, Any]:
    timestamp_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    jurisdiction_summaries = {
        jurisdiction: {
            "label": JURISDICTION_LABELS[jurisdiction],
            "document_count": len(jurisdiction_documents.get(jurisdiction, tuple())),
            "intermediate_corpus_path": str(intermediate_paths[jurisdiction]),
        }
        for jurisdiction in selected_jurisdictions
    }
    return {
        "pipeline": "build_policy_corpus",
        "tool_version": TOOL_VERSION,
        "timestamp_utc": timestamp_utc,
        "outputs_path": str(outputs_path),
        "selected_jurisdictions": list(selected_jurisdictions),
        "query_terms": list(query_terms),
        "include_translations": include_translations,
        "translated_terms": list(translated_terms),
        "include_nim": include_nim,
        "jurisdictions": jurisdiction_summaries,
        "merged_document_count_before_deduplication": merged_document_count,
        "final_document_count": final_document_count,
        "duplicates_removed": duplicates_removed,
        "final_corpus_path": str(final_corpus_path),
        "nim_document_count": nim_document_count,
        "nim_corpus_path": str(nim_corpus_path) if nim_corpus_path else None,
        "output_layout": {
            "cache": str((outputs_path / CACHE_SUBDIR).resolve()),
            "jurisdictions": str((outputs_path / INTERMEDIATE_SUBDIR).resolve()),
            "final": str((outputs_path / FINAL_CORPUS_SUBDIR).resolve()),
            "nim": str((outputs_path / NIM_SUBDIR).resolve()),
        },
    }


def _clean_terms(
    raw_terms: list[str],
    *,
    field_name: str,
    required: bool,
) -> tuple[str, ...]:
    if not isinstance(raw_terms, list):
        raise CorpusBuildValidationError(f"{field_name} must be a list of strings.")
    cleaned = tuple(
        term.strip()
        for term in raw_terms
        if isinstance(term, str) and term.strip()
    )
    if required and not cleaned:
        raise CorpusBuildValidationError(f"{field_name} must contain at least one non-empty string.")
    if len(cleaned) != len(raw_terms):
        raise CorpusBuildValidationError(f"{field_name} must contain only non-empty strings.")
    return cleaned


def _clean_jurisdictions(raw_jurisdictions: list[str]) -> tuple[str, ...]:
    if not isinstance(raw_jurisdictions, list):
        raise CorpusBuildValidationError("jurisdictions must be a list of strings.")
    if not raw_jurisdictions:
        raise CorpusBuildValidationError("jurisdictions must contain at least one value.")

    cleaned: list[str] = []
    seen: set[str] = set()
    for value in raw_jurisdictions:
        if not isinstance(value, str) or not value.strip():
            raise CorpusBuildValidationError(
                "jurisdictions must contain only non-empty strings."
            )
        jurisdiction = value.strip().upper()
        if jurisdiction not in SUPPORTED_JURISDICTIONS:
            allowed = ", ".join(SUPPORTED_JURISDICTIONS)
            raise CorpusBuildValidationError(
                f"Unsupported jurisdiction '{value}'. Allowed values: {allowed}."
            )
        if jurisdiction not in seen:
            seen.add(jurisdiction)
            cleaned.append(jurisdiction)
    return tuple(cleaned)


__all__ = [
    "CorpusBuildValidationError",
    "PolicyCorpusBuildResult",
    "SUPPORTED_JURISDICTIONS",
    "build_policy_corpus",
]
