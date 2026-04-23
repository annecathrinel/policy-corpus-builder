"""Public top-level orchestration for multi-jurisdiction policy corpus builds."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from policy_corpus_builder.adapters import get_adapter
from policy_corpus_builder.adapters.eurlex_nim_supported.surface import (
    normalize_eligible_legal_act_celex,
)
from policy_corpus_builder.exporters import export_documents_jsonl, export_run_manifest
from policy_corpus_builder.models import NormalizedDocument, Query
from policy_corpus_builder.pipeline import normalize_adapter_results
from policy_corpus_builder.postprocess import (
    clean_documents_for_downstream_analysis,
    deduplicate_documents,
)
from policy_corpus_builder.schemas import NormalizationConfig, SourceConfig

SUPPORTED_JURISDICTIONS = ("EU", "UK", "CA", "AUS", "NZ", "US")
MAIN_DEDUPLICATION_FIELDS = ("document_id",)
NIM_DEDUPLICATION_FIELDS = ("document_id",)
FINAL_CORPUS_SUBDIR = "final"
INTERMEDIATE_SUBDIR = "jurisdictions"
NIM_SUBDIR = "nim"
CACHE_SUBDIR = "cache"
RUN_MANIFEST_FILENAME = "run-manifest.json"
RESULT_SCHEMA_VERSION = "1.0"
MANIFEST_SCHEMA_VERSION = "1.0"

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
class JurisdictionBuildResult:
    """Stable per-jurisdiction output summary."""

    jurisdiction_code: str
    jurisdiction_label: str
    intermediate_corpus_path: Path
    raw_hit_count: int
    document_count: int
    full_text_document_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "jurisdiction_code": self.jurisdiction_code,
            "jurisdiction_label": self.jurisdiction_label,
            "intermediate_corpus_path": str(self.intermediate_corpus_path),
            "raw_hit_count": self.raw_hit_count,
            "document_count": self.document_count,
            "full_text_document_count": self.full_text_document_count,
        }


@dataclass(frozen=True, slots=True)
class _CollectionResult:
    documents: tuple[NormalizedDocument, ...]
    raw_result_count: int

    @property
    def full_text_document_count(self) -> int:
        return sum(1 for document in self.documents if document.full_text)


@dataclass(frozen=True, slots=True)
class PolicyCorpusBuildResult:
    """Stable public result contract for a top-level corpus build."""

    schema_version: str
    outputs_path: Path
    query_terms: tuple[str, ...]
    selected_jurisdictions: tuple[str, ...]
    include_translations: bool
    translated_terms: tuple[str, ...]
    include_nim: bool
    include_nim_fulltext: bool
    nim_max_rows: int | None
    jurisdiction_results: tuple[JurisdictionBuildResult, ...]
    intermediate_paths: dict[str, Path]
    final_corpus_path: Path
    nim_corpus_path: Path | None
    manifest_path: Path
    merged_document_count: int
    final_document_count: int
    duplicates_removed: int
    nim_status: str
    nim_seed_count: int
    nim_eligible_seed_count: int
    nim_document_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "outputs_path": str(self.outputs_path),
            "query_terms": list(self.query_terms),
            "selected_jurisdictions": list(self.selected_jurisdictions),
            "include_translations": self.include_translations,
            "translated_terms": list(self.translated_terms),
            "include_nim": self.include_nim,
            "include_nim_fulltext": self.include_nim_fulltext,
            "nim_max_rows": self.nim_max_rows,
            "jurisdictions": [item.to_dict() for item in self.jurisdiction_results],
            "per_jurisdiction_output_paths": {
                jurisdiction: str(path)
                for jurisdiction, path in self.intermediate_paths.items()
            },
            "final_corpus_path": str(self.final_corpus_path),
            "nim_corpus_path": str(self.nim_corpus_path) if self.nim_corpus_path else None,
            "manifest_path": str(self.manifest_path),
            "merged_document_count": self.merged_document_count,
            "final_document_count": self.final_document_count,
            "duplicates_removed": self.duplicates_removed,
            "nim_status": self.nim_status,
            "nim_seed_count": self.nim_seed_count,
            "nim_eligible_seed_count": self.nim_eligible_seed_count,
            "nim_document_count": self.nim_document_count,
        }

    def to_manifest_dict(self) -> dict[str, object]:
        payload = self.to_dict()
        payload["manifest_schema_version"] = MANIFEST_SCHEMA_VERSION
        payload["timestamp_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        payload["output_layout"] = {
            "cache": str((self.outputs_path / CACHE_SUBDIR).resolve()),
            "jurisdictions": str((self.outputs_path / INTERMEDIATE_SUBDIR).resolve()),
            "final": str((self.outputs_path / FINAL_CORPUS_SUBDIR).resolve()),
            "nim": str((self.outputs_path / NIM_SUBDIR).resolve()),
        }
        payload["tool_version"] = TOOL_VERSION
        payload["pipeline"] = "build_policy_corpus"
        return payload


def build_policy_corpus(
    query_terms: list[str],
    jurisdictions: list[str],
    outputs_path: str | Path,
    include_translations: bool = False,
    translated_terms: list[str] | None = None,
    include_nim: bool = False,
    include_nim_fulltext: bool = True,
    nim_max_rows: int | None = None,
) -> PolicyCorpusBuildResult:
    """Build one normalized policy corpus across supported jurisdictions."""

    _emit_progress("Starting build_policy_corpus: validating inputs.")
    if not isinstance(include_nim_fulltext, bool):
        raise CorpusBuildValidationError("include_nim_fulltext must be a boolean.")
    cleaned_query_terms = _clean_terms(query_terms, field_name="query_terms", required=True)
    cleaned_jurisdictions = _clean_jurisdictions(jurisdictions)
    cleaned_translated_terms = _clean_terms(
        translated_terms or [],
        field_name="translated_terms",
        required=False,
    )
    cleaned_nim_max_rows = _clean_optional_positive_int(nim_max_rows, field_name="nim_max_rows")

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
    jurisdiction_results: list[JurisdictionBuildResult] = []

    _emit_progress(
        "Running jurisdictions: " + ", ".join(cleaned_jurisdictions) + "."
    )

    for jurisdiction in cleaned_jurisdictions:
        _emit_progress(f"Starting jurisdiction {jurisdiction}.")
        jurisdiction_result = _run_jurisdiction(
            jurisdiction,
            query_terms=cleaned_query_terms,
            translated_terms=cleaned_translated_terms,
            include_translations=include_translations,
            output_root=output_root,
            cache_root=cache_root,
        )
        documents = clean_documents_for_downstream_analysis(
            jurisdiction_result.documents,
            expected_jurisdiction_code=jurisdiction,
        )
        jurisdiction_documents[jurisdiction] = documents
        _emit_progress(
            f"Running jurisdiction {jurisdiction}. Total hits: {jurisdiction_result.raw_result_count}."
        )

        jurisdiction_output_dir = intermediate_root / jurisdiction.lower()
        path = export_documents_jsonl(documents, output_dir=jurisdiction_output_dir)
        intermediate_paths[jurisdiction] = path
        jurisdiction_results.append(
            JurisdictionBuildResult(
                jurisdiction_code=jurisdiction,
                jurisdiction_label=JURISDICTION_LABELS[jurisdiction],
                intermediate_corpus_path=path,
                raw_hit_count=jurisdiction_result.raw_result_count,
                document_count=len(documents),
                full_text_document_count=sum(1 for document in documents if document.full_text),
            )
        )
        _emit_progress(
            "Finished jurisdiction "
            f"{jurisdiction}. Unique full-text documents retrieved: "
            f"{sum(1 for document in documents if document.full_text)}. "
            f"Normalized documents: {len(documents)}."
        )

    merged_documents: tuple[NormalizedDocument, ...] = tuple(
        document
        for jurisdiction in cleaned_jurisdictions
        for document in jurisdiction_documents[jurisdiction]
    )
    _emit_progress("Merging jurisdiction corpora and deduplicating final corpus.")
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
    _emit_progress(
        f"Final corpus: {len(deduplication_result.documents)} unique documents "
        f"({deduplication_result.duplicates_removed} duplicates removed)."
    )

    nim_corpus_path: Path | None = None
    nim_status = "not_requested"
    nim_seed_count = 0
    nim_eligible_seed_count = 0
    nim_document_count = 0
    nim_documents: tuple[NormalizedDocument, ...] = tuple()
    if include_nim and "EU" in cleaned_jurisdictions:
        _emit_progress("Running NIM from EU CELEX results.")
        nim_status = "requested"
        nim_seed_candidates = _extract_eu_celex_seed_candidates(
            jurisdiction_documents.get("EU", tuple())
        )
        nim_seed_count = len(nim_seed_candidates)
        eu_celex_seeds = _filter_eligible_nim_celex_seeds(nim_seed_candidates)
        eu_celex_seeds = _defensively_filter_nim_runtime_celex_seeds(eu_celex_seeds)
        nim_eligible_seed_count = len(eu_celex_seeds)
        _emit_progress(f"NIM seed candidates from EU results: {nim_seed_count}.")
        _emit_progress(f"Number of NIM eligible EU acts: {nim_eligible_seed_count}.")
        if eu_celex_seeds:
            nim_status = "ran"
            nim_documents = _run_eu_nim(
                eu_celex_seeds,
                output_root=output_root,
                cache_root=cache_root,
                include_nim_fulltext=include_nim_fulltext,
                nim_max_rows=cleaned_nim_max_rows,
            )
            nim_documents = clean_documents_for_downstream_analysis(nim_documents)
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
            _emit_progress(f"Finished NIM: {nim_document_count} documents.")
        else:
            nim_status = "skipped_no_eligible_eu_legal_acts"
            _emit_progress(
                "Skipping NIM: EU results contained no eligible legal-act CELEX seeds."
            )
    elif include_nim:
        nim_status = "skipped_eu_not_selected"
        _emit_progress("Skipping NIM: EU was not selected.")

    _emit_progress("Writing final outputs and manifest.")
    result = PolicyCorpusBuildResult(
        schema_version=RESULT_SCHEMA_VERSION,
        outputs_path=output_root,
        query_terms=cleaned_query_terms,
        selected_jurisdictions=cleaned_jurisdictions,
        include_translations=include_translations,
        translated_terms=cleaned_translated_terms,
        include_nim=include_nim,
        include_nim_fulltext=include_nim_fulltext,
        nim_max_rows=cleaned_nim_max_rows,
        jurisdiction_results=tuple(jurisdiction_results),
        intermediate_paths=dict(intermediate_paths),
        final_corpus_path=final_corpus_path,
        nim_corpus_path=nim_corpus_path,
        manifest_path=output_root / RUN_MANIFEST_FILENAME,
        merged_document_count=len(merged_documents),
        final_document_count=len(deduplication_result.documents),
        duplicates_removed=deduplication_result.duplicates_removed,
        nim_status=nim_status,
        nim_seed_count=nim_seed_count,
        nim_eligible_seed_count=nim_eligible_seed_count,
        nim_document_count=nim_document_count,
    )
    manifest_path = export_run_manifest(
        result.to_manifest_dict(),
        output_dir=output_root,
    )
    result = PolicyCorpusBuildResult(
        schema_version=result.schema_version,
        outputs_path=result.outputs_path,
        query_terms=result.query_terms,
        selected_jurisdictions=result.selected_jurisdictions,
        include_translations=result.include_translations,
        translated_terms=result.translated_terms,
        include_nim=result.include_nim,
        include_nim_fulltext=result.include_nim_fulltext,
        nim_max_rows=result.nim_max_rows,
        jurisdiction_results=result.jurisdiction_results,
        intermediate_paths=result.intermediate_paths,
        final_corpus_path=result.final_corpus_path,
        nim_corpus_path=result.nim_corpus_path,
        manifest_path=manifest_path,
        merged_document_count=result.merged_document_count,
        final_document_count=result.final_document_count,
        duplicates_removed=result.duplicates_removed,
        nim_status=result.nim_status,
        nim_seed_count=result.nim_seed_count,
        nim_eligible_seed_count=result.nim_eligible_seed_count,
        nim_document_count=result.nim_document_count,
    )
    _emit_progress(
        f"Completed build_policy_corpus: {result.final_document_count} final documents written."
    )
    return result


def _run_jurisdiction(
    jurisdiction: str,
    *,
    query_terms: tuple[str, ...],
    translated_terms: tuple[str, ...],
    include_translations: bool,
    output_root: Path,
    cache_root: Path,
) -> _CollectionResult:
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
    include_nim_fulltext: bool,
    nim_max_rows: int | None,
) -> tuple[NormalizedDocument, ...]:
    runtime_safe_celex_seeds = _defensively_filter_nim_runtime_celex_seeds(celex_seeds)
    if not runtime_safe_celex_seeds:
        return tuple()

    source = SourceConfig(
        name="eu-nim",
        adapter="eurlex-nim",
        settings={
            "cache_dir": str((cache_root / "nim").resolve()),
            "fetch_full_text": include_nim_fulltext,
            "nim_max_rows": nim_max_rows,
            "progress": True,
            "progress_every": 10,
        },
    )
    queries = _build_inline_queries(runtime_safe_celex_seeds, origin="eu-celex-seed")
    return _collect_normalized_documents(source, queries=queries, base_path=output_root).documents


def _collect_normalized_documents(
    source: SourceConfig,
    *,
    queries: tuple[Query, ...],
    base_path: Path,
) -> _CollectionResult:
    adapter = get_adapter(source.adapter)
    adapter.validate_source_config(source, base_path=base_path)

    documents: list[NormalizedDocument] = []
    raw_result_count = 0
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
        raw_result_count += len(raw_results)
        documents.extend(
            normalize_adapter_results(raw_results, source=source, query=query)
        )

    return _CollectionResult(
        documents=tuple(documents),
        raw_result_count=raw_result_count,
    )


def _build_inline_queries(terms: tuple[str, ...], *, origin: str) -> tuple[Query, ...]:
    return tuple(
        Query(
            text=term,
            query_id=f"{origin}-{index:03d}",
            origin=origin,
        )
        for index, term in enumerate(terms, start=1)
    )


def _extract_eu_celex_seed_candidates(documents: tuple[NormalizedDocument, ...]) -> tuple[str, ...]:
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


def _filter_eligible_nim_celex_seeds(candidates: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    eligible: list[str] = []
    for candidate in candidates:
        normalized = normalize_eligible_legal_act_celex(candidate)
        if normalized and normalized not in seen:
            seen.add(normalized)
            eligible.append(normalized)
    return tuple(eligible)


def _defensively_filter_nim_runtime_celex_seeds(candidates: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    runtime_safe: list[str] = []
    for candidate in candidates:
        normalized = normalize_eligible_legal_act_celex(candidate)
        if normalized and normalized not in seen:
            seen.add(normalized)
            runtime_safe.append(normalized)
    return tuple(runtime_safe)


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


def _clean_optional_positive_int(value: int | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise CorpusBuildValidationError(f"{field_name} must be a positive integer when set.")
    return value


def _emit_progress(message: str) -> None:
    print(f"[policy-corpus-builder] {message}", flush=True)


__all__ = [
    "CorpusBuildValidationError",
    "JurisdictionBuildResult",
    "PolicyCorpusBuildResult",
    "SUPPORTED_JURISDICTIONS",
    "build_policy_corpus",
]
