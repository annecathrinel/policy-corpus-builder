"""End-to-end in-memory orchestration for the placeholder pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from policy_corpus_builder.adapters import get_adapter
from policy_corpus_builder.config import load_and_validate_config
from policy_corpus_builder.exporters import export_documents_jsonl
from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.pipeline import normalize_adapter_results
from policy_corpus_builder.postprocess import deduplicate_documents
from policy_corpus_builder.queries import load_queries
from policy_corpus_builder.schemas import BuilderConfig


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Concise summary of an in-memory pipeline run."""

    project_name: str
    query_count: int
    enabled_source_count: int
    source_query_pairs: int
    raw_result_count: int
    raw_normalized_document_count: int
    final_document_count: int
    duplicates_removed: int
    output_dir: str
    exported_files: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RunResult:
    """Combined in-memory result for a pipeline run."""

    documents: tuple[NormalizedDocument, ...]
    summary: RunSummary
    exported_paths: tuple[Path, ...]


def run_from_config_path(config_path: Path | str, *, write_exports: bool = True) -> RunResult:
    """Load validated config from disk and execute the in-memory pipeline."""

    path = Path(config_path)
    config = load_and_validate_config(path)
    return run_in_memory(config, base_path=path.parent, write_exports=write_exports)


def run_in_memory(
    config: BuilderConfig,
    *,
    base_path: Path,
    write_exports: bool = False,
) -> RunResult:
    """Run the configured placeholder pipeline fully in memory."""

    queries = load_queries(config, base_path=base_path)
    enabled_sources = tuple(source for source in config.sources if source.enabled)
    output_dir = (base_path / config.project.output_dir).resolve()

    documents: list[NormalizedDocument] = []
    raw_result_count = 0

    for source in enabled_sources:
        adapter = get_adapter(source.adapter)
        adapter.validate_source_config(source)

        for query in queries:
            raw_results = tuple(adapter.collect(source, query))
            raw_result_count += len(raw_results)
            documents.extend(
                normalize_adapter_results(raw_results, source=source, query=query)
            )

    deduplication_result = deduplicate_documents(
        tuple(documents),
        config=config.normalization,
    )

    exported_paths = _write_exports(
        list(deduplication_result.documents),
        output_dir=output_dir,
        enabled_formats=config.export.formats,
        write_exports=write_exports,
    )

    summary = RunSummary(
        project_name=config.project.name,
        query_count=len(queries),
        enabled_source_count=len(enabled_sources),
        source_query_pairs=len(enabled_sources) * len(queries),
        raw_result_count=raw_result_count,
        raw_normalized_document_count=len(documents),
        final_document_count=len(deduplication_result.documents),
        duplicates_removed=deduplication_result.duplicates_removed,
        output_dir=str(output_dir),
        exported_files=tuple(path.name for path in exported_paths),
    )

    return RunResult(
        documents=deduplication_result.documents,
        summary=summary,
        exported_paths=exported_paths,
    )


def format_run_summary(summary: RunSummary) -> str:
    """Render a concise human-readable run summary."""

    lines = [
        "Run completed successfully.",
        f"Project: {summary.project_name}",
        f"Queries: {summary.query_count}",
        f"Enabled sources: {summary.enabled_source_count}",
        f"Source-query pairs: {summary.source_query_pairs}",
        f"Raw results: {summary.raw_result_count}",
        f"Raw normalized documents: {summary.raw_normalized_document_count}",
        f"Documents after deduplication: {summary.final_document_count}",
        f"Duplicates removed: {summary.duplicates_removed}",
        f"Output directory: {summary.output_dir}",
        (
            f"Exported files: {', '.join(summary.exported_files)}"
            if summary.exported_files
            else "Exported files: none"
        ),
    ]
    return "\n".join(lines)


def _write_exports(
    documents: list[NormalizedDocument],
    *,
    output_dir: Path,
    enabled_formats: tuple[str, ...],
    write_exports: bool,
) -> tuple[Path, ...]:
    if not write_exports:
        return tuple()

    exported_paths: list[Path] = []
    if "jsonl" in enabled_formats:
        exported_paths.append(export_documents_jsonl(documents, output_dir=output_dir))

    return tuple(exported_paths)
