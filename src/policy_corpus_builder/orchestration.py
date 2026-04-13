"""End-to-end in-memory orchestration for the placeholder pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from policy_corpus_builder import __version__
from policy_corpus_builder.adapters import get_adapter
from policy_corpus_builder.config import load_and_validate_config
from policy_corpus_builder.exporters import (
    MANIFEST_FILENAME,
    export_documents_jsonl,
    export_run_manifest,
)
from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.pipeline import normalize_adapter_results
from policy_corpus_builder.postprocess import deduplicate_documents
from policy_corpus_builder.queries import load_queries
from policy_corpus_builder.schemas import BuilderConfig


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Concise summary of an in-memory pipeline run."""

    project_name: str
    config_path: str | None
    query_count: int
    query_source_type: str
    source_names_used: tuple[str, ...]
    enabled_export_formats: tuple[str, ...]
    enabled_source_count: int
    source_query_pairs: int
    raw_result_count: int
    raw_normalized_document_count: int
    final_document_count: int
    duplicates_removed: int
    timestamp_utc: str
    tool_version: str
    output_dir: str
    exported_files: tuple[str, ...]

    def to_manifest_dict(self) -> dict[str, object]:
        """Convert the summary into a stable manifest payload."""

        return {
            "project_name": self.project_name,
            "config_path": self.config_path,
            "output_directory": self.output_dir,
            "enabled_export_formats": list(self.enabled_export_formats),
            "exported_files_written": list(self.exported_files),
            "source_names_used": list(self.source_names_used),
            "query_source_type": self.query_source_type,
            "query_count": self.query_count,
            "raw_result_count": self.raw_result_count,
            "raw_normalized_document_count": self.raw_normalized_document_count,
            "final_document_count": self.final_document_count,
            "duplicates_removed": self.duplicates_removed,
            "timestamp_utc": self.timestamp_utc,
            "tool_version": self.tool_version,
        }


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
    return run_in_memory(
        config,
        base_path=path.parent,
        write_exports=write_exports,
        config_path=path.resolve(),
    )


def run_in_memory(
    config: BuilderConfig,
    *,
    base_path: Path,
    write_exports: bool = False,
    config_path: Path | None = None,
) -> RunResult:
    """Run the configured placeholder pipeline fully in memory."""

    queries = load_queries(config, base_path=base_path)
    enabled_sources = tuple(source for source in config.sources if source.enabled)
    output_dir = (base_path / config.project.output_dir).resolve()
    query_source_type = "inventory" if config.queries.inventory else "items"
    timestamp_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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

    corpus_export_paths = _write_corpus_exports(
        list(deduplication_result.documents),
        output_dir=output_dir,
        enabled_formats=config.export.formats,
        write_exports=write_exports,
    )

    exported_files = tuple(path.name for path in corpus_export_paths)
    if write_exports:
        exported_files = exported_files + (MANIFEST_FILENAME,)

    summary = RunSummary(
        project_name=config.project.name,
        config_path=str(config_path) if config_path else None,
        query_count=len(queries),
        query_source_type=query_source_type,
        source_names_used=tuple(source.name for source in enabled_sources),
        enabled_export_formats=config.export.formats,
        enabled_source_count=len(enabled_sources),
        source_query_pairs=len(enabled_sources) * len(queries),
        raw_result_count=raw_result_count,
        raw_normalized_document_count=len(documents),
        final_document_count=len(deduplication_result.documents),
        duplicates_removed=deduplication_result.duplicates_removed,
        timestamp_utc=timestamp_utc,
        tool_version=__version__,
        output_dir=str(output_dir),
        exported_files=exported_files,
    )

    manifest_path = _write_manifest(
        summary,
        output_dir=output_dir,
        write_exports=write_exports,
    )
    exported_paths = corpus_export_paths + ((manifest_path,) if manifest_path else tuple())

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
        f"Config path: {summary.config_path or '<in-memory>'}",
        f"Queries: {summary.query_count}",
        f"Query source type: {summary.query_source_type}",
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


def _write_corpus_exports(
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


def _write_manifest(
    summary: RunSummary,
    *,
    output_dir: Path,
    write_exports: bool,
) -> Path | None:
    if not write_exports:
        return None

    return export_run_manifest(summary.to_manifest_dict(), output_dir=output_dir)
