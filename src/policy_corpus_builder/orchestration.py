"""End-to-end in-memory orchestration for the placeholder pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from policy_corpus_builder.adapters import get_adapter
from policy_corpus_builder.config import load_and_validate_config
from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.pipeline import normalize_adapter_results
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
    document_count: int


@dataclass(frozen=True, slots=True)
class RunResult:
    """Combined in-memory result for a pipeline run."""

    documents: tuple[NormalizedDocument, ...]
    summary: RunSummary


def run_from_config_path(config_path: Path | str) -> RunResult:
    """Load validated config from disk and execute the in-memory pipeline."""

    path = Path(config_path)
    config = load_and_validate_config(path)
    return run_in_memory(config, base_path=path.parent)


def run_in_memory(config: BuilderConfig, *, base_path: Path) -> RunResult:
    """Run the configured placeholder pipeline fully in memory."""

    queries = load_queries(config, base_path=base_path)
    enabled_sources = tuple(source for source in config.sources if source.enabled)

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

    summary = RunSummary(
        project_name=config.project.name,
        query_count=len(queries),
        enabled_source_count=len(enabled_sources),
        source_query_pairs=len(enabled_sources) * len(queries),
        raw_result_count=raw_result_count,
        document_count=len(documents),
    )

    return RunResult(documents=tuple(documents), summary=summary)


def format_run_summary(summary: RunSummary) -> str:
    """Render a concise human-readable run summary."""

    lines = [
        "Run completed successfully.",
        f"Project: {summary.project_name}",
        f"Queries: {summary.query_count}",
        f"Enabled sources: {summary.enabled_source_count}",
        f"Source-query pairs: {summary.source_query_pairs}",
        f"Raw results: {summary.raw_result_count}",
        f"Normalized documents: {summary.document_count}",
    ]
    return "\n".join(lines)
