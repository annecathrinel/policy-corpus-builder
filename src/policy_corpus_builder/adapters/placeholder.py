"""Non-network placeholder adapter for structural testing."""

from __future__ import annotations

from pathlib import Path

from policy_corpus_builder.adapters.base import AdapterResult
from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig


class PlaceholderAdapter:
    """Simple adapter that emits one predictable in-memory result per query."""

    name = "placeholder"

    def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
        if source.adapter != self.name:
            raise ValueError(
                f"PlaceholderAdapter cannot validate adapter '{source.adapter}'."
            )

    def collect(
        self,
        source: SourceConfig,
        query: Query,
        *,
        base_path: Path,
    ) -> list[AdapterResult]:
        self.validate_source_config(source, base_path=base_path)
        return [
            AdapterResult(
                payload={
                    "document_id": f"{source.name}:{query.query_id}",
                    "source_document_id": query.query_id,
                    "title": f"Placeholder result for {query.text}",
                    "summary": "Synthetic non-network adapter result.",
                    "document_type": "placeholder",
                    "language": "en",
                    "url": f"https://example.invalid/{source.name}/{query.query_id}",
                }
            )
        ]
