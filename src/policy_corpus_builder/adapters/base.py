"""Base source adapter contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig


@dataclass(slots=True)
class AdapterResult:
    """Placeholder for raw adapter output before normalization."""

    payload: dict[str, Any]


class SourceAdapter(Protocol):
    """Minimal protocol for future retrieval adapters."""

    name: str

    def validate_source_config(self, source: SourceConfig) -> None:
        """Validate adapter-specific config."""

    def collect(self, source: SourceConfig, query: Query) -> list[AdapterResult]:
        """Collect raw results for a single source/query pair."""
