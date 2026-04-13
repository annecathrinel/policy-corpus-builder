"""Base source adapter contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig


@dataclass(slots=True)
class AdapterResult:
    """Placeholder for raw adapter output before normalization."""

    payload: dict[str, Any]


class AdapterError(ValueError):
    """Base class for adapter-related errors."""


class AdapterConfigError(AdapterError):
    """Raised when adapter-specific source settings are invalid."""


class AdapterDataError(AdapterError):
    """Raised when adapter input data cannot be parsed."""


class SourceAdapter(Protocol):
    """Minimal protocol for future retrieval adapters."""

    name: str

    def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
        """Validate adapter-specific config."""

    def collect(
        self,
        source: SourceConfig,
        query: Query,
        *,
        base_path: Path,
    ) -> list[AdapterResult]:
        """Collect raw results for a single source/query pair."""
