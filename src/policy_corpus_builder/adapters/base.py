"""Base source adapter contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class AdapterResult:
    """Placeholder for raw adapter output before normalization."""

    payload: dict[str, Any]


class SourceAdapter(Protocol):
    """Minimal protocol for future retrieval adapters."""

    name: str

    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate adapter-specific config."""

    def run(self, config: dict[str, Any]) -> list[AdapterResult]:
        """Execute retrieval and return raw results."""
