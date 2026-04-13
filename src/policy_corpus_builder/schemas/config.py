"""Validated internal schema for tool configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    name: str
    output_dir: str
    description: str | None = None


@dataclass(frozen=True, slots=True)
class QueriesConfig:
    inventory: str | None = None
    items: tuple[str, ...] = field(default_factory=tuple)

    @property
    def query_count(self) -> int:
        if self.inventory:
            return len(self.items) if self.items else 1
        return len(self.items)


@dataclass(frozen=True, slots=True)
class SourceConfig:
    name: str
    adapter: str
    enabled: bool = True
    settings: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NormalizationConfig:
    deduplicate: bool
    deduplicate_fields: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ExportConfig:
    formats: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BuilderConfig:
    project: ProjectConfig
    queries: QueriesConfig
    sources: tuple[SourceConfig, ...]
    normalization: NormalizationConfig
    export: ExportConfig
