"""Core in-memory models for queries and normalized documents."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Query:
    """Normalized query item used by the internal pipeline."""

    text: str
    query_id: str
    origin: str
    source_path: str | None = None


@dataclass(slots=True)
class NormalizedDocument:
    """Source-agnostic metadata record for a policy document."""

    document_id: str
    source_name: str
    title: str | None = None
    source_document_id: str | None = None
    summary: str | None = None
    document_type: str | None = None
    language: str | None = None
    jurisdiction: str | None = None
    publication_date: str | None = None
    effective_date: str | None = None
    url: str | None = None
    download_url: str | None = None
    query: str | None = None
    full_text: str | None = None
    retrieved_at: str | None = None
    checksum: str | None = None
    content_path: str | None = None
    raw_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
