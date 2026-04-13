"""Post-processing helpers for normalized documents."""

from __future__ import annotations

from dataclasses import dataclass

from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.schemas import NormalizationConfig


@dataclass(frozen=True, slots=True)
class DeduplicationResult:
    """Result of deterministic document deduplication."""

    documents: tuple[NormalizedDocument, ...]
    duplicates_removed: int


def deduplicate_documents(
    documents: tuple[NormalizedDocument, ...],
    *,
    config: NormalizationConfig,
) -> DeduplicationResult:
    """Deduplicate normalized documents using configured normalized fields.

    The deduplication key is a tuple of `(field_name, field_value)` pairs in the
    configured field order. Missing values are represented as `None`. When two
    documents collide on the same key, the first document encountered is retained.
    """

    if not config.deduplicate:
        return DeduplicationResult(documents=documents, duplicates_removed=0)

    seen_keys: set[tuple[tuple[str, object], ...]] = set()
    unique_documents: list[NormalizedDocument] = []

    for document in documents:
        dedup_key = build_deduplication_key(document, config.deduplicate_fields)
        if dedup_key in seen_keys:
            continue

        seen_keys.add(dedup_key)
        unique_documents.append(document)

    return DeduplicationResult(
        documents=tuple(unique_documents),
        duplicates_removed=len(documents) - len(unique_documents),
    )


def build_deduplication_key(
    document: NormalizedDocument,
    field_names: tuple[str, ...],
) -> tuple[tuple[str, object], ...]:
    """Build a deterministic deduplication key from configured fields."""

    return tuple((field_name, getattr(document, field_name, None)) for field_name in field_names)
