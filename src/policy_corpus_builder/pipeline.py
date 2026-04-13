"""In-memory pipeline helpers for non-network adapter execution."""

from __future__ import annotations

from typing import Iterable

from policy_corpus_builder.adapters.base import AdapterResult
from policy_corpus_builder.models import NormalizedDocument, Query
from policy_corpus_builder.schemas import SourceConfig


class NormalizationError(ValueError):
    """Raised when adapter output cannot be converted into normalized documents."""


def normalize_adapter_results(
    results: Iterable[AdapterResult],
    *,
    source: SourceConfig,
    query: Query,
) -> tuple[NormalizedDocument, ...]:
    """Convert raw adapter output into normalized document records."""

    documents: list[NormalizedDocument] = []
    for index, result in enumerate(results):
        payload = result.payload
        document_id = _require_non_empty_string(payload.get("document_id"), "document_id")

        documents.append(
            NormalizedDocument(
                document_id=document_id,
                source_name=source.name,
                source_document_id=_optional_string(payload.get("source_document_id")),
                title=_optional_string(payload.get("title")),
                summary=_optional_string(payload.get("summary")),
                document_type=_optional_string(payload.get("document_type")),
                language=_optional_string(payload.get("language")),
                jurisdiction=_optional_string(payload.get("jurisdiction")),
                publication_date=_optional_string(payload.get("publication_date")),
                effective_date=_optional_string(payload.get("effective_date")),
                url=_optional_string(payload.get("url")),
                download_url=_optional_string(payload.get("download_url")),
                query=query.text,
                retrieved_at=_optional_string(payload.get("retrieved_at")),
                checksum=_optional_string(payload.get("checksum")),
                content_path=_optional_string(payload.get("content_path")),
                raw_metadata=_build_raw_metadata(payload, index=index, query=query),
            )
        )

    return tuple(documents)


def _build_raw_metadata(
    payload: dict[str, object],
    *,
    index: int,
    query: Query,
) -> dict[str, object]:
    raw_metadata = dict(payload)
    raw_metadata.setdefault("_query_id", query.query_id)
    raw_metadata.setdefault("_query_origin", query.origin)
    raw_metadata.setdefault("_adapter_result_index", index)
    return raw_metadata


def _require_non_empty_string(raw_value: object, field_name: str) -> str:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise NormalizationError(
            f"Adapter result field '{field_name}' must be a non-empty string."
        )
    return raw_value.strip()


def _optional_string(raw_value: object) -> str | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise NormalizationError("Optional adapter result fields must be strings when set.")
    cleaned = raw_value.strip()
    return cleaned or None
