"""Reusable helpers for adapter raw-record field mapping."""

from __future__ import annotations

from typing import Any

from policy_corpus_builder.adapters.base import AdapterDataError, AdapterResult

REQUIRED_NORMALIZED_FIELDS = ("document_id", "title")
OPTIONAL_NORMALIZED_FIELDS = (
    "source_document_id",
    "summary",
    "document_type",
    "language",
    "jurisdiction",
    "publication_date",
    "effective_date",
    "url",
    "download_url",
    "full_text",
    "retrieved_at",
    "checksum",
    "content_path",
)


def build_adapter_result(
    raw_record: dict[str, Any],
    *,
    field_mapping: dict[str, str],
    defaults: dict[str, str] | None = None,
) -> AdapterResult:
    """Build an AdapterResult from an adapter-defined raw-record mapping."""

    defaults = defaults or {}
    payload: dict[str, Any] = {}

    for normalized_field in REQUIRED_NORMALIZED_FIELDS:
        value = _resolve_mapped_value(
            raw_record,
            normalized_field=normalized_field,
            field_mapping=field_mapping,
            defaults=defaults,
            required=True,
        )
        payload[normalized_field] = value

    for normalized_field in OPTIONAL_NORMALIZED_FIELDS:
        value = _resolve_mapped_value(
            raw_record,
            normalized_field=normalized_field,
            field_mapping=field_mapping,
            defaults=defaults,
            required=False,
        )
        if value is not None:
            payload[normalized_field] = value

    payload["raw_record"] = dict(raw_record)
    return AdapterResult(payload=payload)


def _resolve_mapped_value(
    raw_record: dict[str, Any],
    *,
    normalized_field: str,
    field_mapping: dict[str, str],
    defaults: dict[str, str],
    required: bool,
) -> str | None:
    raw_field_name = field_mapping.get(normalized_field)
    raw_value = raw_record.get(raw_field_name) if raw_field_name else None

    if raw_value is None and normalized_field in defaults:
        raw_value = defaults[normalized_field]

    if raw_value is None:
        if required:
            raise AdapterDataError(
                f"Mapped normalized field '{normalized_field}' is required."
            )
        return None

    if not isinstance(raw_value, str):
        raise AdapterDataError(
            f"Mapped normalized field '{normalized_field}' must resolve to a string."
        )

    cleaned = raw_value.strip()
    if not cleaned:
        if required:
            raise AdapterDataError(
                f"Mapped normalized field '{normalized_field}' must be a non-empty string."
            )
        return None

    return cleaned
