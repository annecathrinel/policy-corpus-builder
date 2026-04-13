"""Local fixture-backed adapter for JSON and JSONL source files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from policy_corpus_builder.adapters.base import (
    AdapterConfigError,
    AdapterDataError,
    AdapterResult,
)
from policy_corpus_builder.adapters.mapping import build_adapter_result
from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig

ALLOWED_FILE_FORMATS = {"json", "jsonl"}
OPTIONAL_RECORD_FIELDS = {
    "summary",
    "document_type",
    "language",
    "jurisdiction",
    "publication_date",
    "effective_date",
    "url",
    "download_url",
    "retrieved_at",
    "checksum",
    "content_path",
}
LOCAL_FILE_FIELD_MAPPING = {
    "document_id": "id",
    "source_document_id": "source_document_id",
    "title": "title",
    "summary": "summary",
    "document_type": "document_type",
    "language": "language",
    "jurisdiction": "jurisdiction",
    "publication_date": "publication_date",
    "effective_date": "effective_date",
    "url": "url",
    "download_url": "download_url",
    "retrieved_at": "retrieved_at",
    "checksum": "checksum",
    "content_path": "content_path",
}


class LocalFileAdapter:
    """Read structured policy records from a local JSON or JSONL file."""

    name = "local-file"

    def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
        settings = source.settings
        fixture_path = settings.get("path")
        if not isinstance(fixture_path, str) or not fixture_path.strip():
            raise AdapterConfigError(
                "local-file adapter requires source.settings.path as a non-empty string."
            )

        file_format = settings.get("format")
        if file_format is not None:
            if not isinstance(file_format, str) or file_format.lower() not in ALLOWED_FILE_FORMATS:
                raise AdapterConfigError(
                    "local-file adapter source.settings.format must be 'json' or 'jsonl'."
                )

        query_field = settings.get("query_field", "queries")
        if not isinstance(query_field, str) or not query_field.strip():
            raise AdapterConfigError(
                "local-file adapter source.settings.query_field must be a non-empty string."
            )

        resolved_path = self._resolve_fixture_path(source, base_path=base_path)
        if not resolved_path.exists():
            raise AdapterConfigError(
                f"local-file adapter fixture path does not exist: {fixture_path}"
            )

        if resolved_path.suffix.lower() not in {".json", ".jsonl"} and file_format is None:
            raise AdapterConfigError(
                "local-file adapter could not infer file format; set source.settings.format."
            )

    def collect(
        self,
        source: SourceConfig,
        query: Query,
        *,
        base_path: Path,
    ) -> list[AdapterResult]:
        self.validate_source_config(source, base_path=base_path)
        records = self._load_records(source, base_path=base_path)
        matching_records = [
            record
            for record in records
            if self._record_matches_query(record, source=source, query=query)
        ]
        return [self._record_to_result(record, source=source) for record in matching_records]

    def _load_records(self, source: SourceConfig, *, base_path: Path) -> list[dict[str, Any]]:
        fixture_path = self._resolve_fixture_path(source, base_path=base_path)
        file_format = self._resolve_format(source, fixture_path=fixture_path)

        if file_format == "json":
            raw_data = json.loads(fixture_path.read_text(encoding="utf-8"))
            if isinstance(raw_data, list):
                records = raw_data
            elif isinstance(raw_data, dict) and isinstance(raw_data.get("records"), list):
                records = raw_data["records"]
            else:
                raise AdapterDataError(
                    "local-file JSON fixtures must contain a list or an object with a 'records' list."
                )
        else:
            records = []
            for line_number, line in enumerate(
                fixture_path.read_text(encoding="utf-8").splitlines(),
                start=1,
            ):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    records.append(json.loads(stripped))
                except json.JSONDecodeError as exc:
                    raise AdapterDataError(
                        f"local-file JSONL fixture contains invalid JSON on line {line_number}."
                    ) from exc

        if not all(isinstance(record, dict) for record in records):
            raise AdapterDataError("local-file fixtures must contain only object records.")

        return records

    def _record_matches_query(
        self,
        record: dict[str, Any],
        *,
        source: SourceConfig,
        query: Query,
    ) -> bool:
        query_field = source.settings.get("query_field", "queries")
        raw_terms = record.get(query_field)
        if raw_terms is None:
            return True

        if isinstance(raw_terms, str):
            terms = [raw_terms]
        elif isinstance(raw_terms, list) and all(isinstance(item, str) for item in raw_terms):
            terms = raw_terms
        else:
            raise AdapterDataError(
                "local-file adapter query field must be a string or list of strings when present."
            )

        normalized_query = query.text.strip().casefold()
        return any(term.strip().casefold() == normalized_query for term in terms)

    def _record_to_result(self, record: dict[str, Any], *, source: SourceConfig) -> AdapterResult:
        self._validate_record_fields(record)

        result = build_adapter_result(
            record,
            field_mapping=LOCAL_FILE_FIELD_MAPPING,
            defaults={"source_document_id": str(record["id"])},
        )
        result.payload["document_id"] = f"{source.name}:{result.payload['document_id']}"
        return result

    def _validate_record_fields(self, record: dict[str, Any]) -> None:
        record_id = record.get("id")
        title = record.get("title")
        if not isinstance(record_id, str) or not record_id.strip():
            raise AdapterDataError("local-file records require a non-empty string 'id'.")
        if not isinstance(title, str) or not title.strip():
            raise AdapterDataError("local-file records require a non-empty string 'title'.")

        raw_source_document_id = record.get("source_document_id")
        if raw_source_document_id is not None:
            if not isinstance(raw_source_document_id, str) or not raw_source_document_id.strip():
                raise AdapterDataError(
                    "local-file record field 'source_document_id' must be a non-empty string when present."
                )

        for field_name in OPTIONAL_RECORD_FIELDS:
            raw_value = record.get(field_name)
            if raw_value is not None and not isinstance(raw_value, str):
                raise AdapterDataError(
                    f"local-file record field '{field_name}' must be a string when present."
                )

    def _resolve_fixture_path(self, source: SourceConfig, *, base_path: Path) -> Path:
        fixture_path = Path(str(source.settings["path"]))
        if fixture_path.is_absolute():
            return fixture_path
        return (base_path / fixture_path).resolve()

    def _resolve_format(self, source: SourceConfig, *, fixture_path: Path) -> str:
        file_format = source.settings.get("format")
        if isinstance(file_format, str):
            return file_format.lower()
        return fixture_path.suffix.lower().lstrip(".")
