"""Adapter wrapper for the supported non-EU retrieval workflow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from policy_corpus_builder.adapters.base import AdapterConfigError, AdapterResult
from policy_corpus_builder.adapters.mapping import build_adapter_result
from policy_corpus_builder.adapters.non_eu import run_non_eu_query_pipeline
from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig

SUPPORTED_NON_EU_COUNTRIES = ("UK", "AUS", "NZ", "CA", "US")
NON_EU_FIELD_MAPPING = {
    "document_id": "doc_id",
    "source_document_id": "doc_uid",
    "title": "title",
    "language": "lang",
    "jurisdiction": "jurisdiction",
    "publication_date": "date",
    "url": "url",
    "download_url": "full_text_url",
    "full_text": "full_text_clean",
}


class NonEUAdapter:
    """Run one non-EU retrieval query through search, full-text, and harmonization."""

    name = "non-eu"
    execution_mode = "query-aware"

    def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
        settings = source.settings
        countries = self._resolve_countries(settings)
        invalid_countries = sorted(set(countries) - set(SUPPORTED_NON_EU_COUNTRIES))
        if invalid_countries:
            allowed = ", ".join(SUPPORTED_NON_EU_COUNTRIES)
            raise AdapterConfigError(
                "non-eu adapter source.settings.countries contains unsupported values: "
                f"{', '.join(invalid_countries)}. Allowed values: {allowed}."
            )

        self._require_positive_int(settings, "max_per_term", default=100)
        self._require_positive_int(settings, "max_workers", default=4)
        self._require_non_negative_int(settings, "progress_every", default=0)
        self._require_bool(settings, "obey_robots", default=True)

        if "US" in countries:
            api_key = self._resolve_us_api_key(settings)
            if not api_key:
                env_name = self._resolve_us_api_key_env(settings)
                raise AdapterConfigError(
                    "non-eu adapter requires a US API key when countries includes 'US'. "
                    f"Set environment variable {env_name} or remove 'US' from countries."
                )

    def collect(
        self,
        source: SourceConfig,
        query: Query,
        *,
        base_path: Path,
        loaded_source: Any | None = None,
    ) -> list[AdapterResult]:
        self.validate_source_config(source, base_path=base_path)

        settings = source.settings
        workflow_result = run_non_eu_query_pipeline(
            query.text,
            countries=self._resolve_countries(settings),
            us_api_key=self._resolve_us_api_key(settings),
            max_per_term=self._require_positive_int(settings, "max_per_term", default=100),
            max_workers=self._require_positive_int(settings, "max_workers", default=4),
            progress_every=self._require_non_negative_int(settings, "progress_every", default=0),
            obey_robots=self._require_bool(settings, "obey_robots", default=True),
        )

        return [
            self._row_to_result(
                row,
                source=source,
                source_log=workflow_result.source_log,
            )
            for row in workflow_result.harmonized_docs_df.to_dict(orient="records")
        ]

    def _row_to_result(
        self,
        row: dict[str, object],
        *,
        source: SourceConfig,
        source_log: list[dict[str, object]],
    ) -> AdapterResult:
        normalized_row = {
            key: self._stringify(value)
            for key, value in row.items()
        }
        normalized_row.setdefault("doc_id", self._stringify(row.get("doc_id")))
        normalized_row["title"] = (
            self._stringify(row.get("title"))
            or normalized_row["doc_id"]
            or self._stringify(row.get("url"))
        )
        normalized_row["doc_uid"] = (
            self._stringify(row.get("doc_uid"))
            or normalized_row["doc_id"]
        )
        normalized_row["full_text_url"] = (
            self._stringify(row.get("full_text_url"))
            or self._stringify(row.get("source_file"))
        )
        normalized_row["full_text_clean"] = self._stringify(row.get("full_text_clean"))

        result = build_adapter_result(
            normalized_row,
            field_mapping=NON_EU_FIELD_MAPPING,
            defaults={"source_document_id": normalized_row["doc_uid"]},
        )
        result.payload["document_id"] = f"{source.name}:{result.payload['document_id']}"
        result.payload["document_type"] = "policy_document"
        result.payload["raw_record"] = {
            "country": normalized_row.get("country", ""),
            "date": normalized_row.get("date", ""),
            "doc_id": normalized_row.get("doc_id", ""),
            "doc_uid": normalized_row.get("doc_uid", ""),
            "full_text_error": normalized_row.get("full_text_error", ""),
            "full_text_format": normalized_row.get("full_text_format", ""),
            "full_text_url": normalized_row.get("full_text_url", ""),
            "has_text": normalized_row.get("has_text", ""),
            "jurisdiction": normalized_row.get("jurisdiction", ""),
            "lang": normalized_row.get("lang", ""),
            "matched_terms": normalized_row.get("matched_terms", ""),
            "retrieval_status": normalized_row.get("retrieval_status", ""),
            "source": normalized_row.get("source", ""),
            "source_log": source_log,
            "source_file": normalized_row.get("source_file", ""),
            "text_len": normalized_row.get("text_len", ""),
            "title": normalized_row.get("title", ""),
            "url": normalized_row.get("url", ""),
            "year": normalized_row.get("year", ""),
        }
        return result

    def _resolve_countries(self, settings: dict[str, Any]) -> tuple[str, ...]:
        raw_countries = settings.get("countries", ["UK"])
        if not isinstance(raw_countries, list) or not raw_countries:
            raise AdapterConfigError(
                "non-eu adapter source.settings.countries must be a non-empty list of strings."
            )

        cleaned: list[str] = []
        for item in raw_countries:
            if not isinstance(item, str) or not item.strip():
                raise AdapterConfigError(
                    "non-eu adapter source.settings.countries must contain non-empty strings."
                )
            cleaned.append(item.strip().upper())

        return tuple(cleaned)

    def _resolve_us_api_key_env(self, settings: dict[str, Any]) -> str:
        raw_value = settings.get("us_api_key_env", "REGULATIONS_GOV_API_KEY")
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise AdapterConfigError(
                "non-eu adapter source.settings.us_api_key_env must be a non-empty string."
            )
        return raw_value.strip()

    def _resolve_us_api_key(self, settings: dict[str, Any]) -> str | None:
        env_name = self._resolve_us_api_key_env(settings)
        return os.getenv(env_name) or None

    def _require_positive_int(
        self,
        settings: dict[str, Any],
        key: str,
        *,
        default: int,
    ) -> int:
        raw_value = settings.get(key, default)
        if not isinstance(raw_value, int) or isinstance(raw_value, bool) or raw_value <= 0:
            raise AdapterConfigError(
                f"non-eu adapter source.settings.{key} must be a positive integer."
            )
        return raw_value

    def _require_non_negative_int(
        self,
        settings: dict[str, Any],
        key: str,
        *,
        default: int,
    ) -> int:
        raw_value = settings.get(key, default)
        if not isinstance(raw_value, int) or isinstance(raw_value, bool) or raw_value < 0:
            raise AdapterConfigError(
                f"non-eu adapter source.settings.{key} must be a non-negative integer."
            )
        return raw_value

    def _require_bool(
        self,
        settings: dict[str, Any],
        key: str,
        *,
        default: bool,
    ) -> bool:
        raw_value = settings.get(key, default)
        if not isinstance(raw_value, bool):
            raise AdapterConfigError(
                f"non-eu adapter source.settings.{key} must be a boolean."
            )
        return raw_value

    def _stringify(self, value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        try:
            import pandas as pd  # local import keeps the registry lightweight
        except Exception:
            pd = None  # type: ignore[assignment]
        if pd is not None and pd.isna(value):
            return ""
        return str(value).strip()
