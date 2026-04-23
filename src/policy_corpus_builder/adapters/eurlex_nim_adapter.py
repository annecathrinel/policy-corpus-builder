"""Adapter wrapper for the first supported EUR-Lex NIM workflow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

from policy_corpus_builder.adapters.base import AdapterConfigError, AdapterResult
from policy_corpus_builder.adapters.eurlex_adapter import (
    _require_bool,
    _require_non_negative_int,
    _require_non_negative_number,
    _require_positive_int,
    _resolve_credentials_env_name,
    _resolve_expert_scope,
    _resolve_search_fields,
    _resolve_search_language,
    _stringify,
)
from policy_corpus_builder.adapters.eurlex_nim_supported import (
    resolve_cache_dir,
    resolve_optional_positive_int,
    resolve_timeout_tuple,
    run_eurlex_nim_query_pipeline,
)
from policy_corpus_builder.adapters.mapping import build_adapter_result
from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig

EURLEX_NIM_FIELD_MAPPING = {
    "document_id": "document_id",
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
    "full_text": "full_text",
    "retrieved_at": "retrieved_at",
    "content_path": "content_path",
}


class EurlexNIMAdapter:
    """Run one supported EUR-Lex NIM workflow for either a CELEX seed or search query."""

    name = "eurlex-nim"
    execution_mode = "query-aware"

    def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
        settings = source.settings
        _resolve_search_language(settings)
        _resolve_search_fields(settings)
        _resolve_expert_scope(settings)
        _require_positive_int(settings, "page_size", default=100)
        _require_positive_int(settings, "max_pages", default=20)
        _require_positive_int(settings, "timeout_s", default=45)
        _require_non_negative_int(settings, "retry_5xx", default=3)
        _require_non_negative_number(settings, "min_interval_s", default=1.6)

        _require_positive_int(settings, "nim_page_size", default=100)
        resolve_optional_positive_int(settings, "nim_max_pages")
        _require_non_negative_number(settings, "nim_sleep_s", default=0.2)

        _require_bool(settings, "fetch_full_text", default=True)
        _require_bool(settings, "use_cache", default=True)
        _require_non_negative_int(settings, "fulltext_retries", default=3)
        _require_non_negative_number(settings, "fulltext_min_interval_s", default=0.5)
        _require_non_negative_int(settings, "progress_every", default=0)
        _require_positive_int(settings, "cache_every", default=50)
        _require_non_negative_int(settings, "success_min_chars", default=500)
        _require_bool(settings, "progress", default=False)
        resolve_optional_positive_int(settings, "nim_max_rows")
        resolve_cache_dir(source, base_path=base_path)
        resolve_timeout_tuple(settings)

        user_env = _resolve_credentials_env_name(
            settings,
            "webservice_user_env",
            default="EURLEX_WS_USER",
        )
        pass_env = _resolve_credentials_env_name(
            settings,
            "webservice_password_env",
            default="EURLEX_WS_PASS",
        )
        user = os.getenv(user_env) or os.getenv("EURLEX_USER")
        password = os.getenv(pass_env) or os.getenv("EURLEX_WEB_PASS")
        if not user or not password:
            raise AdapterConfigError(
                "eurlex-nim adapter requires EUR-Lex WebService credentials. "
                f"Set {user_env}/{pass_env} or EURLEX_USER/EURLEX_WEB_PASS."
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
        self._activate_webservice_credentials(source.settings)
        rows = run_eurlex_nim_query_pipeline(query.text, source=source, base_path=base_path)
        return [self._row_to_result(row) for row in rows]

    def _activate_webservice_credentials(self, settings: dict[str, Any]) -> None:
        user_env = _resolve_credentials_env_name(
            settings,
            "webservice_user_env",
            default="EURLEX_WS_USER",
        )
        pass_env = _resolve_credentials_env_name(
            settings,
            "webservice_password_env",
            default="EURLEX_WS_PASS",
        )
        user = os.getenv(user_env) or os.getenv("EURLEX_USER")
        password = os.getenv(pass_env) or os.getenv("EURLEX_WEB_PASS")
        if user:
            os.environ["EURLEX_USER"] = user
            os.environ.setdefault("EURLEX_WS_USER", user)
        if password:
            os.environ["EURLEX_WEB_PASS"] = password
            os.environ.setdefault("EURLEX_WS_PASS", password)

    def _row_to_result(self, row: dict[str, object]) -> AdapterResult:
        raw_record = _build_raw_record(row)
        result = build_adapter_result(
            row,
            field_mapping=EURLEX_NIM_FIELD_MAPPING,
            defaults={
                "source_document_id": _stringify(row.get("source_document_id")),
                "download_url": _stringify(row.get("download_url")) or _stringify(row.get("url")),
            },
        )
        result.payload["raw_record"] = raw_record
        return result


def _optional_text(value: object) -> str | None:
    text = _stringify(value)
    return text or None


def _sanitize_row(row: dict[str, object]) -> dict[str, object]:
    return {key: _sanitize_value(value) for key, value in row.items()}


def _sanitize_value(value: object) -> object:
    if isinstance(value, dict):
        return {key: _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _build_raw_record(row: dict[str, object]) -> dict[str, object]:
    raw_record = {
        "celex": _optional_text(row.get("celex")),
        "eu_act_title": _optional_text(row.get("eu_act_title")),
        "eu_act_type": _optional_text(row.get("eu_act_type")),
        "year": row.get("year"),
        "nim_celex": _optional_text(row.get("nim_celex")),
        "national_measure_id": _optional_text(row.get("national_measure_id")),
        "nim_title": _optional_text(row.get("nim_title")),
        "nim_title_notice": _optional_text(row.get("nim_title_notice")),
        "nim_title_lang": _optional_text(row.get("nim_title_lang")),
        "nim_date": _optional_text(row.get("nim_date")),
        "member_state_iso3": _optional_text(row.get("member_state_iso3")),
        "member_state_name": _optional_text(row.get("member_state_name")),
        "available_expr_langs3": _optional_text(row.get("available_expr_langs3")),
        "available_langs": _optional_text(row.get("available_langs")),
        "available_languages": _optional_text(row.get("available_languages")),
        "eurlex_url": _optional_text(row.get("eurlex_url")),
        "nim_resource_uri": _optional_text(row.get("nim_resource_uri")),
        "text_source_url": _optional_text(row.get("text_source_url")),
        "text_path": _optional_text(row.get("text_path")),
        "html_path": _optional_text(row.get("html_path")),
        "route_used": _optional_text(row.get("route_used")),
        "text_route_used": _optional_text(row.get("text_route_used")),
        "content_type": _optional_text(row.get("content_type")),
        "source_format": _optional_text(row.get("source_format")),
        "retrieval_status": row.get("retrieval_status"),
        "retrieval_error": _optional_text(row.get("retrieval_error")),
        "fetch_seconds": row.get("fetch_seconds"),
        "fetched_from_cache": row.get("fetched_from_cache"),
        "lang_detected": _optional_text(row.get("lang_detected")),
        "lang_source": _optional_text(row.get("lang_source")),
        "cache_key": _optional_text(row.get("cache_key")),
        "query_text": _optional_text(row.get("query_text")),
    }
    return {
        key: value
        for key, value in _sanitize_row(raw_record).items()
        if value is not None
    }


__all__ = ["EurlexNIMAdapter", "run_eurlex_nim_query_pipeline"]
