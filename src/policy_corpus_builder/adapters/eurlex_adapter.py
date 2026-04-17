"""Adapter wrapper for the first supported ordinary EUR-Lex workflow."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from policy_corpus_builder.adapters.base import AdapterConfigError, AdapterResult
from policy_corpus_builder.adapters.mapping import build_adapter_result
from policy_corpus_builder.adapters.eurlex_supported import (
    batch_fetch_eurlex_fulltext,
    build_eu_doc_tables,
    fetch_eurlex_job,
    filter_celex_types_for_fulltext,
)
from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig

EURLEX_FIELD_MAPPING = {
    "document_id": "document_id",
    "source_document_id": "source_document_id",
    "title": "title",
    "document_type": "document_type",
    "language": "language",
    "jurisdiction": "jurisdiction",
    "publication_date": "publication_date",
    "url": "url",
    "download_url": "download_url",
    "full_text": "full_text",
    "retrieved_at": "retrieved_at",
    "content_path": "content_path",
}
ALLOWED_FULLTEXT_MODES = {"all", "sector_0_and_3", "sector_3_only", "supported_only"}
ALLOWED_SEARCH_FIELDS = {"DN", "TI", "TE"}


def run_eurlex_query_pipeline(
    query_text: str,
    *,
    source: SourceConfig,
    base_path: Path,
) -> list[dict[str, object]]:
    """Run the supported ordinary EUR-Lex search and full-text workflow for one query."""

    settings = source.settings
    search_language = _resolve_search_language(settings)
    search_fields = _resolve_search_fields(settings)
    rows = fetch_eurlex_job(
        {
            "scope": "ALL_ALL",
            "expert_scope": _resolve_expert_scope(settings),
            "lang": search_language,
            "terms": [query_text],
        },
        fields=search_fields,
        terms_per_query=1,
        page_size=_require_positive_int(settings, "page_size", default=100),
        max_pages=_require_positive_int(settings, "max_pages", default=20),
        min_interval_s=_require_non_negative_number(settings, "min_interval_s", default=1.6),
        timeout=_require_positive_int(settings, "timeout_s", default=45),
        retry_5xx=_require_non_negative_int(settings, "retry_5xx", default=3),
        debug=False,
    )
    if not rows:
        return []

    _, docs_df = build_eu_doc_tables(pd.DataFrame(rows))
    filtered_docs_df = filter_celex_types_for_fulltext(
        docs_df,
        mode=_resolve_fulltext_mode(settings),
    )
    if filtered_docs_df.empty:
        return []

    fulltext_df = batch_fetch_eurlex_fulltext(
        filtered_docs_df,
        cache_dir=_resolve_cache_dir(source, base_path=base_path),
        use_cache=_require_bool(settings, "use_cache", default=True),
        timeout_s=_require_positive_int(settings, "timeout_s", default=45),
        retries=_require_non_negative_int(settings, "fulltext_retries", default=4),
        min_interval_s=_require_non_negative_number(settings, "fulltext_min_interval_s", default=2.0),
        verbose=False,
        resume=True,
        retry_failures=_require_bool(settings, "retry_failures", default=True),
        progress_every=_require_non_negative_int(settings, "progress_every", default=0),
        cache_every=_require_positive_int(settings, "cache_every", default=50),
        success_min_chars=_require_non_negative_int(settings, "success_min_chars", default=500),
    )

    fulltext_columns = [
        "celex_full",
        "celex",
        "celex_version",
        "text_source_url",
        "full_text_raw",
        "full_text_clean",
        "retrieval_status",
        "retrieval_error",
        "lang",
        "fetch_seconds",
        "fetched_from_cache",
        "text_path",
        "route_used",
        "content_type",
        "celex_variant_used",
        "fulltext_support",
    ]
    available_fulltext_columns = [
        column for column in fulltext_columns if column in fulltext_df.columns
    ]
    merged_df = filtered_docs_df.merge(
        fulltext_df[available_fulltext_columns],
        on=["celex_full", "celex", "celex_version"],
        how="left",
    )

    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    documents: list[dict[str, object]] = []
    for row in merged_df.to_dict(orient="records"):
        row = _coalesce_merge_artifacts(row)
        celex_full = _stringify(row.get("celex_full"))
        title = _stringify(row.get("title")) or celex_full or _stringify(row.get("url_fix"))
        language = _stringify(row.get("lang")) or search_language
        full_text = _resolve_full_text(row)
        download_url = _resolve_download_url(row)
        row["document_id"] = f"{source.name}:EU:{celex_full}"
        row["source_document_id"] = celex_full
        row["title"] = title
        row["document_type"] = _stringify(row.get("celex_descriptor_label")) or "eu_legal_document"
        row["language"] = language
        row["jurisdiction"] = "European Union"
        row["publication_date"] = _optional_text(row.get("date"))
        row["url"] = _stringify(row.get("url_fix")) or _stringify(row.get("url"))
        row["download_url"] = download_url
        row["full_text"] = full_text
        row["retrieved_at"] = timestamp
        row["content_path"] = _optional_text(row.get("text_path"))
        row["query_text"] = query_text
        documents.append(_sanitize_row(row))

    return documents


class EurlexAdapter:
    """Run one supported ordinary EUR-Lex query through search and full-text retrieval."""

    name = "eurlex"
    execution_mode = "query-aware"

    def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
        settings = source.settings
        _resolve_search_language(settings)
        _resolve_search_fields(settings)
        _resolve_expert_scope(settings)
        _resolve_fulltext_mode(settings)
        _require_positive_int(settings, "page_size", default=100)
        _require_positive_int(settings, "max_pages", default=20)
        _require_positive_int(settings, "timeout_s", default=45)
        _require_non_negative_int(settings, "retry_5xx", default=3)
        _require_non_negative_number(settings, "min_interval_s", default=1.6)
        _require_non_negative_int(settings, "fulltext_retries", default=4)
        _require_non_negative_number(settings, "fulltext_min_interval_s", default=2.0)
        _require_bool(settings, "use_cache", default=True)
        _require_bool(settings, "retry_failures", default=True)
        _require_non_negative_int(settings, "progress_every", default=0)
        _require_positive_int(settings, "cache_every", default=50)
        _require_non_negative_int(settings, "success_min_chars", default=500)
        _resolve_cache_dir(source, base_path=base_path)

        user_env = _resolve_credentials_env_name(settings, "webservice_user_env", default="EURLEX_WS_USER")
        pass_env = _resolve_credentials_env_name(settings, "webservice_password_env", default="EURLEX_WS_PASS")
        user = os.getenv(user_env) or os.getenv("EURLEX_USER")
        password = os.getenv(pass_env) or os.getenv("EURLEX_WEB_PASS")
        if not user or not password:
            raise AdapterConfigError(
                "eurlex adapter requires EUR-Lex WebService credentials. "
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
        rows = run_eurlex_query_pipeline(query.text, source=source, base_path=base_path)
        return [self._row_to_result(row) for row in rows]

    def _activate_webservice_credentials(self, settings: dict[str, Any]) -> None:
        user_env = _resolve_credentials_env_name(settings, "webservice_user_env", default="EURLEX_WS_USER")
        pass_env = _resolve_credentials_env_name(settings, "webservice_password_env", default="EURLEX_WS_PASS")
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
            field_mapping=EURLEX_FIELD_MAPPING,
            defaults={
                "source_document_id": _stringify(row.get("source_document_id")),
                "download_url": _stringify(row.get("download_url")) or _stringify(row.get("url")),
            },
        )
        result.payload["raw_record"] = raw_record
        return result


def _resolve_search_language(settings: dict[str, Any]) -> str:
    raw_value = settings.get("search_language", "en")
    if not isinstance(raw_value, str) or len(raw_value.strip()) != 2:
        raise AdapterConfigError(
            "eurlex adapter source.settings.search_language must be a two-letter language code."
        )
    return raw_value.strip().lower()


def _resolve_search_fields(settings: dict[str, Any]) -> tuple[str, ...]:
    raw_value = settings.get("search_fields", ["TI", "TE"])
    if not isinstance(raw_value, list) or not raw_value:
        raise AdapterConfigError(
            "eurlex adapter source.settings.search_fields must be a non-empty list of strings."
        )

    cleaned: list[str] = []
    for item in raw_value:
        if not isinstance(item, str) or not item.strip():
            raise AdapterConfigError(
                "eurlex adapter source.settings.search_fields must contain non-empty strings."
            )
        field_name = item.strip().upper()
        if field_name not in ALLOWED_SEARCH_FIELDS:
            allowed = ", ".join(sorted(ALLOWED_SEARCH_FIELDS))
            raise AdapterConfigError(
                "eurlex adapter source.settings.search_fields contains unsupported values: "
                f"{field_name}. Allowed values: {allowed}."
            )
        cleaned.append(field_name)
    return tuple(cleaned)


def _resolve_expert_scope(settings: dict[str, Any]) -> str:
    raw_value = settings.get("expert_scope", "DTS_SUBDOM = ALL_ALL")
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise AdapterConfigError(
            "eurlex adapter source.settings.expert_scope must be a non-empty string."
        )
    return raw_value.strip()


def _resolve_fulltext_mode(settings: dict[str, Any]) -> str:
    raw_value = settings.get("fulltext_mode", "supported_only")
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise AdapterConfigError(
            "eurlex adapter source.settings.fulltext_mode must be a non-empty string."
        )
    cleaned = raw_value.strip().lower()
    if cleaned not in ALLOWED_FULLTEXT_MODES:
        allowed = ", ".join(sorted(ALLOWED_FULLTEXT_MODES))
        raise AdapterConfigError(
            "eurlex adapter source.settings.fulltext_mode contains an unsupported value. "
            f"Allowed values: {allowed}."
        )
    return cleaned


def _resolve_cache_dir(source: SourceConfig, *, base_path: Path) -> Path:
    raw_value = source.settings.get("cache_dir", f".cache/eurlex/{source.name}")
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise AdapterConfigError(
            "eurlex adapter source.settings.cache_dir must be a non-empty string when set."
        )
    cache_dir = Path(raw_value.strip())
    if not cache_dir.is_absolute():
        cache_dir = (base_path / cache_dir).resolve()
    return cache_dir


def _resolve_credentials_env_name(
    settings: dict[str, Any],
    key: str,
    *,
    default: str,
) -> str:
    raw_value = settings.get(key, default)
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise AdapterConfigError(
            f"eurlex adapter source.settings.{key} must be a non-empty string."
        )
    return raw_value.strip()


def _require_positive_int(settings: dict[str, Any], key: str, *, default: int) -> int:
    raw_value = settings.get(key, default)
    if not isinstance(raw_value, int) or isinstance(raw_value, bool) or raw_value <= 0:
        raise AdapterConfigError(
            f"eurlex adapter source.settings.{key} must be a positive integer."
        )
    return raw_value


def _require_non_negative_int(settings: dict[str, Any], key: str, *, default: int) -> int:
    raw_value = settings.get(key, default)
    if not isinstance(raw_value, int) or isinstance(raw_value, bool) or raw_value < 0:
        raise AdapterConfigError(
            f"eurlex adapter source.settings.{key} must be a non-negative integer."
        )
    return raw_value


def _require_non_negative_number(
    settings: dict[str, Any],
    key: str,
    *,
    default: float,
) -> float:
    raw_value = settings.get(key, default)
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)) or raw_value < 0:
        raise AdapterConfigError(
            f"eurlex adapter source.settings.{key} must be a non-negative number."
        )
    return float(raw_value)


def _require_bool(settings: dict[str, Any], key: str, *, default: bool) -> bool:
    raw_value = settings.get(key, default)
    if not isinstance(raw_value, bool):
        raise AdapterConfigError(
            f"eurlex adapter source.settings.{key} must be a boolean."
        )
    return raw_value


def _stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _optional_text(value: object) -> str | None:
    text = _stringify(value)
    return text or None


def _resolve_full_text(row: dict[str, object]) -> str | None:
    cleaned_text = _optional_text(row.get("full_text_clean"))
    if cleaned_text:
        return cleaned_text
    return _optional_text(row.get("full_text_raw")) or _optional_text(row.get("full_text"))


def _resolve_download_url(row: dict[str, object]) -> str | None:
    text_source_url = _optional_text(row.get("text_source_url"))
    if text_source_url and text_source_url != "CACHE":
        return text_source_url
    return _optional_text(row.get("url_fix")) or _optional_text(row.get("url"))


def _coalesce_merge_artifacts(row: dict[str, object]) -> dict[str, object]:
    merged = dict(row)
    suffix_roots = {
        key[:-2]
        for key in merged
        if key.endswith("_x") or key.endswith("_y")
    }
    for root in suffix_roots:
        left_value = merged.pop(f"{root}_x", None)
        right_value = merged.pop(f"{root}_y", None)
        merged[root] = _first_present_value(right_value, left_value)
    return merged


def _first_present_value(*values: object) -> object | None:
    for value in values:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _sanitize_row(row: dict[str, object]) -> dict[str, object]:
    return {
        key: _sanitize_value(value)
        for key, value in row.items()
    }


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


def _parse_list_field(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [item for item in (_optional_text(v) for v in value) if item]
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if not (text.startswith("[") and text.endswith("]")):
        return [text]
    try:
        import json

        parsed = json.loads(text)
    except Exception:
        return [text]
    if not isinstance(parsed, list):
        return [text]
    return [item for item in (_optional_text(v) for v in parsed) if item]


def _build_raw_record(row: dict[str, object]) -> dict[str, object]:
    raw_record = {
        "source": _optional_text(row.get("source")),
        "celex": _optional_text(row.get("celex")),
        "celex_full": _optional_text(row.get("celex_full")),
        "celex_version": _optional_text(row.get("celex_version")),
        "celex_class": _optional_text(row.get("celex_class")),
        "celex_sector": _optional_text(row.get("celex_sector")),
        "celex_sector_label": _optional_text(row.get("celex_sector_label")),
        "celex_descriptor": _optional_text(row.get("celex_descriptor")),
        "celex_descriptor_label": _optional_text(row.get("celex_descriptor_label")),
        "celex_notes": _optional_text(row.get("celex_notes")),
        "celex_valid": row.get("celex_valid"),
        "celex_is_consolidated": row.get("celex_is_consolidated"),
        "celex_is_corrigendum": row.get("celex_is_corrigendum"),
        "celex_variant_used": _optional_text(row.get("celex_variant_used")),
        "fulltext_support": _optional_text(row.get("fulltext_support")),
        "scopes": _parse_list_field(row.get("scopes")),
        "query_langs": _parse_list_field(row.get("query_langs")),
        "query_term_groups": _parse_list_field(row.get("query_term_groups")),
        "text_source_url": _optional_text(row.get("text_source_url")),
        "text_path": _optional_text(row.get("text_path")),
        "content_type": _optional_text(row.get("content_type")),
        "route_used": _optional_text(row.get("route_used")),
        "retrieval_status": row.get("retrieval_status"),
        "retrieval_error": _optional_text(row.get("retrieval_error")),
        "fetch_seconds": row.get("fetch_seconds"),
        "fetched_from_cache": row.get("fetched_from_cache"),
        "url_fix": _optional_text(row.get("url_fix")),
        "date": _optional_text(row.get("date")),
        "query_text": _optional_text(row.get("query_text")),
    }
    return {
        key: value
        for key, value in _sanitize_row(raw_record).items()
        if value is not None
    }
