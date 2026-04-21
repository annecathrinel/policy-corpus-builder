"""Supported EUR-Lex NIM workflow helpers used by the public adapter path."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from policy_corpus_builder.adapters.base import AdapterConfigError
from policy_corpus_builder.adapters.eurlex_adapter import (
    _require_bool,
    _require_non_negative_int,
    _require_non_negative_number,
    _require_positive_int,
    _resolve_expert_scope,
    _resolve_search_fields,
    _resolve_search_language,
    _stringify,
)
from policy_corpus_builder.adapters.eurlex_nim_supported.surface import (
    batch_fetch_nim_fulltext,
    enrich_nim_metadata,
    get_national_transpositions_by_celex_ws,
    normalize_legal_act_celex,
    select_eligible_celex_acts,
)
from policy_corpus_builder.adapters.eurlex_supported import build_eu_doc_tables, fetch_eurlex_job
from policy_corpus_builder.schemas import SourceConfig
from policy_corpus_builder.utils.celex import parse_celex

ALLOWED_LEGAL_ACT_DESCRIPTORS = {"L", "R", "D"}


def run_eurlex_nim_query_pipeline(
    query_text: str,
    *,
    source: SourceConfig,
    base_path: Path,
) -> list[dict[str, object]]:
    settings = source.settings
    acts_df = _resolve_seed_acts(query_text, settings)
    if acts_df.empty:
        return []

    nim_df = _retrieve_nim_rows(acts_df, settings)
    if nim_df.empty:
        return []

    merged_df = nim_df.copy()
    if _require_bool(settings, "fetch_full_text", default=True):
        fulltext_df = batch_fetch_nim_fulltext(
            nim_df,
            cache_dir=resolve_cache_dir(source, base_path=base_path),
            use_cache=_require_bool(settings, "use_cache", default=True),
            timeout=resolve_timeout_tuple(settings),
            retries=_require_non_negative_int(settings, "fulltext_retries", default=3),
            min_interval_s=_require_non_negative_number(settings, "fulltext_min_interval_s", default=0.5),
            verbose=False,
            resume=False,
            retry_failures=True,
            progress_every=_require_non_negative_int(settings, "progress_every", default=0),
            cache_every=_require_positive_int(settings, "cache_every", default=50),
            success_min_chars=_require_non_negative_int(settings, "success_min_chars", default=500),
        )
        normalized_nim_df = _normalize_nim_merge_frame(nim_df)
        normalized_fulltext_df = _normalize_nim_merge_frame(fulltext_df)
        merge_keys = [
            key
            for key in ("celex", "nim_celex", "national_measure_id")
            if key in normalized_nim_df.columns and key in normalized_fulltext_df.columns
        ]
        if merge_keys:
            fulltext_columns = [
                column
                for column in (
                    "celex",
                    "nim_celex",
                    "national_measure_id",
                    "text_source_url",
                    "source_url",
                    "full_text_raw",
                    "full_text_clean",
                    "text_len",
                    "retrieval_status",
                    "retrieval_error",
                    "fetch_seconds",
                    "fetched_from_cache",
                    "lang",
                    "lang_detected",
                    "lang_source",
                    "text_path",
                    "html_path",
                    "route_used",
                    "text_route_used",
                    "content_type",
                    "source_format",
                    "available_languages",
                    "page_title",
                    "page_title_lang",
                    "cache_key",
                )
                if column in normalized_fulltext_df.columns
            ]
            merged_df = normalized_nim_df.merge(
                normalized_fulltext_df[fulltext_columns],
                on=merge_keys,
                how="left",
            )
        else:
            merged_df = normalized_nim_df

    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    documents: list[dict[str, object]] = []
    for row in merged_df.to_dict(orient="records"):
        row = _coalesce_merge_artifacts(row)
        source_document_id = _stringify(row.get("national_measure_id")) or _stringify(row.get("nim_celex"))
        title = (
            _stringify(row.get("nim_title"))
            or _stringify(row.get("nim_title_notice"))
            or source_document_id
            or _stringify(row.get("eurlex_url"))
        )
        jurisdiction = _stringify(row.get("member_state_name")) or _stringify(row.get("member_state_iso3"))
        language = _stringify(row.get("lang")) or _stringify(row.get("nim_title_lang")) or None
        row["document_id"] = _build_document_id(source.name, row)
        row["source_document_id"] = source_document_id
        row["title"] = title
        row["summary"] = _build_summary(row)
        row["document_type"] = "national_implementation_measure"
        row["language"] = language
        row["jurisdiction"] = jurisdiction
        row["publication_date"] = _optional_text(row.get("nim_date"))
        row["effective_date"] = None
        row["url"] = _stringify(row.get("eurlex_url")) or _stringify(row.get("nim_resource_uri"))
        row["download_url"] = _resolve_download_url(row)
        row["full_text"] = _resolve_full_text(row)
        row["retrieved_at"] = timestamp
        row["content_path"] = _optional_text(row.get("text_path"))
        row["query_text"] = query_text
        documents.append(_sanitize_row(row))

    return documents


def resolve_cache_dir(source: SourceConfig, *, base_path: Path) -> Path:
    raw_value = source.settings.get("cache_dir", f".cache/eurlex_nim/{source.name}")
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise AdapterConfigError(
            "eurlex-nim adapter source.settings.cache_dir must be a non-empty string when set."
        )
    cache_dir = Path(raw_value.strip())
    if not cache_dir.is_absolute():
        cache_dir = (base_path / cache_dir).resolve()
    return cache_dir


def resolve_optional_positive_int(settings: dict[str, Any], key: str) -> int | None:
    raw_value = settings.get(key)
    if raw_value is None:
        return None
    if not isinstance(raw_value, int) or isinstance(raw_value, bool) or raw_value <= 0:
        raise AdapterConfigError(
            f"eurlex-nim adapter source.settings.{key} must be a positive integer when set."
        )
    return raw_value


def resolve_timeout_tuple(settings: dict[str, Any]) -> tuple[int, int]:
    connect_timeout = _require_positive_int(settings, "fulltext_connect_timeout_s", default=15)
    read_timeout = _require_positive_int(settings, "fulltext_read_timeout_s", default=90)
    return connect_timeout, read_timeout


def _resolve_seed_acts(query_text: str, settings: dict[str, Any]) -> pd.DataFrame:
    normalized_celex = normalize_legal_act_celex(query_text)
    if normalized_celex:
        parsed = parse_celex(normalized_celex)
        if parsed.descriptor not in ALLOWED_LEGAL_ACT_DESCRIPTORS:
            raise AdapterConfigError(
                "eurlex-nim direct CELEX input must point to a legal act with descriptor L, R, or D."
            )
        return pd.DataFrame(
            [
                {
                    "celex": normalized_celex,
                    "eu_act_title": "",
                    "eu_act_type": parsed.descriptor_label or "",
                    "year": parsed.year,
                }
            ]
        )

    rows = fetch_eurlex_job(
        {
            "scope": "ALL_ALL",
            "expert_scope": _resolve_expert_scope(settings),
            "lang": _resolve_search_language(settings),
            "terms": [query_text],
        },
        fields=_resolve_search_fields(settings),
        terms_per_query=1,
        page_size=_require_positive_int(settings, "page_size", default=100),
        max_pages=_require_positive_int(settings, "max_pages", default=20),
        min_interval_s=_require_non_negative_number(settings, "min_interval_s", default=1.6),
        timeout=_require_positive_int(settings, "timeout_s", default=45),
        retry_5xx=_require_non_negative_int(settings, "retry_5xx", default=3),
        debug=False,
    )
    if not rows:
        return pd.DataFrame()
    _, docs_df = build_eu_doc_tables(pd.DataFrame(rows))
    return select_eligible_celex_acts(docs_df)


def _retrieve_nim_rows(acts_df: pd.DataFrame, settings: dict[str, Any]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for row in acts_df.to_dict(orient="records"):
        celex = normalize_legal_act_celex(row.get("celex"))
        if not celex:
            continue
        page_df = get_national_transpositions_by_celex_ws(
            celex,
            page_size=_require_positive_int(settings, "nim_page_size", default=100),
            max_pages=resolve_optional_positive_int(settings, "nim_max_pages"),
            search_language=_resolve_search_language(settings),
            sleep_s=_require_non_negative_number(settings, "nim_sleep_s", default=0.2),
        )
        if page_df.empty:
            continue
        page_df = _normalize_nim_merge_frame(page_df)
        page_df["eu_act_title"] = _stringify(row.get("eu_act_title")) or _stringify(row.get("title"))
        page_df["eu_act_type"] = _stringify(row.get("eu_act_type"))
        page_df["year"] = row.get("year")
        frames.append(page_df)
    if not frames:
        return pd.DataFrame()
    combined = _normalize_nim_merge_frame(pd.concat(frames, ignore_index=True, sort=False))
    return enrich_nim_metadata(combined)


def _normalize_nim_merge_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        if "celex" not in out.columns:
            out["celex"] = pd.Series(dtype="object")
        if "nim_celex" not in out.columns:
            out["nim_celex"] = pd.Series(dtype="object")
        if "national_measure_id" not in out.columns:
            out["national_measure_id"] = pd.Series(dtype="object")
        return out

    out = df.copy()
    if "celex" not in out.columns and "act_celex" in out.columns:
        out["celex"] = out["act_celex"]
    if "celex" not in out.columns:
        out["celex"] = ""
    if "nim_celex" not in out.columns:
        out["nim_celex"] = ""
    if "national_measure_id" not in out.columns:
        out["national_measure_id"] = ""

    out["celex"] = out["celex"].map(normalize_legal_act_celex).fillna("")
    for column in ("nim_celex", "national_measure_id"):
        out[column] = out[column].map(_stringify)
    return out


def _build_document_id(source_name: str, row: dict[str, object]) -> str:
    member_state = _stringify(row.get("member_state_iso3")) or "UNK"
    seed = _stringify(row.get("national_measure_id")) or _stringify(row.get("nim_celex")) or "unknown"
    return f"{source_name}:NIM:{member_state}:{seed}"


def _build_summary(row: dict[str, object]) -> str | None:
    act_celex = _optional_text(row.get("celex"))
    eu_act_title = _optional_text(row.get("eu_act_title"))
    if not act_celex and not eu_act_title:
        return None
    if act_celex and eu_act_title:
        return f"National implementation measure for {act_celex}: {eu_act_title}"
    return f"National implementation measure for {act_celex or eu_act_title}"


def _resolve_download_url(row: dict[str, object]) -> str | None:
    text_source_url = _optional_text(row.get("text_source_url")) or _optional_text(row.get("source_url"))
    if text_source_url and text_source_url != "CACHE":
        return text_source_url
    return _optional_text(row.get("eurlex_url")) or _optional_text(row.get("nim_resource_uri"))


def _resolve_full_text(row: dict[str, object]) -> str | None:
    cleaned_text = _optional_text(row.get("full_text_clean"))
    if cleaned_text:
        return cleaned_text
    return _optional_text(row.get("full_text_raw")) or _optional_text(row.get("full_text"))


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


__all__ = [
    "resolve_cache_dir",
    "resolve_optional_positive_int",
    "resolve_timeout_tuple",
    "run_eurlex_nim_query_pipeline",
]
