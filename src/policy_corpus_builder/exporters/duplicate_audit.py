"""Duplicate-audit artifact export for top-level corpus builds."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.utils.celex import extract_celex_token

DUPLICATE_AUDIT_CSV_FILENAME = "likely_duplicates.csv"
DUPLICATE_AUDIT_JSONL_FILENAME = "likely_duplicates.jsonl"

_AUDIT_COLUMNS = (
    "duplicate_group_id",
    "signal",
    "group_size",
    "representative_value",
    "document_id",
    "source_name",
    "source_document_id",
    "title",
    "normalized_title",
    "url",
    "normalized_url",
    "celex",
    "jurisdiction",
    "publication_date",
)


def export_duplicate_audit(
    documents: Iterable[NormalizedDocument],
    *,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write conservative likely-duplicate audit artifacts.

    The audit is observational only. It does not remove or reorder documents.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    rows = build_duplicate_audit_rows(tuple(documents))
    csv_path = output_dir / DUPLICATE_AUDIT_CSV_FILENAME
    jsonl_path = output_dir / DUPLICATE_AUDIT_JSONL_FILENAME

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    with jsonl_path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            fh.write("\n")

    return csv_path, jsonl_path


def build_duplicate_audit_rows(
    documents: tuple[NormalizedDocument, ...],
) -> list[dict[str, object]]:
    """Return one audit row per document in each likely duplicate group."""

    document_context = [_build_document_context(document) for document in documents]
    groups: list[tuple[str, str, list[dict[str, str]]]] = []
    for signal, key_name in (
        ("document_id", "document_id"),
        ("source_document_id", "source_document_id"),
        ("celex", "celex"),
        ("normalized_url", "normalized_url"),
        ("normalized_title", "normalized_title"),
    ):
        groups.extend(_find_groups(document_context, signal=signal, key_name=key_name))

    rows: list[dict[str, object]] = []
    for group_index, (signal, representative_value, contexts) in enumerate(groups, start=1):
        duplicate_group_id = f"dup-{group_index:06d}"
        for context in contexts:
            rows.append(
                {
                    "duplicate_group_id": duplicate_group_id,
                    "signal": signal,
                    "group_size": len(contexts),
                    "representative_value": representative_value,
                    "document_id": context["document_id"],
                    "source_name": context["source_name"],
                    "source_document_id": context["source_document_id"],
                    "title": context["title"],
                    "normalized_title": context["normalized_title"],
                    "url": context["url"],
                    "normalized_url": context["normalized_url"],
                    "celex": context["celex"],
                    "jurisdiction": context["jurisdiction"],
                    "publication_date": context["publication_date"],
                }
            )
    return rows


def _find_groups(
    contexts: list[dict[str, str]],
    *,
    signal: str,
    key_name: str,
) -> list[tuple[str, str, list[dict[str, str]]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for context in contexts:
        key = context[key_name]
        if key:
            grouped[key].append(context)

    groups = [
        (signal, key, sorted(items, key=lambda item: item["document_id"]))
        for key, items in grouped.items()
        if len(items) > 1
    ]
    return sorted(groups, key=lambda item: (item[0], item[1]))


def _build_document_context(document: NormalizedDocument) -> dict[str, str]:
    normalized_title = _normalize_text(document.title)
    if len(normalized_title) < 16:
        normalized_title = ""
    normalized_url = _normalize_url(document.url)
    return {
        "document_id": _normalize_identifier(document.document_id),
        "source_name": document.source_name or "",
        "source_document_id": _normalize_identifier(document.source_document_id),
        "title": document.title or "",
        "normalized_title": normalized_title,
        "url": document.url or "",
        "normalized_url": normalized_url,
        "celex": _extract_document_celex(document),
        "jurisdiction": document.jurisdiction or "",
        "publication_date": document.publication_date or "",
    }


def _normalize_identifier(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", "", str(value).strip()).upper()


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().casefold())


def _normalize_url(value: str | None) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    netloc = hostname
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    path = re.sub(r"/+", "/", parts.path or "/")
    if path != "/":
        path = path.rstrip("/")
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit((scheme, netloc, path, query, ""))


def _extract_document_celex(document: NormalizedDocument) -> str:
    candidates: list[object] = [
        document.source_document_id,
        document.document_id,
        document.url,
        document.download_url,
    ]
    raw_record = document.raw_metadata.get("raw_record")
    if isinstance(raw_record, dict):
        candidates.extend(
            raw_record.get(key)
            for key in (
                "celex",
                "celex_full",
                "nim_celex",
                "act_celex",
                "source_celex",
            )
        )
    for candidate in candidates:
        token = extract_celex_token(str(candidate)) if candidate else None
        if token:
            return token.upper()
    return ""

