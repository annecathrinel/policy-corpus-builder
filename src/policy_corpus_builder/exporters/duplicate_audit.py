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
DUPLICATE_GROUPS_SUMMARY_CSV_FILENAME = "duplicate_groups_summary.csv"
DUPLICATE_GROUPS_SUMMARY_JSON_FILENAME = "duplicate_groups_summary.json"

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

_SUMMARY_COLUMNS = (
    "review_rank",
    "duplicate_group_id",
    "signal",
    "group_size",
    "representative_value",
    "jurisdictions",
    "source_names",
    "publication_date_min",
    "publication_date_max",
    "spans_multiple_jurisdictions",
    "spans_multiple_source_names",
    "document_ids",
    "review_interest_score",
    "review_interest_reasons",
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
    group_summary_rows = build_duplicate_group_summary_rows(rows)
    aggregate_summary = build_duplicate_group_aggregate_summary(group_summary_rows)
    csv_path = output_dir / DUPLICATE_AUDIT_CSV_FILENAME
    jsonl_path = output_dir / DUPLICATE_AUDIT_JSONL_FILENAME
    summary_csv_path = output_dir / DUPLICATE_GROUPS_SUMMARY_CSV_FILENAME
    summary_json_path = output_dir / DUPLICATE_GROUPS_SUMMARY_JSON_FILENAME

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    with jsonl_path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            fh.write("\n")

    with summary_csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(group_summary_rows)

    summary_json_path.write_text(
        json.dumps(aggregate_summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

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


def build_duplicate_group_summary_rows(
    rows: Iterable[dict[str, object]],
) -> list[dict[str, object]]:
    """Return one compact manual-review row per likely duplicate group."""

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["duplicate_group_id"])].append(row)

    summary_rows: list[dict[str, object]] = []
    for duplicate_group_id, group_rows in grouped.items():
        first_row = group_rows[0]
        jurisdictions = _sorted_present_values(row.get("jurisdiction") for row in group_rows)
        source_names = _sorted_present_values(row.get("source_name") for row in group_rows)
        publication_dates = _sorted_present_values(
            row.get("publication_date") for row in group_rows
        )
        document_ids = _sorted_present_values(row.get("document_id") for row in group_rows)
        score, reasons = _score_group_for_review(
            group_size=int(first_row["group_size"]),
            jurisdictions=jurisdictions,
            source_names=source_names,
            publication_dates=publication_dates,
        )
        summary_rows.append(
            {
                "review_rank": 0,
                "duplicate_group_id": duplicate_group_id,
                "signal": first_row["signal"],
                "group_size": first_row["group_size"],
                "representative_value": first_row["representative_value"],
                "jurisdictions": " | ".join(jurisdictions),
                "source_names": " | ".join(source_names),
                "publication_date_min": publication_dates[0] if publication_dates else "",
                "publication_date_max": publication_dates[-1] if publication_dates else "",
                "spans_multiple_jurisdictions": len(jurisdictions) > 1,
                "spans_multiple_source_names": len(source_names) > 1,
                "document_ids": " | ".join(document_ids),
                "review_interest_score": score,
                "review_interest_reasons": " | ".join(reasons),
            }
        )

    summary_rows.sort(
        key=lambda row: (
            -int(row["review_interest_score"]),
            -int(row["group_size"]),
            str(row["signal"]),
            str(row["duplicate_group_id"]),
        )
    )
    for rank, row in enumerate(summary_rows, start=1):
        row["review_rank"] = rank
    return summary_rows


def build_duplicate_group_aggregate_summary(
    group_summary_rows: Iterable[dict[str, object]],
) -> dict[str, object]:
    """Return compact aggregate counts and top review candidates."""

    rows = list(group_summary_rows)
    document_ids: set[str] = set()
    for row in rows:
        document_ids.update(_split_joined_values(row["document_ids"]))

    return {
        "duplicate_group_count": len(rows),
        "document_count": len(document_ids),
        "groups_by_signal": _count_rows_by_field(rows, "signal"),
        "groups_by_jurisdiction": _count_rows_by_joined_field(rows, "jurisdictions"),
        "groups_by_jurisdiction_combination": _count_rows_by_field(rows, "jurisdictions"),
        "groups_by_source_name": _count_rows_by_joined_field(rows, "source_names"),
        "groups_by_source_name_combination": _count_rows_by_field(rows, "source_names"),
        "top_largest_groups": _top_groups(rows, sort_fields=("group_size",), limit=10),
        "top_review_candidates": _top_groups(
            rows,
            sort_fields=("review_interest_score", "group_size"),
            limit=20,
        ),
    }


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


def _sorted_present_values(values: Iterable[object]) -> list[str]:
    present = {str(value).strip() for value in values if str(value or "").strip()}
    return sorted(present)


def _score_group_for_review(
    *,
    group_size: int,
    jurisdictions: list[str],
    source_names: list[str],
    publication_dates: list[str],
) -> tuple[int, list[str]]:
    score = group_size
    reasons = [f"group_size={group_size}"]
    if len(jurisdictions) > 1:
        score += 3
        reasons.append("spans_multiple_jurisdictions")
    if len(source_names) > 1:
        score += 2
        reasons.append("spans_multiple_source_names")
    if len(publication_dates) > 1 and publication_dates[0] != publication_dates[-1]:
        score += 1
        reasons.append("publication_date_range")
    return score, reasons


def _split_joined_values(value: object) -> list[str]:
    return [item.strip() for item in str(value).split("|") if item.strip()]


def _count_rows_by_field(
    rows: Iterable[dict[str, object]],
    field_name: str,
) -> list[dict[str, object]]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        key = str(row[field_name]) if row[field_name] else "(missing)"
        counts[key] += 1
    return [
        {field_name: key, "group_count": count}
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _count_rows_by_joined_field(
    rows: Iterable[dict[str, object]],
    field_name: str,
) -> list[dict[str, object]]:
    count_field_name = field_name.removesuffix("s")
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        values = _split_joined_values(row[field_name])
        if not values:
            counts["(missing)"] += 1
        for value in values:
            counts[value] += 1
    return [
        {count_field_name: key, "group_count": count}
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _top_groups(
    rows: Iterable[dict[str, object]],
    *,
    sort_fields: tuple[str, ...],
    limit: int,
) -> list[dict[str, object]]:
    sorted_rows = sorted(
        rows,
        key=lambda row: tuple(-int(row[field]) for field in sort_fields)
        + (str(row["duplicate_group_id"]),),
    )
    fields = (
        "review_rank",
        "duplicate_group_id",
        "signal",
        "group_size",
        "representative_value",
        "jurisdictions",
        "source_names",
        "publication_date_min",
        "publication_date_max",
        "spans_multiple_jurisdictions",
        "spans_multiple_source_names",
        "review_interest_score",
        "review_interest_reasons",
    )
    return [
        {field: row[field] for field in fields}
        for row in sorted_rows[:limit]
    ]


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
