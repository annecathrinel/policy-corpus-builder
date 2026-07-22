"""JSONL export for normalized documents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from policy_corpus_builder.models import NormalizedDocument

JSONL_FILENAME = "documents.jsonl"

def _sanitize_json_value(value):
    if isinstance(value, str):
        return "".join(
            ch if not (0xD800 <= ord(ch) <= 0xDFFF) else "\uFFFD"
            for ch in value
        )
    if isinstance(value, dict):
        return {k: _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(v) for v in value]
    return value


def export_documents_jsonl(
    documents: Iterable[NormalizedDocument],
    *,
    output_dir: Path,
) -> Path:
    """Write normalized documents to a JSONL file and return the output path."""

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / JSONL_FILENAME

    with output_path.open("w", encoding="utf-8", newline="\n") as fh:
        for document in documents:
            payload = _sanitize_json_value(document.to_dict())
            fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            fh.write("\n")

    return output_path
