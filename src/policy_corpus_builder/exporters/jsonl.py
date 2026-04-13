"""JSONL export for normalized documents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from policy_corpus_builder.models import NormalizedDocument

JSONL_FILENAME = "documents.jsonl"


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
            fh.write(json.dumps(document.to_dict(), ensure_ascii=False, sort_keys=True))
            fh.write("\n")

    return output_path
