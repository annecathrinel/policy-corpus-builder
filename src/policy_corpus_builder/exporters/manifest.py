"""JSON run manifest export."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MANIFEST_FILENAME = "run-manifest.json"


def export_run_manifest(manifest: dict[str, Any], *, output_dir: Path) -> Path:
    """Write a machine-readable run manifest and return the output path."""

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / MANIFEST_FILENAME
    output_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path
