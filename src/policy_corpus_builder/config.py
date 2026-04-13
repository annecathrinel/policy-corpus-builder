"""Configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import tomllib


def load_config(path: Path | str) -> dict[str, Any]:
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    if not isinstance(data, dict):
        raise ValueError("Configuration must deserialize to a TOML table.")

    return data
