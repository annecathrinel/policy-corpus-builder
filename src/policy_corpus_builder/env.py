from __future__ import annotations

"""Minimal local .env loader for untracked repository secrets."""

import os
from pathlib import Path


def load_local_env(*, start_path: str | Path | None = None, override: bool = False) -> Path | None:
    """Load key-value pairs from the nearest repository .env file if present."""
    env_path = find_local_env(start_path=start_path)
    if env_path is None:
        return None

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        parsed_value = _parse_env_value(value.strip())
        if override or key not in os.environ:
            os.environ[key] = parsed_value

    return env_path


def find_local_env(*, start_path: str | Path | None = None) -> Path | None:
    """Return the nearest .env file found while walking upward from start_path."""
    current = Path(start_path) if start_path is not None else Path(__file__)
    current = current.resolve()
    if current.is_file():
        current = current.parent

    for directory in (current, *current.parents):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate
    return None


def _parse_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value
