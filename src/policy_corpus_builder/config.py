"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import tomllib

from policy_corpus_builder.adapters import available_adapters
from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.schemas.config import (
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)

REQUIRED_TOP_LEVEL_SECTIONS = {
    "project",
    "queries",
    "sources",
    "normalization",
    "export",
}
ALLOWED_EXPORT_FORMATS = {"jsonl", "csv", "parquet"}
ALLOWED_DEDUPLICATION_FIELDS = {
    field.name for field in fields(NormalizedDocument) if field.name != "raw_metadata"
}


class ConfigValidationError(ValueError):
    """Raised when a TOML config violates the expected schema."""


def load_config(path: Path | str) -> dict[str, Any]:
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    if not isinstance(data, dict):
        raise ConfigValidationError("Configuration must deserialize to a TOML table.")

    return data


def load_and_validate_config(path: Path | str) -> BuilderConfig:
    config_path = Path(path)
    raw_data = load_config(config_path)
    return validate_config_dict(raw_data, base_path=config_path.parent)


def validate_config_dict(
    raw_data: dict[str, Any],
    *,
    base_path: Path | None = None,
) -> BuilderConfig:
    missing_sections = REQUIRED_TOP_LEVEL_SECTIONS - raw_data.keys()
    if missing_sections:
        missing_list = ", ".join(sorted(missing_sections))
        raise ConfigValidationError(f"Missing required top-level sections: {missing_list}.")

    unknown_sections = raw_data.keys() - REQUIRED_TOP_LEVEL_SECTIONS
    if unknown_sections:
        unknown_list = ", ".join(sorted(unknown_sections))
        raise ConfigValidationError(f"Unknown top-level sections: {unknown_list}.")

    project = _validate_project(raw_data["project"])
    queries = _validate_queries(raw_data["queries"], base_path=base_path)
    sources = _validate_sources(raw_data["sources"])
    normalization = _validate_normalization(raw_data["normalization"])
    export = _validate_export(raw_data["export"])

    return BuilderConfig(
        project=project,
        queries=queries,
        sources=sources,
        normalization=normalization,
        export=export,
    )


def format_config_summary(config: BuilderConfig) -> str:
    if config.queries.inventory:
        query_summary = f"inventory file ({config.queries.inventory})"
    else:
        query_summary = f"inline ({config.queries.query_count} entries)"

    lines = [
        "Config validation successful.",
        f"Project: {config.project.name}",
        f"Output directory: {config.project.output_dir}",
        f"Queries: {query_summary}",
        f"Sources: {', '.join(source.name for source in config.sources)}",
        f"Adapters: {', '.join(source.adapter for source in config.sources)}",
        (
            "Deduplication: enabled"
            f" ({', '.join(config.normalization.deduplicate_fields)})"
            if config.normalization.deduplicate
            else "Deduplication: disabled"
        ),
        f"Export formats: {', '.join(config.export.formats)}",
    ]
    return "\n".join(lines)


def _validate_project(raw_value: Any) -> ProjectConfig:
    data = _ensure_dict(raw_value, "project")
    _reject_unknown_keys(data, "project", {"name", "description", "output_dir"})

    name = _require_non_empty_string(data, "project.name")
    output_dir = _require_non_empty_string(data, "project.output_dir")
    description = _optional_non_empty_string(data.get("description"), "project.description")

    return ProjectConfig(name=name, output_dir=output_dir, description=description)


def _validate_queries(raw_value: Any, *, base_path: Path | None) -> QueriesConfig:
    data = _ensure_dict(raw_value, "queries")
    _reject_unknown_keys(data, "queries", {"inventory", "items"})

    inventory = data.get("inventory")
    items = data.get("items")

    if inventory is None and items is None:
        raise ConfigValidationError(
            "queries must define exactly one of 'inventory' or 'items'."
        )

    if inventory is not None and items is not None:
        raise ConfigValidationError(
            "queries cannot define both 'inventory' and 'items'; choose one."
        )

    if inventory is not None:
        inventory_path = _require_non_empty_string(data, "queries.inventory")
        resolved_path = (base_path / inventory_path).resolve() if base_path else Path(inventory_path)
        if not resolved_path.exists():
            raise ConfigValidationError(
                f"queries.inventory file does not exist: {inventory_path}"
            )
        return QueriesConfig(inventory=inventory_path, items=tuple())

    if not isinstance(items, list) or not items:
        raise ConfigValidationError("queries.items must be a non-empty list of strings.")

    cleaned_items = tuple(_require_string_value(item, "queries.items[]") for item in items)
    return QueriesConfig(inventory=None, items=cleaned_items)


def _validate_sources(raw_value: Any) -> tuple[SourceConfig, ...]:
    if not isinstance(raw_value, list) or not raw_value:
        raise ConfigValidationError("sources must be a non-empty array of tables.")

    known_adapters = set(available_adapters())
    validated_sources: list[SourceConfig] = []

    for index, source_value in enumerate(raw_value):
        label = f"sources[{index}]"
        data = _ensure_dict(source_value, label)
        _reject_unknown_keys(data, label, {"name", "adapter", "enabled", "settings"})

        name = _require_non_empty_string(data, f"{label}.name")
        adapter = _require_non_empty_string(data, f"{label}.adapter")
        enabled = _optional_bool(data.get("enabled"), f"{label}.enabled", default=True)
        settings = _optional_dict(data.get("settings"), f"{label}.settings")

        if adapter not in known_adapters:
            known_adapter_list = ", ".join(sorted(known_adapters))
            raise ConfigValidationError(
                f"{label}.adapter references unknown adapter '{adapter}'. "
                f"Known adapters: {known_adapter_list}."
            )

        validated_sources.append(
            SourceConfig(
                name=name,
                adapter=adapter,
                enabled=enabled,
                settings=settings,
            )
        )

    return tuple(validated_sources)


def _validate_normalization(raw_value: Any) -> NormalizationConfig:
    data = _ensure_dict(raw_value, "normalization")
    _reject_unknown_keys(data, "normalization", {"deduplicate", "deduplicate_fields"})

    deduplicate = _require_bool(data, "normalization.deduplicate")
    deduplicate_fields_raw = data.get("deduplicate_fields", [])

    if deduplicate:
        if not isinstance(deduplicate_fields_raw, list) or not deduplicate_fields_raw:
            raise ConfigValidationError(
                "normalization.deduplicate_fields must be a non-empty list when "
                "normalization.deduplicate is true."
            )
    elif deduplicate_fields_raw not in ([], None):
        if not isinstance(deduplicate_fields_raw, list):
            raise ConfigValidationError(
                "normalization.deduplicate_fields must be a list of strings."
            )

    deduplicate_fields = tuple(
        _require_string_value(value, "normalization.deduplicate_fields[]")
        for value in (deduplicate_fields_raw or [])
    )

    invalid_fields = sorted(set(deduplicate_fields) - ALLOWED_DEDUPLICATION_FIELDS)
    if invalid_fields:
        raise ConfigValidationError(
            "normalization.deduplicate_fields contains unknown metadata fields: "
            f"{', '.join(invalid_fields)}."
        )

    return NormalizationConfig(
        deduplicate=deduplicate,
        deduplicate_fields=deduplicate_fields,
    )


def _validate_export(raw_value: Any) -> ExportConfig:
    data = _ensure_dict(raw_value, "export")
    _reject_unknown_keys(data, "export", {"formats"})

    formats_raw = data.get("formats")
    if not isinstance(formats_raw, list) or not formats_raw:
        raise ConfigValidationError("export.formats must be a non-empty list of strings.")

    formats = tuple(_require_string_value(value, "export.formats[]").lower() for value in formats_raw)
    invalid_formats = sorted(set(formats) - ALLOWED_EXPORT_FORMATS)
    if invalid_formats:
        allowed = ", ".join(sorted(ALLOWED_EXPORT_FORMATS))
        raise ConfigValidationError(
            f"export.formats contains unsupported values: {', '.join(invalid_formats)}. "
            f"Allowed values: {allowed}."
        )

    return ExportConfig(formats=formats)


def _ensure_dict(raw_value: Any, label: str) -> dict[str, Any]:
    if not isinstance(raw_value, dict):
        raise ConfigValidationError(f"{label} must be a TOML table.")
    return raw_value


def _reject_unknown_keys(data: dict[str, Any], label: str, allowed_keys: set[str]) -> None:
    unknown_keys = data.keys() - allowed_keys
    if unknown_keys:
        unknown_list = ", ".join(sorted(unknown_keys))
        raise ConfigValidationError(f"{label} contains unknown keys: {unknown_list}.")


def _require_non_empty_string(data: dict[str, Any], key: str) -> str:
    if key.split(".")[-1] not in data:
        raise ConfigValidationError(f"Missing required value: {key}.")
    return _require_string_value(data[key.split(".")[-1]], key)


def _optional_non_empty_string(raw_value: Any, label: str) -> str | None:
    if raw_value is None:
        return None
    return _require_string_value(raw_value, label)


def _require_string_value(raw_value: Any, label: str) -> str:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ConfigValidationError(f"{label} must be a non-empty string.")
    return raw_value.strip()


def _require_bool(data: dict[str, Any], key: str) -> bool:
    if key.split(".")[-1] not in data:
        raise ConfigValidationError(f"Missing required value: {key}.")

    value = data[key.split(".")[-1]]
    if not isinstance(value, bool):
        raise ConfigValidationError(f"{key} must be a boolean.")
    return value


def _optional_bool(raw_value: Any, label: str, *, default: bool) -> bool:
    if raw_value is None:
        return default
    if not isinstance(raw_value, bool):
        raise ConfigValidationError(f"{label} must be a boolean.")
    return raw_value


def _optional_dict(raw_value: Any, label: str) -> dict[str, Any]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ConfigValidationError(f"{label} must be a TOML table.")
    return raw_value
