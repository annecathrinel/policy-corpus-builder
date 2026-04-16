"""Adapter registry plumbing."""

from __future__ import annotations

from importlib import import_module

from policy_corpus_builder.adapters.base import SourceAdapter
from policy_corpus_builder.adapters.local_file import LocalFileAdapter
from policy_corpus_builder.adapters.placeholder import PlaceholderAdapter

ADAPTERS: dict[str, type[SourceAdapter]] = {
    LocalFileAdapter.name: LocalFileAdapter,
    PlaceholderAdapter.name: PlaceholderAdapter,
}

LAZY_ADAPTERS: dict[str, tuple[str, str]] = {
    "eurlex": ("policy_corpus_builder.adapters.eurlex_adapter", "EurlexAdapter"),
    "non-eu": ("policy_corpus_builder.adapters.non_eu_adapter", "NonEUAdapter"),
}


def get_adapter(adapter_name: str) -> SourceAdapter:
    try:
        adapter_class = _get_adapter_class(adapter_name)
    except KeyError as exc:
        known_adapters = ", ".join(sorted(set(ADAPTERS) | set(LAZY_ADAPTERS)))
        raise KeyError(
            f"Unknown adapter '{adapter_name}'. Known adapters: {known_adapters}."
        ) from exc

    return adapter_class()


def register_adapter(adapter_class: type[SourceAdapter]) -> None:
    adapter_name = getattr(adapter_class, "name", None)
    if not isinstance(adapter_name, str) or not adapter_name.strip():
        raise ValueError("Adapter classes must define a non-empty string 'name'.")

    ADAPTERS[adapter_name] = adapter_class


def available_adapters() -> list[str]:
    return sorted(set(ADAPTERS) | set(LAZY_ADAPTERS))


def _get_adapter_class(adapter_name: str) -> type[SourceAdapter]:
    adapter_class = ADAPTERS.get(adapter_name)
    if adapter_class is not None:
        return adapter_class

    lazy_target = LAZY_ADAPTERS.get(adapter_name)
    if lazy_target is None:
        raise KeyError(adapter_name)

    module_name, class_name = lazy_target
    module = import_module(module_name)
    adapter_class = getattr(module, class_name)
    ADAPTERS[adapter_name] = adapter_class
    return adapter_class


__all__ = [
    "ADAPTERS",
    "SourceAdapter",
    "available_adapters",
    "get_adapter",
    "register_adapter",
]
