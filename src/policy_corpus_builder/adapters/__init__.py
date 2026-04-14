"""Adapter registry plumbing."""

from policy_corpus_builder.adapters.base import SourceAdapter
from policy_corpus_builder.adapters.local_file import LocalFileAdapter
from policy_corpus_builder.adapters.placeholder import PlaceholderAdapter

ADAPTERS: dict[str, type[SourceAdapter]] = {
    LocalFileAdapter.name: LocalFileAdapter,
    PlaceholderAdapter.name: PlaceholderAdapter,
}


def get_adapter(adapter_name: str) -> SourceAdapter:
    try:
        adapter_class = ADAPTERS[adapter_name]
    except KeyError as exc:
        known_adapters = ", ".join(sorted(ADAPTERS))
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
    return sorted(ADAPTERS)


__all__ = [
    "ADAPTERS",
    "SourceAdapter",
    "available_adapters",
    "get_adapter",
    "register_adapter",
]
