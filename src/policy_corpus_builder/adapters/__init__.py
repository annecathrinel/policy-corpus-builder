"""Adapter registry placeholders."""

from policy_corpus_builder.adapters.base import SourceAdapter

ADAPTERS: dict[str, type[SourceAdapter]] = {}


def available_adapters() -> list[str]:
    return sorted(ADAPTERS) or ["placeholder"]
