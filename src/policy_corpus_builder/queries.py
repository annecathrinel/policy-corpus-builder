"""Query inventory loading and normalization."""

from __future__ import annotations

from pathlib import Path

from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import BuilderConfig


def load_queries(config: BuilderConfig, *, base_path: Path) -> tuple[Query, ...]:
    """Load queries from config and normalize them into a single representation."""

    if config.queries.inventory:
        return _load_inventory_queries(config.queries.inventory, base_path=base_path)

    return tuple(
        Query(
            text=query_text,
            query_id=f"inline-{index:03d}",
            origin="inline",
        )
        for index, query_text in enumerate(config.queries.items, start=1)
    )


def _load_inventory_queries(inventory_path: str, *, base_path: Path) -> tuple[Query, ...]:
    inventory_file = (base_path / inventory_path).resolve()
    query_items: list[Query] = []

    for line_number, raw_line in enumerate(
        inventory_file.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        normalized_line = raw_line.strip()
        if not normalized_line or normalized_line.startswith("#"):
            continue

        query_items.append(
            Query(
                text=normalized_line,
                query_id=f"inventory-{line_number:03d}",
                origin="inventory",
                source_path=inventory_path,
            )
        )

    return tuple(query_items)
