"""Supported EUR-Lex NIM helper surface used by the public adapter path."""

from policy_corpus_builder.adapters.eurlex_nim_supported.surface import (
    batch_fetch_nim_fulltext,
    enrich_nim_metadata,
    get_national_transpositions_by_celex_ws,
    normalize_legal_act_celex,
    select_eligible_celex_acts,
)
from policy_corpus_builder.adapters.eurlex_nim_supported.workflow import (
    resolve_cache_dir,
    resolve_optional_positive_int,
    resolve_timeout_tuple,
    run_eurlex_nim_query_pipeline,
)

__all__ = [
    "batch_fetch_nim_fulltext",
    "enrich_nim_metadata",
    "get_national_transpositions_by_celex_ws",
    "normalize_legal_act_celex",
    "resolve_cache_dir",
    "resolve_optional_positive_int",
    "resolve_timeout_tuple",
    "run_eurlex_nim_query_pipeline",
    "select_eligible_celex_acts",
]
