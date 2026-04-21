"""Public package for policy corpus building."""

from policy_corpus_builder.corpus_builder import (
    CorpusBuildValidationError,
    PolicyCorpusBuildResult,
    build_policy_corpus,
)
from policy_corpus_builder.env import load_local_env

__all__ = [
    "__version__",
    "CorpusBuildValidationError",
    "PolicyCorpusBuildResult",
    "build_policy_corpus",
]

load_local_env()

__version__ = "0.1.0"
