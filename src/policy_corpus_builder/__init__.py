"""Public package for policy corpus building."""

from policy_corpus_builder.env import load_local_env

__all__ = ["__version__"]

load_local_env()

__version__ = "0.1.0"
