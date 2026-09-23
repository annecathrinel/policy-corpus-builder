"""Pure, source-aware document classification."""

from .case_law import CaseLawDecision, classify_case_law

__all__ = ["CaseLawDecision", "classify_case_law"]
