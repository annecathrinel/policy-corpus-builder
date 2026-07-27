"""Shared pytest fixtures for the policy-corpus-builder test suite."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _no_real_sleeps_in_tests():
    """Never actually sleep during tests, regardless of which retry/backoff
    or rate-limiting code path a test happens to exercise.

    time.sleep is a single shared function on the stdlib time module, so
    patching it here covers every module that does `import time` and calls
    time.sleep(...) - EUR-Lex/NIM retry backoff, the non-eu WAF-challenge
    retry and per-host throttle, etc. - without each test needing its own
    patch.object(some_module.time, "sleep") boilerplate. Tests that already
    patch time.sleep themselves are unaffected; this is just a safety net
    for ones that don't, so a real backoff delay never silently slows down
    (or, worse, makes flaky) the suite.
    """
    with patch("time.sleep", return_value=None):
        yield


@pytest.fixture(autouse=True)
def _reset_non_eu_host_throttle_state():
    """Reset non_eu.py's per-host WAF throttle bookkeeping between tests.

    This is state shared at module scope (by design - it needs to persist
    across concurrent full-text fetch worker threads in production), so
    without a reset it would otherwise persist across unrelated test
    methods too.
    """
    try:
        from policy_corpus_builder.adapters import non_eu
    except Exception:
        yield
        return

    non_eu._host_last_request_monotonic.clear()
    yield
    non_eu._host_last_request_monotonic.clear()
