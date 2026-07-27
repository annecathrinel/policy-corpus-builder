from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _QueuedResponseSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.calls = 0

    def get(self, *args, **kwargs):
        response = self._responses[self.calls]
        self.calls += 1
        return response


class GetWithWafRetryTests(unittest.TestCase):
    # Regression tests for a real 2026-07 NZ smoke test: legislation.govt.nz
    # returned a WAF challenge for 92/97 full-text downloads, in a pattern
    # (a handful of early successes, then near-total blocking) consistent
    # with rate-based bot detection. This mirrors the fix already applied
    # to EUR-Lex NIM's HTTP 202 ("still generating, try again") full-text
    # fetch: retry with backoff instead of failing on the first response.

    def test_retries_challenge_then_succeeds(self) -> None:
        session = _QueuedResponseSession(
            [
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(200, "real document text"),
            ]
        )

        response = non_eu._get_with_waf_retry(
            session,
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
            headers={},
            timeout=10,
            max_retries=2,
            use_browser_impersonation=False,
        )

        self.assertEqual(session.calls, 3)
        self.assertFalse(non_eu._is_waf_challenge_response(response))
        self.assertEqual(response.text, "real document text")

    def test_gives_up_after_exhausting_retries_and_still_reports_challenge(self) -> None:
        session = _QueuedResponseSession(
            [
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
            ]
        )

        response = non_eu._get_with_waf_retry(
            session,
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
            headers={},
            timeout=10,
            max_retries=2,
            use_browser_impersonation=False,
        )

        # max_retries=2 means 3 total attempts (the initial try plus 2 retries).
        self.assertEqual(session.calls, 3)
        self.assertTrue(non_eu._is_waf_challenge_response(response))

    def test_does_not_retry_a_clean_success(self) -> None:
        session = _QueuedResponseSession([_FakeResponse(200, "real document text")])

        response = non_eu._get_with_waf_retry(
            session,
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
            headers={},
            timeout=10,
            max_retries=2,
            use_browser_impersonation=False,
        )

        self.assertEqual(session.calls, 1)
        self.assertEqual(response.text, "real document text")


class BrowserImpersonationRoutingTests(unittest.TestCase):
    # Regression coverage for _get_thread_impersonated_session: a 2026-07
    # diagnostic found a real Chrome browser fetched a www.legislation.govt.nz
    # URL with zero WAF challenges, on the first try, while requests with a
    # browser-like User-Agent still got challenged every time - pointing at
    # TLS/JA3 fingerprinting rather than a header check. _get_with_waf_retry
    # now prefers a curl_cffi session (which impersonates a real browser's
    # TLS fingerprint) over the plain requests session for hosts known to
    # run this kind of check.

    def test_waf_prone_host_uses_the_impersonated_session_when_available(self) -> None:
        plain_session = _QueuedResponseSession([_FakeResponse(200, "should not be used")])
        impersonated_session = _QueuedResponseSession([_FakeResponse(200, "real document text")])

        with patch.object(non_eu, "_get_thread_impersonated_session", return_value=impersonated_session):
            response = non_eu._get_with_waf_retry(
                plain_session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
            )

        self.assertEqual(plain_session.calls, 0)
        self.assertEqual(impersonated_session.calls, 1)
        self.assertEqual(response.text, "real document text")

    def test_falls_back_to_the_plain_session_when_curl_cffi_is_unavailable(self) -> None:
        plain_session = _QueuedResponseSession([_FakeResponse(200, "real document text")])

        with patch.object(non_eu, "_get_thread_impersonated_session", return_value=None):
            response = non_eu._get_with_waf_retry(
                plain_session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
            )

        self.assertEqual(plain_session.calls, 1)
        self.assertEqual(response.text, "real document text")

    def test_use_browser_impersonation_false_always_uses_the_plain_session(self) -> None:
        plain_session = _QueuedResponseSession([_FakeResponse(200, "real document text")])
        impersonated_session = _QueuedResponseSession([_FakeResponse(200, "should not be used")])

        with patch.object(non_eu, "_get_thread_impersonated_session", return_value=impersonated_session):
            response = non_eu._get_with_waf_retry(
                plain_session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
                use_browser_impersonation=False,
            )

        self.assertEqual(plain_session.calls, 1)
        self.assertEqual(impersonated_session.calls, 0)
        self.assertEqual(response.text, "real document text")

    def test_a_host_not_known_to_be_waf_prone_never_uses_the_impersonated_session(self) -> None:
        plain_session = _QueuedResponseSession([_FakeResponse(200, "real document text")])
        impersonated_session = _QueuedResponseSession([_FakeResponse(200, "should not be used")])

        with patch.object(non_eu, "_get_thread_impersonated_session", return_value=impersonated_session):
            response = non_eu._get_with_waf_retry(
                plain_session,
                "https://example.org/doc-1.xml",
                headers={},
                timeout=10,
            )

        self.assertEqual(plain_session.calls, 1)
        self.assertEqual(impersonated_session.calls, 0)
        self.assertEqual(response.text, "real document text")


class ImpersonatedSessionFactoryTests(unittest.TestCase):
    def test_returns_none_when_curl_cffi_is_not_installed(self) -> None:
        previous = getattr(non_eu._thread_local, "impersonated_session", None)
        try:
            if hasattr(non_eu._thread_local, "impersonated_session"):
                delattr(non_eu._thread_local, "impersonated_session")
            with patch.object(non_eu, "curl_cffi_requests", None):
                self.assertIsNone(non_eu._get_thread_impersonated_session())
        finally:
            if previous is None and hasattr(non_eu._thread_local, "impersonated_session"):
                delattr(non_eu._thread_local, "impersonated_session")
            elif previous is not None:
                non_eu._thread_local.impersonated_session = previous

    def test_reuses_the_same_session_across_calls_on_one_thread(self) -> None:
        previous = getattr(non_eu._thread_local, "impersonated_session", None)
        try:
            if hasattr(non_eu._thread_local, "impersonated_session"):
                delattr(non_eu._thread_local, "impersonated_session")

            class _FakeCurlCffiModule:
                class Session:
                    def __init__(self, impersonate: str) -> None:
                        self.impersonate = impersonate

            with patch.object(non_eu, "curl_cffi_requests", _FakeCurlCffiModule):
                first = non_eu._get_thread_impersonated_session()
                second = non_eu._get_thread_impersonated_session()

            self.assertIsNotNone(first)
            self.assertIs(first, second)
        finally:
            if previous is None and hasattr(non_eu._thread_local, "impersonated_session"):
                delattr(non_eu._thread_local, "impersonated_session")
            elif previous is not None:
                non_eu._thread_local.impersonated_session = previous


class ThrottleHostRequestTests(unittest.TestCase):
    def test_throttles_repeated_requests_to_a_waf_prone_host(self) -> None:
        sleep_calls: list[float] = []
        with patch.object(non_eu.time, "sleep", side_effect=sleep_calls.append):
            non_eu._throttle_host_request("https://www.legislation.govt.nz/act/public/2024/12")
            non_eu._throttle_host_request("https://www.legislation.govt.nz/act/public/2024/13")

        # First call for a host has no prior timestamp, so it must not wait;
        # the second call within the same instant must wait close to the
        # full configured interval for that host.
        self.assertEqual(len(sleep_calls), 1)
        expected_interval = non_eu._WAF_PRONE_HOST_MIN_INTERVAL_S["www.legislation.govt.nz"]
        self.assertGreater(sleep_calls[0], 0)
        self.assertLessEqual(sleep_calls[0], expected_interval)

    def test_is_a_no_op_for_hosts_not_known_to_be_waf_prone(self) -> None:
        sleep_calls: list[float] = []
        with patch.object(non_eu.time, "sleep", side_effect=sleep_calls.append):
            non_eu._throttle_host_request("https://example.org/doc-1")
            non_eu._throttle_host_request("https://example.org/doc-2")

        self.assertEqual(sleep_calls, [])


class PdfModeWafDetectionTests(unittest.TestCase):
    def test_pdf_candidate_reports_waf_challenge_instead_of_pdf_unavailable(self) -> None:
        # Regression test: the pdf branch never checked for a WAF challenge
        # at all before this fix, so a challenged PDF request (e.g. one of
        # NZ's pdf-format candidates) was mislabeled as the unrelated
        # "pdf_unavailable" error instead of the actionable "waf_challenge".
        session = _QueuedResponseSession(
            [_FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"})] * 3
        )

        with patch.object(non_eu, "_get_thread_session", return_value=session), patch.object(
            non_eu, "_get_thread_robots", return_value=type("AllowAll", (), {"allowed": staticmethod(lambda url: True)})()
        ), patch.object(non_eu, "_get_thread_impersonated_session", return_value=None):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "NZ",
                    "jurisdiction": "New Zealand",
                    # No "url"/"text_url"/"doc_url" fallback on purpose, so
                    # get_url_candidates yields exactly one candidate (the
                    # pdf) - this isolates the test to the pdf branch's WAF
                    # detection rather than also exercising a fallback html
                    # candidate.
                    "pdf_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(enriched["full_text_error"], "waf_challenge")


class AddFullTextsParallelCurlCffiDiagnosticTests(unittest.TestCase):
    # Regression coverage for the observability gap behind a real run: a
    # 2026-07 NZ smoke test still showed 9x waf_challenge and only 5/33
    # full texts retrieved despite the curl_cffi impersonation fix already
    # being in the code, with nothing in the log to say whether curl_cffi
    # was actually active or had silently failed to install. This asserts
    # add_full_texts_parallel now prints which case it's in.
    def test_prints_available_when_curl_cffi_is_importable(self) -> None:
        stdout = StringIO()

        class _FakeCurlCffiModule:
            class Session:
                def __init__(self, impersonate: str) -> None:
                    self.impersonate = impersonate

        with (
            patch.object(non_eu, "curl_cffi_requests", _FakeCurlCffiModule),
            patch.object(
                non_eu,
                "enrich_one_record_fulltext",
                return_value={"full_text": "some text", "full_text_error": ""},
            ),
            redirect_stdout(stdout),
        ):
            non_eu.add_full_texts_parallel(
                [{"source": "NZ", "url": "https://www.legislation.govt.nz/act/public/2024/1"}],
                us_api_key=None,
            )

        self.assertIn(
            "[FULLTEXT] curl_cffi browser-TLS impersonation: available",
            stdout.getvalue(),
        )

    def test_prints_not_available_when_curl_cffi_is_missing(self) -> None:
        stdout = StringIO()

        with (
            patch.object(non_eu, "curl_cffi_requests", None),
            patch.object(
                non_eu,
                "enrich_one_record_fulltext",
                return_value={"full_text": "some text", "full_text_error": ""},
            ),
            redirect_stdout(stdout),
        ):
            non_eu.add_full_texts_parallel(
                [{"source": "NZ", "url": "https://www.legislation.govt.nz/act/public/2024/1"}],
                us_api_key=None,
            )

        self.assertIn(
            "[FULLTEXT] curl_cffi browser-TLS impersonation: NOT available",
            stdout.getvalue(),
        )

    def test_prints_nothing_extra_when_records_is_empty(self) -> None:
        stdout = StringIO()

        with redirect_stdout(stdout):
            result = non_eu.add_full_texts_parallel([], us_api_key=None)

        self.assertEqual(result, [])
        self.assertEqual(stdout.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
