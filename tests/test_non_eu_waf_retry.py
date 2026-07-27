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
            use_browser_challenge_solver=False,
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

    def test_retries_a_waf_block_then_succeeds(self) -> None:
        # Regression test: a 2026-07-27 AUS smoke test
        # (www.legislation.gov.au) got a clean HTTP 200 for its first ~12
        # search requests, then HTTP 403 for every remaining request,
        # permanently, for the rest of the run - a hard block rather than
        # the interactive 202 challenge NZ/UK showed. _get_with_waf_retry
        # now retries on that too (see _is_waf_block_response).
        session = _QueuedResponseSession(
            [
                _FakeResponse(403, ""),
                _FakeResponse(200, "real document text"),
            ]
        )

        response = non_eu._get_with_waf_retry(
            session,
            "https://www.legislation.gov.au/search/text(%22biodiversity%22,nameAndText,contains)/pointintime(Latest)",
            headers={},
            timeout=10,
            max_retries=2,
            use_browser_impersonation=False,
        )

        self.assertEqual(session.calls, 2)
        self.assertFalse(non_eu._is_waf_block_response(response))
        self.assertEqual(response.text, "real document text")

    def test_gives_up_after_exhausting_retries_and_still_reports_block(self) -> None:
        session = _QueuedResponseSession([_FakeResponse(403, "")] * 3)

        response = non_eu._get_with_waf_retry(
            session,
            "https://www.legislation.gov.au/search/text(%22biodiversity%22,nameAndText,contains)/pointintime(Latest)",
            headers={},
            timeout=10,
            max_retries=2,
            use_browser_impersonation=False,
            use_browser_challenge_solver=False,
        )

        self.assertEqual(session.calls, 3)
        self.assertTrue(non_eu._is_waf_block_response(response))


class WafResponseClassificationTests(unittest.TestCase):
    def test_classifies_202_with_challenge_header_as_waf_challenge(self) -> None:
        response = _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"})
        self.assertTrue(non_eu._is_waf_challenge_response(response))
        self.assertFalse(non_eu._is_waf_block_response(response))
        self.assertEqual(non_eu._classify_waf_response(response), "waf_challenge")

    def test_classifies_403_as_waf_block(self) -> None:
        response = _FakeResponse(403, "")
        self.assertFalse(non_eu._is_waf_challenge_response(response))
        self.assertTrue(non_eu._is_waf_block_response(response))
        self.assertEqual(non_eu._classify_waf_response(response), "waf_block")

    def test_classifies_x_amzn_waf_action_block_header_as_waf_block_regardless_of_status(self) -> None:
        response = _FakeResponse(200, "", headers={"x-amzn-waf-action": "block"})
        self.assertTrue(non_eu._is_waf_block_response(response))
        self.assertEqual(non_eu._classify_waf_response(response), "waf_block")

    def test_classifies_a_clean_200_as_neither(self) -> None:
        response = _FakeResponse(200, "ok")
        self.assertIsNone(non_eu._classify_waf_response(response))

    def test_classifies_an_unrelated_error_status_as_neither(self) -> None:
        # A genuine 404/503/etc. should not be mistaken for a WAF response -
        # only the specific signals evidenced in real smoke tests (202 +
        # challenge header, 403, or an explicit block header) count.
        for status in (404, 500, 503):
            with self.subTest(status=status):
                response = _FakeResponse(status, "")
                self.assertIsNone(non_eu._classify_waf_response(response))

    def test_classifies_none_response_as_neither(self) -> None:
        self.assertIsNone(non_eu._classify_waf_response(None))


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
        ), patch.object(non_eu, "_get_thread_impersonated_session", return_value=None), patch.object(
            non_eu, "_get_thread_browser_waf_cookies", return_value=None
        ):
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


class BrowserChallengeSolverFallbackTests(unittest.TestCase):
    # Regression coverage for the follow-up fix after curl_cffi TLS
    # impersonation alone turned out not to be enough: a 2026-07-27 live NZ
    # run with it deployed still got waf_challenge on 16/17 full-text
    # requests, statistically the same as the 92/97 (94.8%) rate *before*
    # that fix - see _get_thread_impersonated_session's 2026-07-27 update.
    # _get_with_waf_retry now falls back to actually solving the challenge
    # in a real headless browser (_solve_waf_challenge_via_browser) after
    # its plain/impersonated retries are exhausted. None of these tests
    # launch a real browser - the solver itself is mocked out via
    # _get_thread_browser_waf_cookies, so behavior here doesn't depend on
    # whether playwright happens to be installed in the environment running
    # the suite.

    def test_falls_back_to_browser_solved_cookies_after_exhausting_retries(self) -> None:
        session = _QueuedResponseSession(
            [
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(200, "real document text"),
            ]
        )

        with patch.object(
            non_eu, "_get_thread_browser_waf_cookies", return_value={"aws-waf-token": "solved"}
        ) as mock_solve:
            response = non_eu._get_with_waf_retry(
                session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
                max_retries=2,
                use_browser_impersonation=False,
            )

        # 3 attempts to exhaust the normal retry budget, then 1 more with
        # the browser-solved cookies attached.
        self.assertEqual(session.calls, 4)
        mock_solve.assert_called_once()
        self.assertFalse(non_eu._is_waf_challenge_response(response))
        self.assertEqual(response.text, "real document text")

    def test_gives_up_when_browser_solve_returns_no_cookies(self) -> None:
        session = _QueuedResponseSession(
            [_FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"})] * 3
        )

        with patch.object(non_eu, "_get_thread_browser_waf_cookies", return_value=None) as mock_solve:
            response = non_eu._get_with_waf_retry(
                session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
                max_retries=2,
                use_browser_impersonation=False,
            )

        # No extra request when the solver couldn't get cookies (playwright
        # missing, browser launch failure, or a genuinely unsolved
        # challenge) - 3, not 4.
        self.assertEqual(session.calls, 3)
        mock_solve.assert_called_once()
        self.assertTrue(non_eu._is_waf_challenge_response(response))

    def test_use_browser_challenge_solver_false_skips_the_fallback_entirely(self) -> None:
        session = _QueuedResponseSession(
            [_FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"})] * 3
        )

        with patch.object(non_eu, "_get_thread_browser_waf_cookies") as mock_solve:
            response = non_eu._get_with_waf_retry(
                session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
                max_retries=2,
                use_browser_impersonation=False,
                use_browser_challenge_solver=False,
            )

        self.assertEqual(session.calls, 3)
        mock_solve.assert_not_called()
        self.assertTrue(non_eu._is_waf_challenge_response(response))

    def test_a_host_not_known_to_be_waf_prone_never_attempts_a_browser_solve(self) -> None:
        session = _QueuedResponseSession([_FakeResponse(404, "")] * 3)

        with patch.object(non_eu, "_get_thread_browser_waf_cookies") as mock_solve:
            non_eu._get_with_waf_retry(
                session,
                "https://example.org/doc-1.xml",
                headers={},
                timeout=10,
                max_retries=2,
                use_browser_impersonation=False,
            )

        mock_solve.assert_not_called()

    def test_browser_solved_cookies_are_passed_on_the_final_request(self) -> None:
        captured_kwargs: list[dict] = []

        class _CapturingSession:
            def __init__(self, responses: list[_FakeResponse]):
                self._responses = list(responses)
                self.calls = 0

            def get(self, *args, **kwargs):
                captured_kwargs.append(kwargs)
                response = self._responses[self.calls]
                self.calls += 1
                return response

        session = _CapturingSession(
            [
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"}),
                _FakeResponse(200, "real document text"),
            ]
        )

        with patch.object(
            non_eu, "_get_thread_browser_waf_cookies", return_value={"aws-waf-token": "solved"}
        ):
            non_eu._get_with_waf_retry(
                session,
                "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                headers={},
                timeout=10,
                max_retries=2,
                use_browser_impersonation=False,
            )

        self.assertEqual(captured_kwargs[-1].get("cookies"), {"aws-waf-token": "solved"})


class BrowserWafChallengeSolverTests(unittest.TestCase):
    def test_returns_none_when_playwright_is_not_installed(self) -> None:
        with patch.object(non_eu, "sync_playwright", None):
            self.assertIsNone(
                non_eu._solve_waf_challenge_via_browser("https://www.legislation.govt.nz/act/public/2024/12")
            )

    def test_returns_cookie_dict_from_a_mocked_browser_session(self) -> None:
        class _FakePage:
            def goto(self, url, timeout=None, wait_until=None):
                pass

            def wait_for_load_state(self, state, timeout=None):
                pass

        class _FakeContext:
            def new_page(self):
                return _FakePage()

            def cookies(self):
                return [{"name": "aws-waf-token", "value": "abc123"}]

        class _FakeBrowser:
            def new_context(self, user_agent=None):
                return _FakeContext()

            def close(self):
                pass

        class _FakeChromium:
            def launch(self, headless=True, args=None):
                return _FakeBrowser()

        class _FakePlaywrightInstance:
            chromium = _FakeChromium()

        class _FakePlaywrightContextManager:
            def __enter__(self):
                return _FakePlaywrightInstance()

            def __exit__(self, *args):
                return False

        with patch.object(non_eu, "sync_playwright", lambda: _FakePlaywrightContextManager()):
            cookies = non_eu._solve_waf_challenge_via_browser(
                "https://www.legislation.govt.nz/act/public/2024/12"
            )

        self.assertEqual(cookies, {"aws-waf-token": "abc123"})

    def test_returns_none_when_no_cookies_result_from_the_page_load(self) -> None:
        class _FakePage:
            def goto(self, url, timeout=None, wait_until=None):
                pass

            def wait_for_load_state(self, state, timeout=None):
                pass

        class _FakeContext:
            def new_page(self):
                return _FakePage()

            def cookies(self):
                return []

        class _FakeBrowser:
            def new_context(self, user_agent=None):
                return _FakeContext()

            def close(self):
                pass

        class _FakeChromium:
            def launch(self, headless=True, args=None):
                return _FakeBrowser()

        class _FakePlaywrightInstance:
            chromium = _FakeChromium()

        class _FakePlaywrightContextManager:
            def __enter__(self):
                return _FakePlaywrightInstance()

            def __exit__(self, *args):
                return False

        with patch.object(non_eu, "sync_playwright", lambda: _FakePlaywrightContextManager()):
            cookies = non_eu._solve_waf_challenge_via_browser(
                "https://www.legislation.govt.nz/act/public/2024/12"
            )

        self.assertIsNone(cookies)

    def test_returns_none_on_any_exception_during_browser_automation(self) -> None:
        def _raising_sync_playwright():
            raise RuntimeError("browser launch failed")

        with patch.object(non_eu, "sync_playwright", _raising_sync_playwright):
            cookies = non_eu._solve_waf_challenge_via_browser(
                "https://www.legislation.govt.nz/act/public/2024/12"
            )

        self.assertIsNone(cookies)


class BrowserWafCookieCacheTests(unittest.TestCase):
    def _clear_cache(self) -> None:
        if hasattr(non_eu._thread_local, "browser_waf_cookies"):
            delattr(non_eu._thread_local, "browser_waf_cookies")

    def test_caches_the_result_per_host_within_a_thread(self) -> None:
        self._clear_cache()
        try:
            with patch.object(
                non_eu, "_solve_waf_challenge_via_browser", return_value={"aws-waf-token": "solved"}
            ) as mock_solve:
                first = non_eu._get_thread_browser_waf_cookies(
                    "https://www.legislation.govt.nz/act/public/2024/12"
                )
                second = non_eu._get_thread_browser_waf_cookies(
                    "https://www.legislation.govt.nz/act/public/2024/13"
                )

            mock_solve.assert_called_once()
            self.assertEqual(first, {"aws-waf-token": "solved"})
            self.assertEqual(second, {"aws-waf-token": "solved"})
        finally:
            self._clear_cache()

    def test_caches_a_failed_solve_too_rather_than_retrying(self) -> None:
        self._clear_cache()
        try:
            with patch.object(non_eu, "_solve_waf_challenge_via_browser", return_value=None) as mock_solve:
                first = non_eu._get_thread_browser_waf_cookies(
                    "https://www.legislation.govt.nz/act/public/2024/12"
                )
                second = non_eu._get_thread_browser_waf_cookies(
                    "https://www.legislation.govt.nz/act/public/2024/13"
                )

            mock_solve.assert_called_once()
            self.assertIsNone(first)
            self.assertIsNone(second)
        finally:
            self._clear_cache()

    def test_does_not_share_the_cache_across_different_hosts(self) -> None:
        self._clear_cache()
        try:
            with patch.object(
                non_eu, "_solve_waf_challenge_via_browser", return_value={"aws-waf-token": "solved"}
            ) as mock_solve:
                non_eu._get_thread_browser_waf_cookies("https://www.legislation.govt.nz/act/public/2024/12")
                non_eu._get_thread_browser_waf_cookies("https://www.legislation.gov.uk/ukpga/2024/1")

            self.assertEqual(mock_solve.call_count, 2)
        finally:
            self._clear_cache()


class PlaywrightDiagnosticPrintTests(unittest.TestCase):
    # Mirrors AddFullTextsParallelCurlCffiDiagnosticTests above for the new
    # Playwright availability print.
    def test_prints_available_when_playwright_is_importable(self) -> None:
        stdout = StringIO()

        with (
            patch.object(non_eu, "sync_playwright", lambda: None),
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
            "[FULLTEXT] Playwright headless-browser WAF-challenge solver: available",
            stdout.getvalue(),
        )

    def test_prints_not_available_when_playwright_is_missing(self) -> None:
        stdout = StringIO()

        with (
            patch.object(non_eu, "sync_playwright", None),
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
            "[FULLTEXT] Playwright headless-browser WAF-challenge solver: NOT available",
            stdout.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
