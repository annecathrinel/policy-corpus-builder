from __future__ import annotations

import unittest
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
        )

        self.assertEqual(session.calls, 1)
        self.assertEqual(response.text, "real document text")


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


if __name__ == "__main__":
    unittest.main()
