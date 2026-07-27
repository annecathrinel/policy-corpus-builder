from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu


AUS_SEARCH_HTML = """
<html>
  <body>
    <a href="/F2023L01234/asmade">Biodiversity Conservation Rule 2023</a>
    <a href="/C2021A00001/latest">Environment Protection Act 2021</a>
    <a href="/F2023L01234/asmade/downloads">Downloads</a>
  </body>
</html>
"""


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeRobots:
    def allowed(self, url: str) -> bool:
        return True


class _FakeSession:
    def __init__(self, responses: dict[str, _FakeResponse]):
        self._responses = responses

    def get(self, url: str, **kwargs) -> _FakeResponse:
        try:
            return self._responses[url]
        except KeyError as exc:
            raise AssertionError(f"unexpected URL fetched: {url}") from exc


class NonEUAustraliaTests(unittest.TestCase):
    def test_build_aus_search_url_matches_current_live_route_shape(self) -> None:
        self.assertEqual(
            non_eu.build_aus_search_url("biodiversity"),
            "https://www.legislation.gov.au/search/text(%22biodiversity%22,nameAndText,contains)/pointintime(Latest)",
        )
        self.assertEqual(
            non_eu.build_aus_search_url("soil biodiversity"),
            "https://www.legislation.gov.au/search/text(%22soil%20biodiversity%22,nameAndText,contains)/pointintime(Latest)",
        )

    def test_fetch_aus_documents_extracts_results_from_current_search_page(self) -> None:
        # fetch_aus_documents' search request now goes through
        # _get_with_waf_retry (see the module-level tests below and in
        # test_non_eu_waf_retry.py for coverage of that function's own
        # throttle/impersonation/retry behavior) rather than calling
        # safe_get directly, so these tests patch _get_with_waf_retry
        # itself to isolate fetch_aus_documents' own term-loop/parsing/
        # diagnostic-printing logic.
        with patch.object(non_eu, "_get_with_waf_retry", return_value=_FakeResponse(200, AUS_SEARCH_HTML)):
            df = non_eu.fetch_aus_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertEqual(
            df["url"].tolist(),
            [
                "https://www.legislation.gov.au/F2023L01234/asmade/text",
                "https://www.legislation.gov.au/C2021A00001/latest/text",
            ],
        )
        self.assertEqual(
            df["title"].tolist(),
            [
                "Biodiversity Conservation Rule 2023",
                "Environment Protection Act 2021",
            ],
        )

    def test_fetch_aus_documents_prints_progress_diagnostics_by_default(self) -> None:
        stdout = StringIO()
        with (
            patch.object(non_eu, "_get_with_waf_retry", return_value=_FakeResponse(200, AUS_SEARCH_HTML)),
            redirect_stdout(stdout),
        ):
            non_eu.fetch_aus_documents(["biodiversity"], max_per_term=10)

        output = stdout.getvalue()
        self.assertIn("[AUS] term='biodiversity'", output)
        self.assertIn("candidates=2", output)
        self.assertIn("DONE -> kept=2", output)
        self.assertIn("[AUS] total rows kept: 2", output)

    def test_fetch_aus_documents_verbose_false_suppresses_output(self) -> None:
        stdout = StringIO()
        with (
            patch.object(non_eu, "_get_with_waf_retry", return_value=_FakeResponse(200, AUS_SEARCH_HTML)),
            redirect_stdout(stdout),
        ):
            non_eu.fetch_aus_documents(["biodiversity"], max_per_term=10, verbose=False)

        self.assertEqual(stdout.getvalue(), "")

    def test_fetch_aus_documents_logs_non_200_status_and_continues_to_next_term(self) -> None:
        def fake_get_with_waf_retry(session, url: str, **kwargs) -> _FakeResponse:
            if "soil" in url:
                return _FakeResponse(503, "")
            return _FakeResponse(200, AUS_SEARCH_HTML)

        stdout = StringIO()
        with (
            patch.object(non_eu, "_get_with_waf_retry", side_effect=fake_get_with_waf_retry),
            redirect_stdout(stdout),
        ):
            df = non_eu.fetch_aus_documents(["soil biodiversity", "biodiversity"], max_per_term=10)

        output = stdout.getvalue()
        self.assertIn("term='soil biodiversity' ERROR -> HTTP 503", output)
        self.assertEqual(len(df), 2)

    def test_fetch_aus_documents_logs_waf_block_and_continues_to_next_term(self) -> None:
        # Regression test: a 2026-07-27 AUS smoke test got a clean HTTP 200
        # for its first ~12 search terms, then HTTP 403 for every single
        # term after that, permanently, for the rest of the run. This
        # covers fetch_aus_documents' own handling once _get_with_waf_retry
        # (already retried internally and still) returns a 403: it should
        # log it distinctly as waf_block rather than a generic HTTP error,
        # and keep going to the next term rather than aborting the run.
        def fake_get_with_waf_retry(session, url: str, **kwargs) -> _FakeResponse:
            if "biodiversity%20strategy" in url:
                return _FakeResponse(403, "")
            return _FakeResponse(200, AUS_SEARCH_HTML)

        stdout = StringIO()
        with (
            patch.object(non_eu, "_get_with_waf_retry", side_effect=fake_get_with_waf_retry),
            redirect_stdout(stdout),
        ):
            df = non_eu.fetch_aus_documents(
                ["biodiversity strategy", "biodiversity"], max_per_term=10
            )

        output = stdout.getvalue()
        self.assertIn("term='biodiversity strategy' ERROR -> waf_block (HTTP 403)", output)
        self.assertEqual(len(df), 2)

    def test_fetch_aus_documents_recovers_from_a_persistent_block_via_the_real_waf_retry_path(self) -> None:
        # End-to-end version of the regression above: exercises the real
        # _get_with_waf_retry (throttle + retry-with-backoff), not a mock
        # of it, against a fake session standing in for
        # www.legislation.gov.au - confirming fetch_aus_documents composes
        # correctly with the shared WAF-retry helper the same way the
        # UK/NZ/generic full-text paths already do. Impersonation is
        # disabled and time.sleep is patched out so this stays hermetic
        # and fast (curl_cffi is installed in this environment, and the
        # host is WAF-prone, so without disabling impersonation this would
        # otherwise attempt a real network call).
        term_1_url = non_eu.build_aus_search_url("biodiversity strategy")
        term_2_url = non_eu.build_aus_search_url("biodiversity")

        class _AlwaysBlockedSession:
            def __init__(self):
                self.calls: list[str] = []

            def get(self, url, **kwargs):
                self.calls.append(url)
                if url == term_1_url:
                    return _FakeResponse(403, "")
                return _FakeResponse(200, AUS_SEARCH_HTML)

        session = _AlwaysBlockedSession()
        stdout = StringIO()
        with (
            patch.object(non_eu, "_get_thread_impersonated_session", return_value=None),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            df = non_eu.fetch_aus_documents(
                ["biodiversity strategy", "biodiversity"],
                max_per_term=10,
                session=session,
            )

        # 1 initial attempt + 2 retries (_get_with_waf_retry's default
        # max_retries=2) for the always-403 term, then 1 for the term that
        # succeeds immediately.
        self.assertEqual(session.calls.count(term_1_url), 3)
        self.assertIn("term='biodiversity strategy' ERROR -> waf_block (HTTP 403)", stdout.getvalue())
        self.assertEqual(len(df), 2)

    def test_extract_aus_embedded_text_assets_prefers_document_1_html(self) -> None:
        wrapper_url = "https://www.legislation.gov.au/C2004A00485/latest/text"
        html = """
        <html><body>
          <a href="/C2004A00485/2026-03-28/2026-03-28/text/original/epub/OEBPS/document_2/document_2.html#toc">Volume 2</a>
          <a href="/C2004A00485/2026-03-28/2026-03-28/text/original/epub/OEBPS/document_1/document_1.html#toc">Volume 1</a>
        </body></html>
        """

        self.assertEqual(
            non_eu._extract_aus_embedded_text_assets(wrapper_url, html),
            [
                "https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/original/epub/OEBPS/document_1/document_1.html",
                "https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/original/epub/OEBPS/document_2/document_2.html",
            ],
        )

    def test_enrich_australia_text_page_prefers_embedded_document_asset(self) -> None:
        wrapper_url = "https://www.legislation.gov.au/C2004A00485/latest/text"
        asset_url = "https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/original/epub/OEBPS/document_1/document_1.html"
        session = _FakeSession(
            {
                wrapper_url: _FakeResponse(
                    200,
                    """
                    <html><body>
                      <a href="/C2004A00485/2026-03-28/2026-03-28/text/original/epub/OEBPS/document_1/document_1.html">Volume 1</a>
                      Wrapper navigation text only.
                    </body></html>
                    """,
                ),
                asset_url: _FakeResponse(
                    200,
                    "<html><body>Actual Australia legislation body text.</body></html>",
                ),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            # aus_text_page mode now routes through _get_with_waf_retry
            # (www.legislation.gov.au is a WAF-prone host), which prefers a
            # curl_cffi-impersonated session over the injected fake one
            # when curl_cffi is importable - it is, in this environment.
            # Disabling impersonation keeps these tests exercising the
            # injected _FakeSession instead of attempting a real request.
            patch.object(non_eu, "_get_thread_impersonated_session", return_value=None),
            # www.legislation.gov.au is throttled (_WAF_PRONE_HOST_MIN_INTERVAL_S)
            # and _host_last_request_monotonic is a shared module-level
            # dict, so back-to-back tests hitting this host could
            # otherwise incur a real time.sleep of up to that interval.
            patch.object(non_eu.time, "sleep"),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "AUS",
                    "jurisdiction": "Australia",
                    "url": wrapper_url,
                    "text_url": wrapper_url,
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(enriched["full_text_url"], asset_url)
        self.assertEqual(enriched["full_text_format"], "html")
        self.assertIn("Actual Australia legislation body text.", enriched["full_text"])

    def test_enrich_australia_text_page_combines_multiple_embedded_documents(self) -> None:
        wrapper_url = "https://www.legislation.gov.au/C2004A00485/latest/text"
        asset_url_1 = "https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/1/epub/OEBPS/document_1/document_1.html"
        asset_url_2 = "https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/1/epub/OEBPS/document_2/document_2.html"
        session = _FakeSession(
            {
                wrapper_url: _FakeResponse(
                    200,
                    f"""
                    <html><body>
                      <a href="{asset_url_2}#toc">Volume 2</a>
                      <a href="{asset_url_1}#toc">Volume 1</a>
                    </body></html>
                    """,
                ),
                asset_url_1: _FakeResponse(200, "<html><body>Volume one text.</body></html>"),
                asset_url_2: _FakeResponse(200, "<html><body>Volume two text.</body></html>"),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            # aus_text_page mode now routes through _get_with_waf_retry
            # (www.legislation.gov.au is a WAF-prone host), which prefers a
            # curl_cffi-impersonated session over the injected fake one
            # when curl_cffi is importable - it is, in this environment.
            # Disabling impersonation keeps these tests exercising the
            # injected _FakeSession instead of attempting a real request.
            patch.object(non_eu, "_get_thread_impersonated_session", return_value=None),
            # www.legislation.gov.au is throttled (_WAF_PRONE_HOST_MIN_INTERVAL_S)
            # and _host_last_request_monotonic is a shared module-level
            # dict, so back-to-back tests hitting this host could
            # otherwise incur a real time.sleep of up to that interval.
            patch.object(non_eu.time, "sleep"),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "AUS",
                    "jurisdiction": "Australia",
                    "url": wrapper_url,
                    "text_url": wrapper_url,
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(
            enriched["full_text_url"],
            '["https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/1/epub/OEBPS/document_1/document_1.html", "https://www.legislation.gov.au/C2004A00485/2026-03-28/2026-03-28/text/1/epub/OEBPS/document_2/document_2.html"]',
        )
        self.assertIn("Volume one text.", enriched["full_text"])
        self.assertIn("Volume two text.", enriched["full_text"])

    def test_enrich_australia_text_page_falls_back_to_wrapper_when_no_embedded_asset_exists(self) -> None:
        wrapper_url = "https://www.legislation.gov.au/C2004A00485/latest/text"
        session = _FakeSession(
            {
                wrapper_url: _FakeResponse(
                    200,
                    "<html><body>Wrapper legislation text fallback.</body></html>",
                ),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            # aus_text_page mode now routes through _get_with_waf_retry
            # (www.legislation.gov.au is a WAF-prone host), which prefers a
            # curl_cffi-impersonated session over the injected fake one
            # when curl_cffi is importable - it is, in this environment.
            # Disabling impersonation keeps these tests exercising the
            # injected _FakeSession instead of attempting a real request.
            patch.object(non_eu, "_get_thread_impersonated_session", return_value=None),
            # www.legislation.gov.au is throttled (_WAF_PRONE_HOST_MIN_INTERVAL_S)
            # and _host_last_request_monotonic is a shared module-level
            # dict, so back-to-back tests hitting this host could
            # otherwise incur a real time.sleep of up to that interval.
            patch.object(non_eu.time, "sleep"),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "AUS",
                    "jurisdiction": "Australia",
                    "url": wrapper_url,
                    "text_url": wrapper_url,
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(enriched["full_text_url"], wrapper_url)
        self.assertIn("Wrapper legislation text fallback.", enriched["full_text"])


if __name__ == "__main__":
    unittest.main()
