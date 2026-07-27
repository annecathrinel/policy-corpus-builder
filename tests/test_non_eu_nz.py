from __future__ import annotations

import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu


NZ_API_PAYLOAD = {
    "results": [
        {
            "work_id": "act_public_2024_12",
            "legislation_type": "act",
            "latest_matching_version": {
                "title": "Biodiversity Restoration Act 2024",
                "version_id": "act_public_2024_12_en_latest",
                "is_latest_version": True,
                "formats": [
                    {"type": "xml", "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml"},
                    {"type": "pdf", "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf"},
                    {"type": "html", "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/"},
                ],
            },
        }
    ],
    "page": 1,
    "per_page": 20,
    "total": 1,
}


NZ_XML = """
<document>
  <title>Biodiversity Restoration Act 2024</title>
  <body>
    <section>Restore biodiversity values across public lands.</section>
  </body>
</document>
"""


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", *, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.content = text.encode("utf-8")

    def json(self):
        return json.loads(self.text)

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


class NonEUNewZealandTests(unittest.TestCase):
    def test_nz_search_url_uses_official_api_contract(self) -> None:
        self.assertEqual(
            non_eu.nz_search_url(non_eu.NZ_API_BASE, "biodiversity", page=2),
            "https://api.legislation.govt.nz/v0/works?search_term=biodiversity&search_field=content&page=2&per_page=20",
        )

    def test_nz_search_url_quotes_multi_word_terms_for_an_exact_phrase_match(self) -> None:
        # Regression test: legislation.govt.nz's search (both the website
        # and this API) treats unquoted multi-word input as a fuzzy/OR
        # match over the individual words, not a phrase - "marine
        # biodiversity" without quotes matched "marine" OR "biodiversity"
        # rather than the actual phrase. `"..."` is the documented
        # operator for an exact phrase match, and UK/US already quote
        # their multi-word terms the same way.
        url = non_eu.nz_search_url(non_eu.NZ_API_BASE, "marine biodiversity", page=1)

        self.assertIn("search_term=%22marine%20biodiversity%22", url)

    def test_nz_search_url_does_not_quote_a_single_word_term(self) -> None:
        url = non_eu.nz_search_url(non_eu.NZ_API_BASE, "biodiversity", page=1)

        self.assertIn("search_term=biodiversity", url)
        self.assertNotIn("%22", url)

    def test_extract_nz_api_rows_prefers_version_formats(self) -> None:
        rows = non_eu._extract_nz_api_rows("biodiversity", NZ_API_PAYLOAD, max_per_term=10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["url"],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
        )
        self.assertEqual(
            rows[0]["xml_url"],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
        )
        self.assertEqual(
            rows[0]["pdf_url"],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
        )
        self.assertEqual(rows[0]["title"], "Biodiversity Restoration Act 2024")

    def test_fetch_nz_documents_uses_api_discovery(self) -> None:
        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            self.assertEqual(
                kwargs["headers"]["X-Api-Key"],
                "nz-test-key",
            )
            return _FakeResponse(200, json.dumps(NZ_API_PAYLOAD))

        with patch.object(non_eu, "safe_get", side_effect=fake_safe_get):
            df = non_eu.fetch_nz_documents(
                ["biodiversity"],
                api_key="nz-test-key",
                max_per_term=5,
                verbose=False,
            )

        self.assertEqual(len(df), 1)
        self.assertEqual(df["source"].iloc[0], "NZ")
        self.assertEqual(df["jurisdiction"].iloc[0], "New Zealand")
        self.assertEqual(
            df["url"].iloc[0],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
        )

    def test_fetch_nz_documents_paginates_past_twenty_pages_when_more_results_remain(self) -> None:
        # Regression test: fetch_nz_documents used to stop after a fixed
        # max_pages ceiling (originally 5, later bumped to 20) regardless
        # of max_per_term, unlike UK/AUS/US which paginate until
        # max_per_term is reached or the API genuinely runs out of
        # results. At the API's fixed per_page=20, a max_pages=20 ceiling
        # silently capped every NZ term at 400 documents, always short of
        # the default max_per_term=500 budget every other jurisdiction
        # gets to use in full. This simulates a term with 25 one-result
        # pages (25 total results) and a max_per_term of 25, which used to
        # get cut off at page 20/result 20.
        total_results = 25
        requested_pages: list[int] = []

        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            page = int(url.split("page=")[1].split("&")[0])
            requested_pages.append(page)
            payload = {
                "results": [
                    {
                        "work_id": f"act_public_2024_{page}",
                        "latest_matching_version": {
                            "title": f"Act {page}",
                            "version_id": f"act_public_2024_{page}_en_latest",
                            "formats": [
                                {
                                    "type": "xml",
                                    "url": f"https://www.legislation.govt.nz/act/public/2024/{page}/en/latest.xml",
                                },
                            ],
                        },
                    }
                ],
                "page": page,
                "per_page": 1,
                "total": total_results,
            }
            return _FakeResponse(200, json.dumps(payload))

        with patch.object(non_eu, "safe_get", side_effect=fake_safe_get):
            df = non_eu.fetch_nz_documents(
                ["biodiversity"],
                api_key="nz-test-key",
                max_per_term=total_results,
                verbose=False,
            )

        self.assertEqual(len(df), total_results)
        self.assertEqual(max(requested_pages), total_results)
        self.assertGreater(max(requested_pages), 20)

    def test_fetch_nz_documents_verbose_api_success_does_not_crash(self) -> None:
        # Regression test: the per-page progress print on a successful API
        # page referenced an undefined "candidates" variable (should have
        # been "page_rows"), raising NameError on every real API success.
        # This was invisible in tests and in production because NZ never
        # had a working API key before, and the only other API-mode test
        # passes verbose=False, which skips the buggy print entirely.
        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            return _FakeResponse(200, json.dumps(NZ_API_PAYLOAD))

        stdout = StringIO()
        with patch.object(non_eu, "safe_get", side_effect=fake_safe_get):
            with redirect_stdout(stdout):
                df = non_eu.fetch_nz_documents(
                    ["biodiversity"],
                    api_key="nz-test-key",
                    max_per_term=5,
                    # verbose defaults to True - this is the path that used
                    # to crash.
                )

        self.assertEqual(len(df), 1)
        self.assertIn("candidates=1", stdout.getvalue())

    def test_fetch_nz_documents_requires_an_api_key(self) -> None:
        # Regression test: NZ retrieval used to fall back to an
        # unauthenticated scrape of the public legislation.govt.nz website
        # when no API key was configured, but that fallback doesn't work
        # (the same WAF that blocks full-text downloads blocks it too) and
        # isn't worth maintaining. NZ now requires a key unconditionally,
        # mirroring fetch_us_documents's RuntimeError for a missing
        # REGULATIONS_GOV_API_KEY.
        original_primary = non_eu.os.environ.pop("NZ_LEGISLATION_API_KEY", None)
        original_alias = non_eu.os.environ.pop("NZ_API_KEY", None)
        try:
            with self.assertRaisesRegex(RuntimeError, "NZ_LEGISLATION_API_KEY"):
                non_eu.fetch_nz_documents(["biodiversity"], api_key=None, max_per_term=10, verbose=False)
        finally:
            if original_primary is not None:
                non_eu.os.environ["NZ_LEGISLATION_API_KEY"] = original_primary
            if original_alias is not None:
                non_eu.os.environ["NZ_API_KEY"] = original_alias

    def test_get_url_candidates_for_nz_prefers_xml_then_pdf_then_html(self) -> None:
        candidates = non_eu.get_url_candidates(
            {
                "source": "NZ",
                "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
                "xml_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                "pdf_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
                "text_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
            },
            "NZ",
            None,
        )

        self.assertEqual(
            candidates,
            [
                ("https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml", "nz_xml"),
                ("https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf", "pdf"),
                ("https://www.legislation.govt.nz/act/public/2024/12/en/latest/", "html"),
            ],
        )

    def test_get_url_candidates_for_nz_adds_a_derived_pdf_when_the_api_gave_none(self) -> None:
        # Regression test: inspecting individual NZ full-text failures (not
        # just the aggregate waf_challenge count) found several where the
        # html/doc_url candidate was blocked, but the same document had a
        # working PDF rendition at the same path with the extension
        # swapped/added - confirmed by opening that URL directly. The API's
        # own "formats" list doesn't always include a pdf entry, so this is
        # tried as a best-effort extra candidate rather than relying on the
        # API to have listed one.
        candidates = non_eu.get_url_candidates(
            {
                "source": "NZ",
                "url": "https://www.legislation.govt.nz/act/public/2024/12/en/whole.html",
                "text_url": "https://www.legislation.govt.nz/act/public/2024/12/en/whole.html",
            },
            "NZ",
            None,
        )

        self.assertEqual(
            candidates,
            [
                ("https://www.legislation.govt.nz/act/public/2024/12/en/whole.pdf", "pdf"),
                ("https://www.legislation.govt.nz/act/public/2024/12/en/whole.html", "html"),
            ],
        )

    def test_get_url_candidates_for_nz_does_not_duplicate_an_existing_pdf_url(self) -> None:
        candidates = non_eu.get_url_candidates(
            {
                "source": "NZ",
                "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
                "pdf_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
                "text_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
            },
            "NZ",
            None,
        )

        pdf_candidates = [c for c in candidates if c[1] == "pdf"]
        self.assertEqual(len(pdf_candidates), 1)

    def test_derive_nz_pdf_url_swaps_html_extension(self) -> None:
        self.assertEqual(
            non_eu._derive_nz_pdf_url("https://www.legislation.govt.nz/act/public/2024/12/en/whole.html"),
            "https://www.legislation.govt.nz/act/public/2024/12/en/whole.pdf",
        )

    def test_derive_nz_pdf_url_appends_when_there_is_no_extension(self) -> None:
        self.assertEqual(
            non_eu._derive_nz_pdf_url("https://www.legislation.govt.nz/act/public/2024/12/en/latest"),
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
        )

    def test_derive_nz_pdf_url_is_a_noop_for_an_existing_pdf(self) -> None:
        self.assertEqual(
            non_eu._derive_nz_pdf_url("https://www.legislation.govt.nz/act/public/2024/12/en/whole.pdf"),
            "",
        )

    def test_derive_nz_pdf_url_does_not_guess_at_an_unrecognized_extension(self) -> None:
        self.assertEqual(
            non_eu._derive_nz_pdf_url("https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml"),
            "",
        )

    def test_derive_nz_pdf_url_handles_an_empty_input(self) -> None:
        self.assertEqual(non_eu._derive_nz_pdf_url(""), "")

    def test_enrich_nz_record_uses_xml_when_available(self) -> None:
        xml_url = "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml"
        session = _FakeSession({xml_url: _FakeResponse(200, NZ_XML)})

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            # Keeps this on the injected fake session rather than a real
            # curl_cffi impersonated session, which _get_with_waf_retry now
            # prefers for legislation.govt.nz - see test_non_eu_waf_retry.py
            # for dedicated coverage of that routing.
            patch.object(non_eu, "_get_thread_impersonated_session", return_value=None),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "NZ",
                    "jurisdiction": "New Zealand",
                    "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
                    "xml_url": xml_url,
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(enriched["full_text_format"], "nz_xml")
        self.assertEqual(enriched["full_text_url"], xml_url)
        self.assertIn("Restore biodiversity values", enriched["full_text"])

    def test_enrich_nz_record_sends_a_browser_user_agent_not_the_tool_default(self) -> None:
        # Regression test: a real NZ smoke test (2026-07-27) found every
        # single full-text request to www.legislation.govt.nz getting a WAF
        # challenge, regardless of request pacing/retries - while requests
        # to other *.govt.nz hosts always succeeded. NZ requests were
        # sending the tool's own self-identifying default User-Agent
        # ("policy-corpus-builder/0.1"); UK's legislation.gov.uk already
        # gets a real browser UA (UK_BROWSER_UA) for exactly this reason.
        # NZ full-text requests must get the same treatment.
        xml_url = "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml"
        captured_headers: list[dict[str, str]] = []

        class _HeaderCapturingSession:
            def get(self, url: str, **kwargs) -> _FakeResponse:
                captured_headers.append(kwargs.get("headers", {}))
                return _FakeResponse(200, NZ_XML)

        with (
            patch.object(non_eu, "_get_thread_session", return_value=_HeaderCapturingSession()),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            patch.object(non_eu, "_get_thread_impersonated_session", return_value=None),
        ):
            non_eu.enrich_one_record_fulltext(
                {
                    "source": "NZ",
                    "jurisdiction": "New Zealand",
                    "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
                    "xml_url": xml_url,
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(len(captured_headers), 1)
        self.assertNotIn("policy-corpus-builder", captured_headers[0]["User-Agent"])
        self.assertIn("Mozilla/5.0", captured_headers[0]["User-Agent"])
        self.assertEqual(captured_headers[0]["Accept-Language"], "en-NZ,en;q=0.9")


if __name__ == "__main__":
    unittest.main()
