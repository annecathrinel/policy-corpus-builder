from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu
from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter
from policy_corpus_builder.schemas import SourceConfig


# A representative publications.gc.ca search-results page: the search page's
# own furniture (a link back to itself, the site's home page, its browse
# index, and its French-language equivalent) mixed in with real result rows
# and a direct .pdf link, matching the shape confirmed working in an earlier
# version of this module before CA search briefly (and incorrectly) moved
# to laws-lois.justice.gc.ca.
CANADA_PUBLICATIONS_SEARCH_HTML = """
<html>
  <body>
    <a href="/site/eng/search/search.html?sLF=eng&text=biodiversity&cnst=&adof=on">New search</a>
    <a href="/site/eng/home.html">Home</a>
    <a href="/site/eng/browse/index.html">Browse all publications</a>
    <a href="/site/eng/9.876543/publication.html">Biodiversity Plan 2024</a>
    <a href="/collections/collection_2024/eccc/En1-45-2024-eng.pdf">Species at Risk Report (PDF)</a>
    <a href="/site/fra/recherche/recherche.html">Français</a>
  </body>
</html>
"""

# Regression fixture: a 2026-07-27 live run found the site's own
# language-switcher link back to the French search page
# (/site/fra/recherche/recherche.html) present as boilerplate on every
# single search-results page, real hits or none - so a genuinely-empty
# search was silently coming back as 1 fake "result" instead of 0 on every
# term. This page has zero real results, only that one furniture link.
CANADA_PUBLICATIONS_ZERO_HITS_HTML = """
<html>
  <body>
    <a href="/site/eng/search/search.html?sLF=eng&text=nature%20restoration&cnst=&adof=on">New search</a>
    <a href="/site/fra/recherche/recherche.html">Français</a>
  </body>
</html>
"""


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")
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


class NonEUCanadaTests(unittest.TestCase):
    def test_build_canada_publications_search_url_matches_current_live_route_shape(self) -> None:
        self.assertEqual(
            non_eu.build_canada_publications_search_url("biodiversity"),
            "https://www.publications.gc.ca/site/eng/search/search.html?sLF=eng&text=%22biodiversity%22&cnst=&adof=on",
        )
        self.assertEqual(
            non_eu.build_canada_publications_search_url("soil biodiversity"),
            "https://www.publications.gc.ca/site/eng/search/search.html?sLF=eng&text=%22soil%20biodiversity%22&cnst=&adof=on",
        )

    def test_extract_canada_publications_result_links_filters_search_furniture(self) -> None:
        results = non_eu._extract_canada_publications_result_links(CANADA_PUBLICATIONS_SEARCH_HTML)

        # Only the two real result rows - the self-referential search link,
        # the home page, the browse index, and the French language-switcher
        # link are all excluded.
        self.assertEqual(
            results,
            [
                (
                    "https://www.publications.gc.ca/site/eng/9.876543/publication.html",
                    "Biodiversity Plan 2024",
                ),
                (
                    "https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf",
                    "Species at Risk Report (PDF)",
                ),
            ],
        )

    def test_extract_canada_publications_result_links_excludes_french_search_self_link(self) -> None:
        # Regression test: a 2026-07-27 live run found every single search
        # term - including genuinely zero-hit terms like "nature
        # restoration" - returning exactly one "result": a link back to the
        # French version of the search page itself
        # (/site/fra/recherche/recherche.html), present as boilerplate on
        # every results page. A real zero-hit search must come back as 0
        # candidates, not 1.
        results = non_eu._extract_canada_publications_result_links(CANADA_PUBLICATIONS_ZERO_HITS_HTML)

        self.assertEqual(results, [])

    def test_fetch_canada_documents_extracts_results_from_search_page(self) -> None:
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)):
            df = non_eu.fetch_canada_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertTrue((df["jurisdiction"] == "Canada").all())
        self.assertTrue((df["source"] == "CA").all())
        self.assertEqual(
            df["doc_url"].tolist(),
            [
                "https://www.publications.gc.ca/site/eng/9.876543/publication.html",
                "https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf",
            ],
        )

    def test_fetch_canada_documents_reports_zero_kept_for_genuinely_empty_search(self) -> None:
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_ZERO_HITS_HTML)):
            df = non_eu.fetch_canada_documents(["nature restoration"], max_per_term=10)

        self.assertEqual(len(df), 0)

    def test_fetch_canada_documents_stops_when_max_per_term_reached(self) -> None:
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)):
            df = non_eu.fetch_canada_documents(["biodiversity"], max_per_term=1)

        self.assertEqual(len(df), 1)

    def test_fetch_canada_documents_prints_progress_diagnostics_by_default(self) -> None:
        stdout = StringIO()
        with (
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            non_eu.fetch_canada_documents(["biodiversity"], max_per_term=10)

        output = stdout.getvalue()
        self.assertIn("[CA] term='biodiversity'", output)
        self.assertIn("candidates=2", output)
        self.assertIn("DONE -> kept=2", output)
        self.assertIn("[CA] total rows kept: 2", output)

    def test_fetch_canada_documents_verbose_false_suppresses_output(self) -> None:
        stdout = StringIO()
        with (
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            non_eu.fetch_canada_documents(["biodiversity"], max_per_term=1, verbose=False)

        self.assertEqual(stdout.getvalue(), "")

    def test_fetch_canada_documents_logs_non_200_status_and_continues_to_next_term(self) -> None:
        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            if "soil" in url:
                return _FakeResponse(503, "")
            return _FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)

        stdout = StringIO()
        with (
            patch.object(non_eu, "safe_get", side_effect=fake_safe_get),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            df = non_eu.fetch_canada_documents(["soil biodiversity", "biodiversity"], max_per_term=10)

        output = stdout.getvalue()
        self.assertIn("term='soil biodiversity' ERROR -> HTTP 503", output)
        self.assertEqual(len(df), 2)

    def test_clean_canada_doc_id_extracts_regulation_key_from_title(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_doc_id({"title": "Fishery (General) Regulations SOR/2021-118", "url": ""}),
            "SOR_2021_118",
        )

    def test_clean_canada_doc_id_falls_back_to_url_path(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_doc_id(
                {"title": "Biodiversity Plan 2024", "url": "https://www.publications.gc.ca/site/eng/9.876543/publication.html"}
            ),
            "site_eng_9.876543_publication",
        )

    def test_clean_canada_title_removes_trailing_catalogue_identifier(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_title(
                "Soil biodiversity : what's most important? : A59-82/2021E-PDF"
            ),
            "Soil biodiversity : what's most important?",
        )
        self.assertEqual(
            non_eu.clean_canada_title(
                "Compendium of Canada's engagement in international environmental agreements and instruments. Intergovernmental Platform on Biodiversity and Ecosystem Services (IPBES). En4-381/4-5-2018E-PDF"
            ),
            "Compendium of Canada's engagement in international environmental agreements and instruments. Intergovernmental Platform on Biodiversity and Ecosystem Services (IPBES).",
        )

    def test_clean_canada_full_text_trims_boilerplate_and_common_encoding_noise(self) -> None:
        cleaned = non_eu.clean_canada_full_text(
            "Title - Government of Canada Publications - Canada.ca "
            "Passer au contenu principal "
            "Passer à « À propos de ce site » "
            "Language selection Français fr / Gouvernement du Canada "
            "Search Search Canada.ca Search Menu Main Menu "
            "Useful body text with biodiversitÃ© and authorâ€™s note. "
            "Page details Report a problem or mistake on this page "
            "About this site Government of Canada All contacts Departments and agencies"
        )

        self.assertIn("Useful body text", cleaned)
        self.assertIn("biodiversité", cleaned)
        self.assertIn("author's note", cleaned)
        self.assertNotIn("Passer au contenu principal", cleaned)
        self.assertNotIn("Government of Canada Publications - Canada.ca", cleaned)

    def test_should_skip_canada_url_flags_data_files(self) -> None:
        self.assertTrue(non_eu.should_skip_canada_url("https://www.publications.gc.ca/tbl/csv/example.csv"))
        self.assertTrue(non_eu.should_skip_canada_url("https://www.publications.gc.ca/download/example.zip"))
        self.assertFalse(non_eu.should_skip_canada_url("https://www.publications.gc.ca/collections/example-eng.pdf"))

    def test_get_url_candidates_for_canada_publication_and_pdf_urls(self) -> None:
        self.assertEqual(
            non_eu.get_url_candidates(
                {"url": "https://www.publications.gc.ca/site/eng/9.876543/publication.html"},
                "CA",
                None,
            ),
            [("https://www.publications.gc.ca/site/eng/9.876543/publication.html", "ca_publication")],
        )
        self.assertEqual(
            non_eu.get_url_candidates(
                {"url": "https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf"},
                "CA",
                None,
            ),
            [("https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf", "pdf")],
        )

    def test_enrich_canada_publication_falls_back_to_landing_page_when_no_asset_is_available(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.123456/publication.html"
        session = _FakeSession(
            {
                landing_url: _FakeResponse(
                    200,
                    """
                    <html><body>
                      Government of Canada Publications - Canada.ca
                      Useful landing page content for fallback.
                    </body></html>
                    """,
                ),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "CA",
                    "jurisdiction": "Canada",
                    "url": landing_url,
                },
                us_api_key=None,
                obey_robots=True,
            )

        self.assertIn("Useful landing page content for fallback.", enriched["full_text"])
        self.assertEqual(enriched["full_text_url"], landing_url)
        self.assertEqual(enriched["full_text_format"], "html")
        self.assertEqual(enriched["full_text_error"], "")

    def test_canada_row_to_result_preserves_working_output_shape(self) -> None:
        adapter = NonEUAdapter()
        source = SourceConfig(name="canada-publications", adapter="non-eu", settings={"countries": ["CA"]})
        row = {
            "country": "Canada",
            "date": "2024",
            "doc_id": "SOR_2024_12",
            "doc_uid": "SOR_2024_12",
            "full_text_clean": "Canadian policy text.",
            "full_text_error": "",
            "full_text_format": "html",
            "full_text_url": "https://www.publications.gc.ca/collections/example-eng.pdf",
            "has_text": "True",
            "jurisdiction": "Canada",
            "lang": "en",
            "matched_terms": "[\"biodiversity\"]",
            "retrieval_status": "ok",
            "source": "CA",
            "source_file": "",
            "text_len": "22",
            "title": "Biodiversity Plan 2024",
            "url": "https://www.publications.gc.ca/collections/example-eng.pdf",
            "year": "2024",
        }

        result = adapter._row_to_result(row, source=source, source_log=[{"source": "CA", "ok": True}])

        self.assertEqual(result.payload["document_id"], "canada-publications:SOR_2024_12")
        self.assertEqual(result.payload["jurisdiction"], "Canada")
        self.assertEqual(result.payload["full_text"], "Canadian policy text.")
        self.assertEqual(
            result.payload["raw_record"]["full_text_url"],
            "https://www.publications.gc.ca/collections/example-eng.pdf",
        )


if __name__ == "__main__":
    unittest.main()
