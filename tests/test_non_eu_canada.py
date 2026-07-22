from __future__ import annotations

import unittest
import json
from pathlib import Path
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu
from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter
from policy_corpus_builder.schemas import SourceConfig


CANADA_LAWS_SEARCH_HTML = """
<html>
  <body>
    <a href="/eng/acts/O-2.4/section-2.html">Oceans Act</a>
    <a href="/eng/regulations/SOR-96-118/section-35.html">Fishery (General) Regulations</a>
    <a href="/fra/lois/O-2.4/section-2.html">Loi sur les oceans</a>
    <a href="/eng/News/2024.html">News (non-legislation result)</a>
  </body>
</html>
"""


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        text: str = "",
        *,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")
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
    def test_extract_canada_laws_result_links_collapses_sections_to_root_act(self) -> None:
        results = non_eu._extract_canada_laws_result_links(CANADA_LAWS_SEARCH_HTML)

        self.assertIn(
            (
                "https://laws-lois.justice.gc.ca/eng/acts/O-2.4/section-2.html",
                "https://laws-lois.justice.gc.ca/eng/acts/O-2.4/",
                "Oceans Act",
            ),
            results,
        )
        self.assertIn(
            (
                "https://laws-lois.justice.gc.ca/eng/regulations/SOR-96-118/section-35.html",
                "https://laws-lois.justice.gc.ca/eng/regulations/SOR-96-118/",
                "Fishery (General) Regulations",
            ),
            results,
        )

    def test_fetch_canada_documents_justice_dep_paginates_and_dedupes_by_canonical_url(self) -> None:
        empty_page_html = "<html><body></body></html>"

        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            if "h1dd3nPag3Num=1" in url:
                return _FakeResponse(200, CANADA_LAWS_SEARCH_HTML)
            if "h1dd3nPag3Num=2" in url:
                return _FakeResponse(200, empty_page_html)
            raise AssertionError(f"unexpected URL: {url}")

        with (
            patch.object(non_eu, "safe_get", side_effect=fake_safe_get),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_canada_documents_justice_dep(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 4)
        self.assertTrue((df["jurisdiction"] == "Canada").all())
        self.assertTrue((df["source"] == "CA").all())
        self.assertIn(
            "https://laws-lois.justice.gc.ca/eng/acts/O-2.4/",
            df["doc_url"].tolist(),
        )

    def test_fetch_canada_documents_justice_dep_stops_when_max_per_term_reached(self) -> None:
        with (
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_LAWS_SEARCH_HTML)),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_canada_documents_justice_dep(["biodiversity"], max_per_term=1)

        self.assertEqual(len(df), 1)

    def test_clean_canada_laws_doc_id_extracts_act_and_regulation_keys(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_laws_doc_id("https://laws-lois.justice.gc.ca/eng/acts/O-2.4/"),
            "acts_o_2_4",
        )
        self.assertEqual(
            non_eu.clean_canada_laws_doc_id(
                "https://laws-lois.justice.gc.ca/eng/regulations/SOR-96-118/"
            ),
            "regulations_sor_96_118",
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

    def test_get_url_candidates_for_canada_laws_url_tries_fulltext_then_root(self) -> None:
        candidates = non_eu.get_url_candidates(
            {"url": "https://laws-lois.justice.gc.ca/eng/acts/O-2.4/section-2.html"},
            "CA",
            None,
        )

        self.assertEqual(
            candidates,
            [
                ("https://laws-lois.justice.gc.ca/eng/acts/O-2.4/FullText.html", "ca_laws_html"),
                ("https://laws-lois.justice.gc.ca/eng/acts/O-2.4/", "ca_laws_root"),
            ],
        )

    def test_enrich_canada_laws_record_uses_fulltext_html_page(self) -> None:
        fulltext_url = "https://laws-lois.justice.gc.ca/eng/acts/O-2.4/FullText.html"
        session = _FakeSession(
            {
                fulltext_url: _FakeResponse(
                    200,
                    "<html><body>Oceans Act full text content about conservation.</body></html>",
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
                    "url": "https://laws-lois.justice.gc.ca/eng/acts/O-2.4/section-2.html",
                },
                us_api_key=None,
                obey_robots=True,
            )

        self.assertIn("Oceans Act full text content", enriched["full_text"])
        self.assertEqual(enriched["full_text_url"], fulltext_url)
        self.assertEqual(enriched["full_text_format"], "html")
        self.assertEqual(enriched["full_text_error"], "")

    def test_enrich_canada_laws_record_falls_back_to_root_page_when_fulltext_is_missing(self) -> None:
        fulltext_url = "https://laws-lois.justice.gc.ca/eng/regulations/SOR-96-118/FullText.html"
        root_url = "https://laws-lois.justice.gc.ca/eng/regulations/SOR-96-118/"
        session = _FakeSession(
            {
                fulltext_url: _FakeResponse(404, ""),
                root_url: _FakeResponse(
                    200,
                    "<html><body>Fishery (General) Regulations root page text.</body></html>",
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
                    "url": "https://laws-lois.justice.gc.ca/eng/regulations/SOR-96-118/section-35.html",
                },
                us_api_key=None,
                obey_robots=True,
            )

        self.assertIn("Fishery (General) Regulations root page text.", enriched["full_text"])
        self.assertEqual(enriched["full_text_url"], root_url)
        self.assertEqual(enriched["full_text_format"], "html")
        self.assertEqual(enriched["full_text_error"], "")

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
