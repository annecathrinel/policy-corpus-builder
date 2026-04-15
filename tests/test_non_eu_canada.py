from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu
from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter
from policy_corpus_builder.schemas import SourceConfig


CANADA_SEARCH_HTML = """
<html>
  <body>
    <a href="/collections/collection_2024/environment-act-eng.pdf">Environment Act 2024</a>
    <a href="/collections/collection_2023/biodiversity-plan-eng.pdf">Biodiversity Plan 2023</a>
    <a href="/site/eng/search/search.html?ast=biodiversity">Search</a>
    <a href="/collections/collection_2024/table.csv">CSV table</a>
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

    def test_fetch_canada_documents_extracts_publications_hits(self) -> None:
        with (
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_SEARCH_HTML)),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_canada_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertEqual(
            sorted(df["url"].tolist()),
            [
                "https://www.publications.gc.ca/collections/collection_2023/biodiversity-plan-eng.pdf",
                "https://www.publications.gc.ca/collections/collection_2024/environment-act-eng.pdf",
            ],
        )
        self.assertTrue((df["jurisdiction"] == "Canada").all())
        self.assertTrue((df["source"] == "CA").all())

    def test_should_skip_canada_url_flags_data_files(self) -> None:
        self.assertTrue(non_eu.should_skip_canada_url("https://www.publications.gc.ca/tbl/csv/example.csv"))
        self.assertTrue(non_eu.should_skip_canada_url("https://www.publications.gc.ca/download/example.zip"))
        self.assertFalse(non_eu.should_skip_canada_url("https://www.publications.gc.ca/collections/example-eng.pdf"))

    def test_extract_canada_asset_links_prefers_real_assets(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.123456/publication.html"
        html = """
        <html><body>
          <a href="/site/eng/9.123456/publication.html">Landing page</a>
          <a href="/collections/collection_2024/example-report-eng.pdf">PDF</a>
          <a href="https://www.canada.ca/en/environment/example-report.html">HTML</a>
          <a href="https://example.com/offsite.pdf">Offsite</a>
        </body></html>
        """

        self.assertEqual(
            non_eu._extract_canada_asset_links(landing_url, html),
            [
                ("https://www.publications.gc.ca/collections/collection_2024/example-report-eng.pdf", "pdf"),
                ("https://www.canada.ca/en/environment/example-report.html", "html"),
            ],
        )

    def test_enrich_canada_publication_prefers_pdf_asset_over_landing_page_text(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.123456/publication.html"
        pdf_url = "https://www.publications.gc.ca/collections/collection_2024/example-report-eng.pdf"
        session = _FakeSession(
            {
                landing_url: _FakeResponse(
                    200,
                    """
                    <html><body>
                      <a href="/collections/collection_2024/example-report-eng.pdf">Download PDF</a>
                      Landing page shell text only.
                    </body></html>
                    """,
                ),
                pdf_url: _FakeResponse(200, content=b"%PDF-1.4 fake pdf bytes"),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            patch.object(non_eu, "_extract_pdf_text", return_value="Real publication content from PDF."),
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

        self.assertEqual(enriched["full_text"], "Real publication content from PDF.")
        self.assertEqual(enriched["full_text_url"], pdf_url)
        self.assertEqual(enriched["full_text_format"], "pdf")
        self.assertEqual(enriched["full_text_error"], "")

    def test_enrich_canada_publication_uses_html_asset_when_pdf_candidate_is_unavailable(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.123456/publication.html"
        pdf_url = "https://www.publications.gc.ca/collections/collection_2024/example-report-eng.pdf"
        html_asset_url = "https://www.canada.ca/en/environment/example-report.html"
        session = _FakeSession(
            {
                landing_url: _FakeResponse(
                    200,
                    """
                    <html><body>
                      <a href="/collections/collection_2024/example-report-eng.pdf">Download PDF</a>
                      <a href="https://www.canada.ca/en/environment/example-report.html">HTML</a>
                    </body></html>
                    """,
                ),
                pdf_url: _FakeResponse(
                    200,
                    "<!DOCTYPE html><html><body>Archive wrapper</body></html>",
                    headers={"content-type": "text/html; charset=utf-8"},
                ),
                html_asset_url: _FakeResponse(
                    200,
                    "<html><body>Real publication content from HTML asset.</body></html>",
                    headers={"content-type": "text/html; charset=utf-8"},
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

        self.assertEqual(enriched["full_text"], "Real publication content from HTML asset.")
        self.assertEqual(enriched["full_text_url"], html_asset_url)
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
