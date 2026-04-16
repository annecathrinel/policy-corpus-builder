from __future__ import annotations

import unittest
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
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text

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
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, AUS_SEARCH_HTML)):
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
