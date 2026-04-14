from __future__ import annotations

import unittest
from io import BytesIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu


DIRECT_UK_HTML = """
<html>
  <body>
    <a href="/uksi/2023/91/contents/made">The Environmental Targets (Biodiversity) (England) Regulations 2023</a>
    <a href="/ukpga/2021/30/part/6">Environment Act 2021</a>
    <a href="/uksi?title=biodiversity">Search Results</a>
  </body>
</html>
"""

DUCKDUCKGO_HTML = """
<html>
  <body>
    <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.legislation.gov.uk%2Fuksi%2F2023%2F91%2Fcontents%2Fmade&rut=abc">
      The Environmental Targets (Biodiversity) (England) Regulations 2023
    </a>
    <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.legislation.gov.uk%2Fukpga%2F2021%2F30%2Fpart%2F6&rut=def">
      Environment Act 2021
    </a>
    <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.legislation.gov.uk%2Fuksi%3Ftitle%3Dbiodiversity&rut=ghi">
      Search Results
    </a>
  </body>
</html>
"""

UK_XML = """
<Legislation>
  <Metadata>
    <Title>Environment Act 2021</Title>
  </Metadata>
  <Body>
    <Title>Environment Act 2021</Title>
    <P>1 Biodiversity gain plans.</P>
    <P>Schedule 1 makes further provision.</P>
  </Body>
</Legislation>
"""


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeUrlOpenResponse:
    def __init__(self, html: str, status: int = 200):
        self._html = html.encode("utf-8")
        self.status = status

    def read(self) -> bytes:
        return self._html

    def __enter__(self) -> _FakeUrlOpenResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class NonEUUkRetrievalTests(unittest.TestCase):
    def test_clean_uk_title_removes_site_noise(self) -> None:
        self.assertEqual(
            non_eu.clean_uk_title("PDF Environment Act 2021 - Legislation.gov.uk"),
            "Environment Act 2021",
        )

    def test_extract_uk_search_result_links_filters_to_document_paths(self) -> None:
        results = non_eu._extract_uk_search_result_links(DIRECT_UK_HTML)

        self.assertEqual(
            results,
            [
                (
                    "https://www.legislation.gov.uk/uksi/2023/91/contents",
                    "The Environmental Targets (Biodiversity) (England) Regulations 2023",
                ),
                (
                    "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                    "Environment Act 2021",
                ),
            ],
        )

    def test_extract_uk_duckduckgo_links_decodes_redirect_targets(self) -> None:
        results = non_eu._extract_uk_duckduckgo_links(DUCKDUCKGO_HTML)

        self.assertEqual(
            results,
            [
                (
                    "https://www.legislation.gov.uk/uksi/2023/91/contents",
                    "The Environmental Targets (Biodiversity) (England) Regulations 2023",
                ),
                (
                    "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                    "Environment Act 2021",
                ),
            ],
        )

    def test_fetch_uk_documents_uses_search_fallback_when_direct_search_is_challenged(self) -> None:
        challenged_response = _FakeResponse(
            202,
            "",
            headers={"x-amzn-waf-action": "challenge"},
        )

        with (
            patch.object(non_eu, "safe_get", return_value=challenged_response),
            patch.object(
                non_eu,
                "urlopen",
                return_value=_FakeUrlOpenResponse(DUCKDUCKGO_HTML),
            ),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_uk_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertEqual(
            sorted(df["url"].tolist()),
            [
                "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                "https://www.legislation.gov.uk/uksi/2023/91/contents",
            ],
        )
        self.assertEqual(
            sorted(df["title"].tolist()),
            [
                "Environment Act 2021",
                "The Environmental Targets (Biodiversity) (England) Regulations 2023",
            ],
        )

    def test_get_url_candidates_for_uk_tries_multiple_content_variants(self) -> None:
        candidates = non_eu.get_url_candidates(
            {"url": "https://www.legislation.gov.uk/ukpga/2021/30/contents"},
            "UK",
            None,
        )

        self.assertEqual(
            candidates,
            [
                ("https://www.legislation.gov.uk/ukpga/2021/30/data.xml", "uk_xml"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/made/data.xml", "uk_xml"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/enacted/data.xml", "uk_xml"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/data.xht", "html"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/made/data.xht", "html"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/enacted/data.xht", "html"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/contents", "html"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/made", "html"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/enacted", "html"),
                ("https://www.legislation.gov.uk/ukpga/2021/30/contents/made", "html"),
            ],
        )

    def test_uk_xml_to_text_extracts_document_text(self) -> None:
        text = non_eu.uk_xml_to_text(UK_XML)

        self.assertIn("Environment Act 2021", text)
        self.assertIn("Biodiversity gain plans.", text)
        self.assertNotIn("<Title>", text)

    def test_enrich_one_record_fulltext_uses_uk_xml_when_available(self) -> None:
        class _XmlSession:
            def get(self, url, *args, **kwargs):
                if url.endswith("/data.xml"):
                    return _FakeResponse(200, UK_XML)
                return _FakeResponse(404, "")

        previous_session = getattr(non_eu._thread_local, "session", None)
        previous_robots = getattr(non_eu._thread_local, "robots", None)
        non_eu._thread_local.session = _XmlSession()
        non_eu._thread_local.robots = type("AllowAll", (), {"allowed": staticmethod(lambda url: True)})()
        try:
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "UK",
                    "jurisdiction": "United Kingdom",
                    "url": "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                },
                us_api_key=None,
                obey_robots=False,
            )
        finally:
            if previous_session is None and hasattr(non_eu._thread_local, "session"):
                delattr(non_eu._thread_local, "session")
            else:
                non_eu._thread_local.session = previous_session
            if previous_robots is None and hasattr(non_eu._thread_local, "robots"):
                delattr(non_eu._thread_local, "robots")
            else:
                non_eu._thread_local.robots = previous_robots

        self.assertIn("Biodiversity gain plans.", enriched["full_text"])
        self.assertEqual(enriched["full_text_format"], "uk_xml")
        self.assertEqual(
            enriched["full_text_url"],
            "https://www.legislation.gov.uk/ukpga/2021/30/data.xml",
        )

    def test_enrich_one_record_fulltext_marks_waf_challenge_instead_of_html_empty(self) -> None:
        class _ChallengeSession:
            def get(self, *args, **kwargs):
                return _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"})

        previous_session = getattr(non_eu._thread_local, "session", None)
        previous_robots = getattr(non_eu._thread_local, "robots", None)
        non_eu._thread_local.session = _ChallengeSession()
        non_eu._thread_local.robots = type("AllowAll", (), {"allowed": staticmethod(lambda url: True)})()
        try:
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "UK",
                    "jurisdiction": "United Kingdom",
                    "url": "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                },
                us_api_key=None,
                obey_robots=False,
            )
        finally:
            if previous_session is None and hasattr(non_eu._thread_local, "session"):
                delattr(non_eu._thread_local, "session")
            else:
                non_eu._thread_local.session = previous_session
            if previous_robots is None and hasattr(non_eu._thread_local, "robots"):
                delattr(non_eu._thread_local, "robots")
            else:
                non_eu._thread_local.robots = previous_robots

        self.assertEqual(enriched["full_text"], "")
        self.assertEqual(enriched["full_text_error"], "waf_challenge")


if __name__ == "__main__":
    unittest.main()
