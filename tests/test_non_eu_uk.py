from __future__ import annotations

import unittest
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu

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

UK_ATOM_FEED = """
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Search results</title>
  <entry>
    <title>The Environmental Targets (Biodiversity) (England) Regulations 2023</title>
    <id>https://www.legislation.gov.uk/uksi/2023/91/contents/made</id>
    <link rel="alternate" href="https://www.legislation.gov.uk/uksi/2023/91/contents/made" />
  </entry>
  <entry>
    <title>Environment Act 2021</title>
    <id>https://www.legislation.gov.uk/ukpga/2021/30/part/6</id>
    <link rel="alternate" href="https://www.legislation.gov.uk/ukpga/2021/30/part/6" />
  </entry>
</feed>
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


class NonEUUkRetrievalTests(unittest.TestCase):
    def test_clean_uk_title_removes_site_noise(self) -> None:
        self.assertEqual(
            non_eu.clean_uk_title("PDF Environment Act 2021 - Legislation.gov.uk"),
            "Environment Act 2021",
        )

    def test_extract_uk_feed_links_decodes_atom_entries(self) -> None:
        results = non_eu._extract_uk_feed_links(UK_ATOM_FEED)

        self.assertEqual(
            results,
            [
                (
                    "https://www.legislation.gov.uk/uksi/2023/91",
                    "The Environmental Targets (Biodiversity) (England) Regulations 2023",
                ),
                (
                    "https://www.legislation.gov.uk/ukpga/2021/30",
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
                    "https://www.legislation.gov.uk/uksi/2023/91",
                    "The Environmental Targets (Biodiversity) (England) Regulations 2023",
                ),
                (
                    "https://www.legislation.gov.uk/ukpga/2021/30",
                    "Environment Act 2021",
                ),
            ],
        )

    def test_uk_contents_url_preserves_contents_view_separately(self) -> None:
        self.assertEqual(
            non_eu.uk_contents_url("https://www.legislation.gov.uk/uksi/2023/91/contents/made"),
            "https://www.legislation.gov.uk/uksi/2023/91/contents",
        )

    def test_fetch_uk_documents_uses_atom_feed_when_available(self) -> None:
        feed_response = _FakeResponse(200, UK_ATOM_FEED)

        with (
            patch.object(non_eu, "safe_get", return_value=feed_response),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_uk_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertEqual(
            sorted(df["url"].tolist()),
            [
                "https://www.legislation.gov.uk/ukpga/2021/30",
                "https://www.legislation.gov.uk/uksi/2023/91",
            ],
        )

    def test_fetch_uk_documents_uses_search_fallback_when_feed_is_challenged(self) -> None:
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
                return_value=type(
                    "_FakeUrlOpenResponse",
                    (),
                    {
                        "read": lambda self: DUCKDUCKGO_HTML.encode("utf-8"),
                        "__enter__": lambda self: self,
                        "__exit__": lambda self, exc_type, exc, tb: None,
                    },
                )(),
            ),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_uk_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertEqual(
            sorted(df["url"].tolist()),
            [
                "https://www.legislation.gov.uk/ukpga/2021/30",
                "https://www.legislation.gov.uk/uksi/2023/91",
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
        captured_headers: list[dict[str, str]] = []

        class _XmlSession:
            def get(self, url, *args, **kwargs):
                captured_headers.append(dict(kwargs.get("headers", {})))
                if url.endswith("/data.xml"):
                    return _FakeResponse(200, UK_XML)
                return _FakeResponse(404, "")

        previous_session = getattr(non_eu._thread_local, "session", None)
        previous_session_user_agent = getattr(non_eu._thread_local, "session_user_agent", None)
        previous_robots = getattr(non_eu._thread_local, "robots", None)
        previous_robots_user_agent = getattr(non_eu._thread_local, "robots_user_agent", None)
        non_eu._thread_local.session = _XmlSession()
        non_eu._thread_local.session_user_agent = non_eu.UA
        non_eu._thread_local.robots = type("AllowAll", (), {"allowed": staticmethod(lambda url: True)})()
        non_eu._thread_local.robots_user_agent = non_eu.UA
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
            if previous_session_user_agent is None and hasattr(non_eu._thread_local, "session_user_agent"):
                delattr(non_eu._thread_local, "session_user_agent")
            else:
                non_eu._thread_local.session_user_agent = previous_session_user_agent
            if previous_robots is None and hasattr(non_eu._thread_local, "robots"):
                delattr(non_eu._thread_local, "robots")
            else:
                non_eu._thread_local.robots = previous_robots
            if previous_robots_user_agent is None and hasattr(non_eu._thread_local, "robots_user_agent"):
                delattr(non_eu._thread_local, "robots_user_agent")
            else:
                non_eu._thread_local.robots_user_agent = previous_robots_user_agent

        self.assertIn("Biodiversity gain plans.", enriched["full_text"])
        self.assertEqual(enriched["full_text_format"], "uk_xml")
        self.assertEqual(
            enriched["full_text_url"],
            "https://www.legislation.gov.uk/ukpga/2021/30/data.xml",
        )
        self.assertEqual(
            captured_headers[0]["User-Agent"],
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        )
        self.assertEqual(
            captured_headers[0]["Accept"],
            "application/xml,text/xml;q=0.9,*/*;q=0.8",
        )
        self.assertEqual(
            captured_headers[0]["Accept-Language"],
            "en-GB,en;q=0.9",
        )

    def test_enrich_one_record_fulltext_marks_waf_challenge_instead_of_html_empty(self) -> None:
        class _ChallengeSession:
            def get(self, *args, **kwargs):
                return _FakeResponse(202, "", headers={"x-amzn-waf-action": "challenge"})

        previous_session = getattr(non_eu._thread_local, "session", None)
        previous_session_user_agent = getattr(non_eu._thread_local, "session_user_agent", None)
        previous_robots = getattr(non_eu._thread_local, "robots", None)
        previous_robots_user_agent = getattr(non_eu._thread_local, "robots_user_agent", None)
        non_eu._thread_local.session = _ChallengeSession()
        non_eu._thread_local.session_user_agent = non_eu.UA
        non_eu._thread_local.robots = type("AllowAll", (), {"allowed": staticmethod(lambda url: True)})()
        non_eu._thread_local.robots_user_agent = non_eu.UA
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
            if previous_session_user_agent is None and hasattr(non_eu._thread_local, "session_user_agent"):
                delattr(non_eu._thread_local, "session_user_agent")
            else:
                non_eu._thread_local.session_user_agent = previous_session_user_agent
            if previous_robots is None and hasattr(non_eu._thread_local, "robots"):
                delattr(non_eu._thread_local, "robots")
            else:
                non_eu._thread_local.robots = previous_robots
            if previous_robots_user_agent is None and hasattr(non_eu._thread_local, "robots_user_agent"):
                delattr(non_eu._thread_local, "robots_user_agent")
            else:
                non_eu._thread_local.robots_user_agent = previous_robots_user_agent

        self.assertEqual(enriched["full_text"], "")
        self.assertEqual(enriched["full_text_error"], "waf_challenge")


if __name__ == "__main__":
    unittest.main()
