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


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


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


if __name__ == "__main__":
    unittest.main()
