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


if __name__ == "__main__":
    unittest.main()
