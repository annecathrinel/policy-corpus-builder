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
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class NonEUCanadaTests(unittest.TestCase):
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
