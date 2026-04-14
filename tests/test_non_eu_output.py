from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


class NonEUOutputContractTests(unittest.TestCase):
    def test_non_eu_adapter_promotes_cleaned_full_text_and_trims_legacy_fields(self) -> None:
        adapter = NonEUAdapter()
        source = SourceConfig(name="uk-legislation", adapter="non-eu")
        row = {
            "country": "United Kingdom",
            "date": "2024",
            "doc_id": "ukpga_2024_1",
            "doc_uid": "ukpga_2024_1",
            "full_text_clean": "Cleaned body text.",
            "full_text_error": "",
            "full_text_format": "html",
            "full_text_url": "https://www.legislation.gov.uk/ukpga/2024/1/contents",
            "has_text": "True",
            "jurisdiction": "United Kingdom",
            "lang": "en",
            "matched_terms": "[\"biodiversity\"]",
            "raw_cache_note": "old internal field",
            "raw_text": "old raw text field",
            "retrieval_status": "ok",
            "retrieval_track": "old internal field",
            "source": "UK",
            "source_file": "",
            "text_len": "18",
            "text_missing": "False",
            "text_norm": "old normalized text",
            "title": "Environment Act 2024",
            "url": "https://www.legislation.gov.uk/ukpga/2024/1/contents",
            "year": "2024",
        }

        result = adapter._row_to_result(
            row,
            source=source,
            source_log=[{"source": "UK", "ok": True}],
        )

        self.assertEqual(result.payload["full_text"], "Cleaned body text.")
        self.assertNotIn("raw_cache_note", result.payload["raw_record"])
        self.assertNotIn("retrieval_track", result.payload["raw_record"])
        self.assertNotIn("raw_text", result.payload["raw_record"])
        self.assertNotIn("text_norm", result.payload["raw_record"])
        self.assertNotIn("text_missing", result.payload["raw_record"])


if __name__ == "__main__":
    unittest.main()
