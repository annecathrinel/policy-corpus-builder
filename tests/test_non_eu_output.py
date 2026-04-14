from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


class NonEUOutputContractTests(unittest.TestCase):
    def test_non_eu_adapter_validates_optional_user_agent_when_provided(self) -> None:
        adapter = NonEUAdapter()
        source = SourceConfig(
            name="uk-legislation",
            adapter="non-eu",
            settings={"countries": ["UK"], "user_agent": ""},
        )

        with self.assertRaisesRegex(Exception, "source.settings.user_agent"):
            adapter.validate_source_config(source, base_path=Path("."))

    def test_non_eu_adapter_promotes_cleaned_full_text_and_trims_legacy_fields(self) -> None:
        adapter = NonEUAdapter()
        source = SourceConfig(name="uk-legislation", adapter="non-eu")
        row = {
            "country": "United Kingdom",
            "contents_url": "https://www.legislation.gov.uk/ukpga/2024/1/contents",
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
            "url": "https://www.legislation.gov.uk/ukpga/2024/1",
            "year": "2024",
        }

        result = adapter._row_to_result(
            row,
            source=source,
            source_log=[{"source": "UK", "ok": True}],
        )

        self.assertEqual(result.payload["full_text"], "Cleaned body text.")
        self.assertEqual(result.payload["url"], "https://www.legislation.gov.uk/ukpga/2024/1")
        self.assertEqual(
            result.payload["raw_record"]["contents_url"],
            "https://www.legislation.gov.uk/ukpga/2024/1/contents",
        )
        self.assertEqual(result.payload["raw_record"]["retrieval_status"], "ok")
        self.assertNotIn("raw_cache_note", result.payload["raw_record"])
        self.assertNotIn("retrieval_track", result.payload["raw_record"])
        self.assertNotIn("raw_text", result.payload["raw_record"])
        self.assertNotIn("text_norm", result.payload["raw_record"])
        self.assertNotIn("text_missing", result.payload["raw_record"])

    def test_non_eu_adapter_passes_user_agent_to_workflow(self) -> None:
        adapter = NonEUAdapter()
        source = SourceConfig(
            name="uk-legislation",
            adapter="non-eu",
            settings={"countries": ["UK"], "user_agent": "policy-corpus-builder/0.1 (contact@example.org)"},
        )
        query = Query(text="biodiversity", query_id="inline-001", origin="inline")

        class _RunResult:
            source_log = []

            class _EmptyFrame:
                @staticmethod
                def to_dict(orient: str = "records"):
                    return []

            harmonized_docs_df = _EmptyFrame()

        with patch(
            "policy_corpus_builder.adapters.non_eu_adapter.run_non_eu_query_pipeline",
            return_value=_RunResult(),
        ) as mocked:
            adapter.collect(source, query, base_path=Path("."))

        self.assertEqual(
            mocked.call_args.kwargs["user_agent"],
            "policy-corpus-builder/0.1 (contact@example.org)",
        )

    def test_build_non_eu_fulltext_docs_marks_waf_blocks_as_upstream_blocked(self) -> None:
        raw_hits_df = __import__("pandas").DataFrame(
            [
                {
                    "doc_id": "ukpga_2021_30",
                    "country": "United Kingdom",
                    "jurisdiction": "United Kingdom",
                    "doc_uid": "ukpga_2021_30",
                    "title": "Environment Act 2021",
                    "url": "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                    "lang": "en",
                    "date": "2021",
                    "year": "2021",
                    "source": "UK",
                    "term": "biodiversity",
                }
            ]
        )

        from unittest.mock import patch
        from policy_corpus_builder.adapters import non_eu

        with patch.object(
            non_eu,
            "add_full_texts_parallel",
            return_value=[
                {
                    "doc_id": "ukpga_2021_30",
                    "jurisdiction": "United Kingdom",
                    "title": "Environment Act 2021",
                    "url": "https://www.legislation.gov.uk/ukpga/2021/30/contents",
                    "lang": "en",
                    "date": "2021",
                    "year": "2021",
                    "source": "UK",
                    "full_text": "",
                    "full_text_url": "",
                    "full_text_error": "waf_challenge",
                    "full_text_format": "",
                }
            ],
        ):
            docs = non_eu.build_non_eu_fulltext_docs(raw_hits_df, obey_robots=False)

        self.assertEqual(docs.iloc[0]["retrieval_status"], "upstream_blocked")


if __name__ == "__main__":
    unittest.main()
