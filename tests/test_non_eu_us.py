from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd  # noqa: E402

from policy_corpus_builder.adapters import non_eu  # noqa: E402


class USNonEUWorkflowTests(unittest.TestCase):
    def test_fetch_us_documents_uses_api_minimum_page_size(self) -> None:
        calls: list[dict[str, object]] = []

        class _FakeResponse:
            status_code = 200

            @staticmethod
            def json() -> dict[str, object]:
                return {
                    "data": [
                        {
                            "id": "EPA-HQ-OPP-2024-0010-0001",
                            "attributes": {"title": "Biodiversity review notice"},
                            "links": {"self": "https://api.regulations.gov/v4/documents/EPA-HQ-OPP-2024-0010-0001"},
                        }
                    ]
                }

        def _fake_safe_get(url: str, **kwargs):
            calls.append(kwargs.get("params", {}))
            if len(calls) == 1:
                return _FakeResponse()
            return None

        with patch.object(non_eu, "safe_get", side_effect=_fake_safe_get):
            df = non_eu.fetch_us_documents(
                ["biodiversity"],
                api_key="test-key",
                max_per_term=3,
                page_size=250,
                sleep_s=0,
            )

        self.assertEqual(calls[0]["page[size]"], 5)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["title"], "Biodiversity review notice")
        self.assertEqual(df.iloc[0]["source"], "US")

    def test_us_json_to_text_builds_usable_text(self) -> None:
        text = non_eu.us_json_to_text(
            {
                "data": {
                    "attributes": {
                        "title": "Endangered Species Act Review",
                        "documentType": "Notice",
                        "agencyId": "FWS",
                        "docketId": "FWS-HQ-ES-2025-0001",
                        "postedDate": "2025-02-01",
                        "summary": "Summary body.",
                        "documentAbstract": "Abstract body.",
                    }
                }
            }
        )

        self.assertIn("Endangered Species Act Review", text)
        self.assertIn("Notice", text)
        self.assertIn("Summary body.", text)
        self.assertIn("Abstract body.", text)

    def test_build_non_eu_fulltext_docs_uses_env_us_api_key_when_not_passed(self) -> None:
        raw_hits_df = pd.DataFrame(
            [
                {
                    "doc_id": "EPA-HQ-OW-2021-0736-1508",
                    "country": "United States",
                    "jurisdiction": "United States",
                    "doc_uid": "EPA-HQ-OW-2021-0736-1508",
                    "title": "Test title",
                    "url": "https://api.regulations.gov/v4/documents/EPA-HQ-OW-2021-0736-1508",
                    "api_self": "https://api.regulations.gov/v4/documents/EPA-HQ-OW-2021-0736-1508",
                    "lang": "en",
                    "date": "2024-01-01",
                    "year": "2024",
                    "source": "US",
                    "term": "biodiversity",
                }
            ]
        )

        with patch.dict(os.environ, {"REGULATIONS_GOV_API_KEY": "env-key"}, clear=False):
            with patch.object(non_eu, "add_full_texts_parallel", return_value=[]) as mocked:
                non_eu.build_non_eu_fulltext_docs(raw_hits_df, max_workers=1, progress_every=0)

        self.assertEqual(mocked.call_args.kwargs["us_api_key"], "env-key")


if __name__ == "__main__":
    unittest.main()
