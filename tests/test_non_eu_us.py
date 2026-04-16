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

    def test_extract_us_download_candidates_prefers_document_content_then_attachments(self) -> None:
        candidates = non_eu.extract_us_download_candidates(
            {
                "data": {
                    "attributes": {
                        "fileFormats": [
                            {
                                "fileUrl": "https://downloads.regulations.gov/ABC-0001/content.pdf",
                                "format": "pdf",
                            },
                            {
                                "fileUrl": "https://downloads.regulations.gov/ABC-0001/content.htm",
                                "format": "htm",
                            },
                        ]
                    }
                },
                "included": [
                    {
                        "type": "attachments",
                        "attributes": {
                            "title": "Attachment A",
                            "docOrder": 1,
                            "fileFormats": [
                                {
                                    "fileUrl": "https://downloads.regulations.gov/ABC-0001/attachment_1.pdf",
                                    "format": "pdf",
                                }
                            ],
                        },
                    }
                ],
            }
        )

        self.assertEqual(candidates[0]["file_url"], "https://downloads.regulations.gov/ABC-0001/content.htm")
        self.assertEqual(candidates[1]["file_url"], "https://downloads.regulations.gov/ABC-0001/content.pdf")
        self.assertEqual(candidates[2]["file_url"], "https://downloads.regulations.gov/ABC-0001/attachment_1.pdf")

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

    def test_enrich_one_record_fulltext_prefers_us_download_file_over_metadata(self) -> None:
        class _FakeResponse:
            def __init__(self, *, json_data=None, text="", content=b"", headers=None, status_code=200):
                self._json_data = json_data
                self.text = text
                self.content = content
                self.headers = headers or {}
                self.status_code = status_code

            def json(self):
                return self._json_data

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f"http_{self.status_code}")

        class _FakeSession:
            def get(self, url, **kwargs):
                if url == "https://api.regulations.gov/v4/documents/ABC-0001":
                    return _FakeResponse(
                        json_data={
                            "data": {
                                "attributes": {
                                    "title": "Metadata title",
                                    "fileFormats": [
                                        {
                                            "fileUrl": "https://downloads.regulations.gov/ABC-0001/content.htm",
                                            "format": "htm",
                                        }
                                    ],
                                }
                            }
                        }
                    )
                if url == "https://downloads.regulations.gov/ABC-0001/content.htm":
                    return _FakeResponse(text="<html><body><h1>Real document body</h1><p>Section text.</p></body></html>")
                raise AssertionError(url)

        class _AllowAllRobots:
            @staticmethod
            def allowed(url: str) -> bool:
                return True

        with patch.object(non_eu, "_get_thread_session", return_value=_FakeSession()):
            with patch.object(non_eu, "_get_thread_robots", return_value=_AllowAllRobots()):
                enriched = non_eu.enrich_one_record_fulltext(
                    {
                        "source": "US",
                        "api_self": "https://api.regulations.gov/v4/documents/ABC-0001",
                        "url": "https://api.regulations.gov/v4/documents/ABC-0001",
                    },
                    us_api_key="test-key",
                    obey_robots=True,
                )

        self.assertEqual(enriched["full_text_url"], "https://downloads.regulations.gov/ABC-0001/content.htm")
        self.assertEqual(enriched["full_text_format"], "html")
        self.assertIn("Real document body", enriched["full_text"])


if __name__ == "__main__":
    unittest.main()
