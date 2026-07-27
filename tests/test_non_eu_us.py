from __future__ import annotations

import os
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
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

    def test_fetch_us_documents_prints_progress_diagnostics_by_default(self) -> None:
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

        calls: list[dict[str, object]] = []

        def _fake_safe_get(url: str, **kwargs):
            calls.append(kwargs.get("params", {}))
            if len(calls) == 1:
                return _FakeResponse()
            return None

        stdout = StringIO()
        with patch.object(non_eu, "safe_get", side_effect=_fake_safe_get):
            with redirect_stdout(stdout):
                non_eu.fetch_us_documents(
                    ["biodiversity"],
                    api_key="test-key",
                    max_per_term=3,
                    sleep_s=0,
                )

        output = stdout.getvalue()
        self.assertIn("[US] term='biodiversity'", output)
        self.assertIn("candidates=1", output)
        self.assertIn("DONE -> kept=1", output)
        self.assertIn("[US] total rows kept: 1", output)

    def test_fetch_us_documents_verbose_false_suppresses_output(self) -> None:
        class _FakeResponse:
            status_code = 200

            @staticmethod
            def json() -> dict[str, object]:
                return {"data": []}

        stdout = StringIO()
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse()):
            with redirect_stdout(stdout):
                non_eu.fetch_us_documents(
                    ["biodiversity"],
                    api_key="test-key",
                    max_per_term=3,
                    sleep_s=0,
                    verbose=False,
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_fetch_us_documents_logs_non_200_status_and_stops_term(self) -> None:
        class _FakeResponse:
            status_code = 503

        stdout = StringIO()
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse()):
            with redirect_stdout(stdout):
                df = non_eu.fetch_us_documents(
                    ["biodiversity"],
                    api_key="test-key",
                    max_per_term=3,
                    sleep_s=0,
                )

        self.assertIn("term='biodiversity' page=1 ERROR -> HTTP 503", stdout.getvalue())
        self.assertEqual(len(df), 0)

    def test_fetch_us_documents_logs_a_distinct_message_for_rate_limiting(self) -> None:
        # Regression test: a 429 from regulations.gov was previously
        # reported as an undifferentiated "ERROR -> HTTP 429" alongside any
        # other non-200 status, with no indication it was rate limiting
        # specifically (as opposed to, say, a real server error) - and,
        # worse, urllib3's own retry/backoff inside the session's transport
        # adapter could silently eat up to roughly a minute per request
        # before this loop ever saw the 429 at all (see
        # test_fetch_us_documents_builds_a_lighter_retry_session_by_default).
        class _FakeResponse:
            status_code = 429

        stdout = StringIO()
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse()):
            with redirect_stdout(stdout):
                df = non_eu.fetch_us_documents(
                    ["biodiversity"],
                    api_key="test-key",
                    max_per_term=3,
                    sleep_s=0,
                )

        self.assertIn("term='biodiversity' page=1 ERROR -> HTTP 429 (rate limited by regulations.gov)", stdout.getvalue())
        self.assertEqual(len(df), 0)

    def test_fetch_us_documents_builds_a_lighter_retry_session_by_default(self) -> None:
        # Regression test: build_session()'s default retry policy
        # (total_retries=6, backoff_factor=1.0, 429 included in
        # status_forcelist) is tuned as a generic "be persistent" default,
        # but fetch_us_documents' search loop is fully sequential (one
        # term, one page, at a time) - so every rate-limited request pays
        # whatever retry cost the transport adapter incurs, silently,
        # before this loop even sees a response. A production run
        # consistent with regulations.gov rate limiting never saw its
        # "Running jurisdiction: US" milestone appear at all. This asserts
        # fetch_us_documents asks for a much lighter, bounded retry budget
        # when it builds its own session (no session= passed in).
        captured_kwargs: list[dict] = []

        def _fake_build_session(**kwargs):
            captured_kwargs.append(kwargs)
            return non_eu.requests.Session()

        class _FakeResponse:
            status_code = 200

            @staticmethod
            def json() -> dict[str, object]:
                return {"data": []}

        with (
            patch.object(non_eu, "build_session", side_effect=_fake_build_session),
            patch.object(non_eu, "safe_get", return_value=_FakeResponse()),
        ):
            non_eu.fetch_us_documents(
                ["biodiversity"],
                api_key="test-key",
                max_per_term=3,
                sleep_s=0,
                verbose=False,
            )

        self.assertEqual(len(captured_kwargs), 1)
        self.assertEqual(captured_kwargs[0].get("total_retries"), 2)
        self.assertEqual(captured_kwargs[0].get("backoff_factor"), 0.5)

    def test_fetch_us_documents_does_not_build_a_session_when_one_is_provided(self) -> None:
        class _FakeResponse:
            status_code = 200

            @staticmethod
            def json() -> dict[str, object]:
                return {"data": []}

        with (
            patch.object(non_eu, "build_session") as mock_build_session,
            patch.object(non_eu, "safe_get", return_value=_FakeResponse()),
        ):
            non_eu.fetch_us_documents(
                ["biodiversity"],
                api_key="test-key",
                max_per_term=3,
                sleep_s=0,
                verbose=False,
                session=non_eu.requests.Session(),
            )

        mock_build_session.assert_not_called()

    def test_fetch_us_documents_falls_back_to_id_based_url_when_links_self_missing(self) -> None:
        # Regression test: a real production run found regulations.gov's search
        # response omitting "links.self" for every single result (0/2023 full
        # text retrieved), even though "id" was always populated. Without a
        # fallback, every one of those records fails full-text enrichment with
        # "no_url_candidate".
        class _FakeResponse:
            status_code = 200

            @staticmethod
            def json() -> dict[str, object]:
                return {
                    "data": [
                        {
                            "id": "EPA-HQ-OAR-2022-0606-0019",
                            "attributes": {"title": "Technical Support Document"},
                            "links": {},
                        }
                    ]
                }

        def _fake_safe_get(url: str, **kwargs):
            return _FakeResponse()

        with patch.object(non_eu, "safe_get", side_effect=_fake_safe_get):
            df = non_eu.fetch_us_documents(
                ["biodiversity"],
                api_key="test-key",
                max_per_term=1,
                sleep_s=0,
            )

        self.assertEqual(len(df), 1)
        expected_url = f"{non_eu.US_BASE}/documents/EPA-HQ-OAR-2022-0606-0019"
        self.assertEqual(df.iloc[0]["api_self"], expected_url)
        self.assertEqual(df.iloc[0]["url"], expected_url)
        self.assertEqual(df.iloc[0]["doc_url"], expected_url)

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
        captured_download_headers: dict[str, str] = {}

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
                    captured_download_headers.update(kwargs.get("headers", {}))
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
        self.assertEqual(
            captured_download_headers["Referer"],
            "https://api.regulations.gov/v4/documents/ABC-0001",
        )
        self.assertIn("Mozilla/5.0", captured_download_headers["User-Agent"])


if __name__ == "__main__":
    unittest.main()
