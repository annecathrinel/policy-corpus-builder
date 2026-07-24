import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.base import AdapterConfigError  # noqa: E402
from policy_corpus_builder.adapters.eurlex_nim_adapter import EurlexNIMAdapter  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.pipeline import normalize_adapter_results  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


class _FakeSoapResponse:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} error", response=self)


class EurlexWsDoQueryRetryTests(unittest.TestCase):
    def test_eurlex_ws_doquery_retries_transient_server_error_then_succeeds(self) -> None:
        # Regression test: a real HPC run (2026-05-26) lost a multi-hour job to a
        # single transient 500 from the EUR-Lex SOAP endpoint because the original
        # implementation made exactly one POST attempt with no retry.
        import policy_corpus_builder.adapters.eurlex_nim_supported.surface as nim_surface_module

        responses_queue = [
            _FakeSoapResponse(500),
            _FakeSoapResponse(200, b"<soap:Envelope>ok</soap:Envelope>"),
        ]

        class _FakeSession:
            def __init__(self) -> None:
                self.trust_env = True
                self.calls = 0

            def post(self, *args, **kwargs):
                response = responses_queue[self.calls]
                self.calls += 1
                return response

        fake_session = _FakeSession()

        with (
            patch.object(nim_surface_module, "get_ws_credentials", return_value=("user", "pass")),
            patch.object(nim_surface_module.requests, "Session", return_value=fake_session),
            patch.object(nim_surface_module.time, "sleep"),
        ):
            content = nim_surface_module.eurlex_ws_doquery(
                "some query", max_retries=2, backoff_factor=0.01
            )

        self.assertEqual(content, b"<soap:Envelope>ok</soap:Envelope>")
        self.assertEqual(fake_session.calls, 2)

    def test_eurlex_ws_doquery_gives_up_after_exhausting_retries(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.surface as nim_surface_module

        class _AlwaysFailingSession:
            def __init__(self) -> None:
                self.trust_env = True
                self.calls = 0

            def post(self, *args, **kwargs):
                self.calls += 1
                return _FakeSoapResponse(503)

        fake_session = _AlwaysFailingSession()

        with (
            patch.object(nim_surface_module, "get_ws_credentials", return_value=("user", "pass")),
            patch.object(nim_surface_module.requests, "Session", return_value=fake_session),
            patch.object(nim_surface_module.time, "sleep"),
        ):
            with self.assertRaises(requests.exceptions.HTTPError):
                nim_surface_module.eurlex_ws_doquery("some query", max_retries=2, backoff_factor=0.01)

        self.assertEqual(fake_session.calls, 3)

    def test_eurlex_ws_doquery_does_not_retry_non_retryable_client_errors(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.surface as nim_surface_module

        class _AuthFailureSession:
            def __init__(self) -> None:
                self.trust_env = True
                self.calls = 0

            def post(self, *args, **kwargs):
                self.calls += 1
                return _FakeSoapResponse(401)

        fake_session = _AuthFailureSession()

        with (
            patch.object(nim_surface_module, "get_ws_credentials", return_value=("user", "pass")),
            patch.object(nim_surface_module.requests, "Session", return_value=fake_session),
            patch.object(nim_surface_module.time, "sleep"),
        ):
            with self.assertRaises(requests.exceptions.HTTPError):
                nim_surface_module.eurlex_ws_doquery("some query", max_retries=3, backoff_factor=0.01)

        self.assertEqual(fake_session.calls, 1)


class _FakeNimTextResponse:
    def __init__(self, status_code: int, text: str = "", url: str = "https://example.test/doc"):
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")
        self.url = url
        self.headers = {"Content-Type": "text/html; charset=utf-8"}


class FetchTextFromCandidateRetryTests(unittest.TestCase):
    # Regression tests for a real smoke test finding: EUR-Lex's legacy
    # LexUriServ route returned HTTP 202 ("accepted, still generating") for
    # 23/25 NIM documents, and the code treated 202 as an immediate terminal
    # failure instead of retrying it like every other non-2xx status.

    def test_fetch_text_from_candidate_retries_202_then_succeeds(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.surface as nim_surface_module

        responses_queue = [
            _FakeNimTextResponse(202),
            _FakeNimTextResponse(202),
            _FakeNimTextResponse(200, text="<html><body>Real NIM document text.</body></html>"),
        ]

        class _FakeSession:
            def __init__(self) -> None:
                self.calls = 0

            def get(self, *args, **kwargs):
                response = responses_queue[self.calls]
                self.calls += 1
                return response

        fake_session = _FakeSession()

        with tempfile.TemporaryDirectory() as tmpdir, patch.object(nim_surface_module.time, "sleep"):
            result = nim_surface_module._fetch_text_from_candidate(
                {"url": "https://eur-lex.europa.eu/LexUriServ/LexUriServ.do?uri=CELEX:1", "link_type": "lexuriserv"},
                session=fake_session,
                timeout=(15, 90),
                retries=3,
                min_interval_s=0,
                file_cache_dir=Path(tmpdir),
                verbose=False,
            )

        self.assertEqual(fake_session.calls, 3)
        self.assertIn("Real NIM document text.", result["text"])
        self.assertEqual(result["error"], "")

    def test_fetch_text_from_candidate_reports_last_error_after_exhausting_202_retries(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.surface as nim_surface_module

        class _AlwaysPendingSession:
            def __init__(self) -> None:
                self.calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                return _FakeNimTextResponse(202)

        fake_session = _AlwaysPendingSession()

        with tempfile.TemporaryDirectory() as tmpdir, patch.object(nim_surface_module.time, "sleep"):
            result = nim_surface_module._fetch_text_from_candidate(
                {"url": "https://eur-lex.europa.eu/LexUriServ/LexUriServ.do?uri=CELEX:1", "link_type": "lexuriserv"},
                session=fake_session,
                timeout=(15, 90),
                retries=2,
                min_interval_s=0,
                file_cache_dir=Path(tmpdir),
                verbose=False,
            )

        # retries=2 means 3 total attempts (range(retries + 1)).
        self.assertEqual(fake_session.calls, 3)
        self.assertEqual(result["text"], "")
        self.assertEqual(result["error"], "HTTP 202")


class EurlexNIMAdapterTests(unittest.TestCase):
    def test_retrieve_nim_rows_normalizes_act_celex_into_celex(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.surface as nim_surface_module
        import policy_corpus_builder.adapters.eurlex_nim_supported.workflow as nim_workflow_module

        original_fetch = nim_workflow_module.get_national_transpositions_by_celex_ws

        def fake_fetch(*args, **kwargs):
            return pd.DataFrame(
                [
                    {
                        "act_celex": "32014L0089",
                        "nim_celex": "72014L0089DNK_270540",
                        "national_measure_id": "270540",
                        "nim_date": "2016-06-01",
                        "nim_title": "Bekendtgorelse om havplanlaegning",
                        "member_state_iso3": "DNK",
                        "member_state_name": "Denmark",
                        "eurlex_url": "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72014L0089DNK_270540",
                    }
                ]
            )

        nim_workflow_module.get_national_transpositions_by_celex_ws = fake_fetch
        try:
            result = nim_workflow_module._retrieve_nim_rows(
                pd.DataFrame(
                    [
                        {
                            "celex": "32014L0089",
                            "eu_act_title": "Directive Example",
                            "eu_act_type": "Directive",
                            "year": 2014,
                        }
                    ]
                ),
                {},
            )
        finally:
            nim_workflow_module.get_national_transpositions_by_celex_ws = original_fetch

        self.assertIn("celex", result.columns)
        self.assertEqual(result.iloc[0]["celex"], "32014L0089")

    def test_retrieve_nim_rows_skips_act_whose_lookup_raises_and_continues(self) -> None:
        # Regression test: a real production run (2026-07) lost its entire NIM
        # stage - including 5865 already-cached full-text results - because one
        # EU act's SOAP lookup exhausted all retries on a persistent (non-
        # transient) 500 and the exception propagated out of _retrieve_nim_rows,
        # crashing the whole multi-hour job. A single bad seed must be skipped,
        # not fatal.
        import policy_corpus_builder.adapters.eurlex_nim_supported.workflow as nim_workflow_module

        original_fetch = nim_workflow_module.get_national_transpositions_by_celex_ws

        def fake_fetch(celex, *args, **kwargs):
            if celex == "32014L0089":
                raise requests.exceptions.HTTPError("500 error")
            return pd.DataFrame(
                [
                    {
                        "act_celex": celex,
                        "nim_celex": "72016L2284DNK_270999",
                        "national_measure_id": "270999",
                        "nim_date": "2018-06-01",
                        "nim_title": "Bekendtgorelse om luftkvalitet",
                        "member_state_iso3": "DNK",
                        "member_state_name": "Denmark",
                        "eurlex_url": "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72016L2284DNK_270999",
                    }
                ]
            )

        nim_workflow_module.get_national_transpositions_by_celex_ws = fake_fetch
        try:
            result = nim_workflow_module._retrieve_nim_rows(
                pd.DataFrame(
                    [
                        {
                            "celex": "32014L0089",
                            "eu_act_title": "Bad Directive",
                            "eu_act_type": "Directive",
                            "year": 2014,
                        },
                        {
                            "celex": "32016L2284",
                            "eu_act_title": "Good Directive",
                            "eu_act_type": "Directive",
                            "year": 2016,
                        },
                    ]
                ),
                {},
            )
        finally:
            nim_workflow_module.get_national_transpositions_by_celex_ws = original_fetch

        # The failing act must not appear, but the exception must not have
        # stopped processing of the remaining act either.
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["celex"], "32016L2284")

    def test_validate_source_config_requires_credentials(self) -> None:
        adapter = EurlexNIMAdapter()
        source = SourceConfig(name="eurlex-nim-source", adapter="eurlex-nim")
        original_user = os.environ.pop("EURLEX_WS_USER", None)
        original_pass = os.environ.pop("EURLEX_WS_PASS", None)
        original_legacy_user = os.environ.pop("EURLEX_USER", None)
        original_legacy_pass = os.environ.pop("EURLEX_WEB_PASS", None)
        try:
            with self.assertRaisesRegex(
                AdapterConfigError,
                "eurlex-nim adapter requires EUR-Lex WebService credentials",
            ):
                adapter.validate_source_config(source, base_path=Path("."))
        finally:
            if original_user is not None:
                os.environ["EURLEX_WS_USER"] = original_user
            if original_pass is not None:
                os.environ["EURLEX_WS_PASS"] = original_pass
            if original_legacy_user is not None:
                os.environ["EURLEX_USER"] = original_legacy_user
            if original_legacy_pass is not None:
                os.environ["EURLEX_WEB_PASS"] = original_legacy_pass

    def test_collect_supports_direct_celex_seed(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.workflow as nim_workflow_module

        original_retrieve = nim_workflow_module._retrieve_nim_rows
        original_batch = nim_workflow_module.batch_fetch_nim_fulltext
        original_user = os.environ.get("EURLEX_WS_USER")
        original_pass = os.environ.get("EURLEX_WS_PASS")

        def fake_retrieve(acts_df, settings):
            self.assertEqual(acts_df.iloc[0]["celex"], "32014L0089")
            return pd.DataFrame(
                [
                    {
                        "celex": "32014L0089",
                        "eu_act_title": "Directive Example",
                        "eu_act_type": "Directive",
                        "year": 2014,
                        "nim_celex": "72014L0089DNK_270540",
                        "national_measure_id": "270540",
                        "nim_date": "2016-06-01",
                        "nim_title": "Bekendtgorelse om havplanlaegning",
                        "nim_title_notice": "Bekendtgorelse om havplanlaegning",
                        "nim_title_lang": "da",
                        "member_state_iso3": "DNK",
                        "member_state_name": "Denmark",
                        "available_expr_langs3": "dan",
                        "available_langs": "DA",
                        "eurlex_url": "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72014L0089DNK_270540",
                        "nim_resource_uri": "http://publications.europa.eu/resource/nim/270540",
                    }
                ]
            )

        def fake_batch(df, **kwargs):
            self.assertEqual(df.iloc[0]["nim_celex"], "72014L0089DNK_270540")
            return pd.DataFrame(
                [
                    {
                        "celex": "32014L0089",
                        "nim_celex": "72014L0089DNK_270540",
                        "national_measure_id": "270540",
                        "text_source_url": "https://example.dk/measure.pdf",
                        "full_text_clean": "National measure full text.",
                        "full_text_raw": "",
                        "retrieval_status": 200,
                        "retrieval_error": "",
                        "fetch_seconds": 0.4,
                        "fetched_from_cache": False,
                        "lang": "da",
                        "lang_detected": "da",
                        "lang_source": "metadata",
                        "text_path": "cache/nim_text_cache/72014L0089DNK_270540.txt",
                        "route_used": "direct_text_pdf",
                        "text_route_used": "direct_text_pdf",
                        "content_type": "application/pdf",
                        "source_format": "pdf",
                        "available_languages": "da",
                        "page_title": "Bekendtgorelse om havplanlaegning",
                        "page_title_lang": "da",
                        "cache_key": "nim-cache-key",
                    }
                ]
            )

        nim_workflow_module._retrieve_nim_rows = fake_retrieve
        nim_workflow_module.batch_fetch_nim_fulltext = fake_batch
        os.environ["EURLEX_WS_USER"] = "demo-user"
        os.environ["EURLEX_WS_PASS"] = "demo-pass"
        try:
            adapter = EurlexNIMAdapter()
            source = SourceConfig(name="eurlex-nim-source", adapter="eurlex-nim")
            result = adapter.collect(
                source,
                query=Query(text="32014L0089", query_id="inline-001", origin="inline"),
                base_path=Path("."),
            )
        finally:
            nim_workflow_module._retrieve_nim_rows = original_retrieve
            nim_workflow_module.batch_fetch_nim_fulltext = original_batch
            if original_user is None:
                os.environ.pop("EURLEX_WS_USER", None)
            else:
                os.environ["EURLEX_WS_USER"] = original_user
            if original_pass is None:
                os.environ.pop("EURLEX_WS_PASS", None)
            else:
                os.environ["EURLEX_WS_PASS"] = original_pass

        self.assertEqual(len(result), 1)
        payload = result[0].payload
        self.assertEqual(payload["document_id"], "eurlex-nim-source:NIM:DNK:270540")
        self.assertEqual(payload["source_document_id"], "270540")
        self.assertEqual(payload["title"], "Bekendtgorelse om havplanlaegning")
        self.assertEqual(payload["document_type"], "national_implementation_measure")
        self.assertEqual(payload["jurisdiction"], "Denmark")
        self.assertEqual(payload["language"], "da")
        self.assertEqual(payload["publication_date"], "2016-06-01")
        self.assertEqual(payload["full_text"], "National measure full text.")
        self.assertEqual(payload["download_url"], "https://example.dk/measure.pdf")
        self.assertEqual(payload["raw_record"]["celex"], "32014L0089")
        self.assertEqual(payload["raw_record"]["nim_celex"], "72014L0089DNK_270540")
        self.assertEqual(payload["raw_record"]["route_used"], "direct_text_pdf")
        self.assertNotIn("document_id", payload["raw_record"])

        documents = normalize_adapter_results(
            result,
            source=source,
            query=Query(text="32014L0089", query_id="inline-001", origin="inline"),
        )
        self.assertEqual(documents[0].raw_metadata["_adapter_name"], "eurlex-nim")
        self.assertEqual(documents[0].raw_metadata["raw_record"]["national_measure_id"], "270540")
        self.assertNotIn("query_text", documents[0].raw_metadata["raw_record"])

    def test_collect_merges_gracefully_when_fulltext_returns_empty_frame(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.workflow as nim_workflow_module

        original_retrieve = nim_workflow_module._retrieve_nim_rows
        original_batch = nim_workflow_module.batch_fetch_nim_fulltext
        original_user = os.environ.get("EURLEX_WS_USER")
        original_pass = os.environ.get("EURLEX_WS_PASS")

        def fake_retrieve(acts_df, settings):
            return pd.DataFrame(
                [
                    {
                        "act_celex": "32014L0089",
                        "nim_celex": "72014L0089DNK_270540",
                        "national_measure_id": "270540",
                        "nim_date": "2016-06-01",
                        "nim_title": "Bekendtgorelse om havplanlaegning",
                        "member_state_iso3": "DNK",
                        "member_state_name": "Denmark",
                        "eurlex_url": "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72014L0089DNK_270540",
                    }
                ]
            )

        def fake_batch(df, **kwargs):
            return pd.DataFrame()

        nim_workflow_module._retrieve_nim_rows = fake_retrieve
        nim_workflow_module.batch_fetch_nim_fulltext = fake_batch
        os.environ["EURLEX_WS_USER"] = "demo-user"
        os.environ["EURLEX_WS_PASS"] = "demo-pass"
        try:
            adapter = EurlexNIMAdapter()
            source = SourceConfig(name="eurlex-nim-source", adapter="eurlex-nim")
            result = adapter.collect(
                source,
                query=Query(text="32014L0089", query_id="inline-001", origin="inline"),
                base_path=Path("."),
            )
        finally:
            nim_workflow_module._retrieve_nim_rows = original_retrieve
            nim_workflow_module.batch_fetch_nim_fulltext = original_batch
            if original_user is None:
                os.environ.pop("EURLEX_WS_USER", None)
            else:
                os.environ["EURLEX_WS_USER"] = original_user
            if original_pass is None:
                os.environ.pop("EURLEX_WS_PASS", None)
            else:
                os.environ["EURLEX_WS_PASS"] = original_pass

        self.assertEqual(len(result), 1)
        payload = result[0].payload
        self.assertEqual(payload["source_document_id"], "270540")
        self.assertEqual(payload["title"], "Bekendtgorelse om havplanlaegning")
        self.assertNotIn("full_text", payload)

    def test_collect_supports_query_seed_resolution(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.workflow as nim_workflow_module

        original_fetch = nim_workflow_module.fetch_eurlex_job
        original_build_tables = nim_workflow_module.build_eu_doc_tables
        original_select = nim_workflow_module.select_eligible_celex_acts
        original_retrieve = nim_workflow_module._retrieve_nim_rows
        original_user = os.environ.get("EURLEX_WS_USER")
        original_pass = os.environ.get("EURLEX_WS_PASS")

        def fake_fetch(*args, **kwargs):
            return [
                {
                    "source": "EU",
                    "scope": "ALL_ALL",
                    "lang": "en",
                    "term_group": "marine spatial planning",
                    "title": "Directive Example",
                    "celex": "32014L0089",
                    "date": "2014-07-23",
                    "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                }
            ]

        def fake_build_tables(df):
            return df, pd.DataFrame(
                [
                    {
                        "celex": "32014L0089",
                        "celex_full": "32014L0089",
                        "title": "Directive Example",
                    }
                ]
            )

        def fake_select(df):
            return pd.DataFrame(
                [
                    {
                        "celex": "32014L0089",
                        "eu_act_title": "Directive Example",
                        "eu_act_type": "Directive",
                        "year": 2014,
                    }
                ]
            )

        def fake_retrieve(acts_df, settings):
            self.assertEqual(list(acts_df["celex"]), ["32014L0089"])
            return pd.DataFrame(
                [
                    {
                        "celex": "32014L0089",
                        "eu_act_title": "Directive Example",
                        "eu_act_type": "Directive",
                        "year": 2014,
                        "nim_celex": "72014L0089DNK_270540",
                        "national_measure_id": "270540",
                        "nim_date": "2016-06-01",
                        "nim_title": "Bekendtgorelse om havplanlaegning",
                        "member_state_iso3": "DNK",
                        "member_state_name": "Denmark",
                        "eurlex_url": "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72014L0089DNK_270540",
                    }
                ]
            )

        nim_workflow_module.fetch_eurlex_job = fake_fetch
        nim_workflow_module.build_eu_doc_tables = fake_build_tables
        nim_workflow_module.select_eligible_celex_acts = fake_select
        nim_workflow_module._retrieve_nim_rows = fake_retrieve
        os.environ["EURLEX_WS_USER"] = "demo-user"
        os.environ["EURLEX_WS_PASS"] = "demo-pass"
        try:
            adapter = EurlexNIMAdapter()
            source = SourceConfig(
                name="eurlex-nim-source",
                adapter="eurlex-nim",
                settings={"fetch_full_text": False},
            )
            result = adapter.collect(
                source,
                query=Query(text="marine spatial planning", query_id="inline-001", origin="inline"),
                base_path=Path("."),
            )
        finally:
            nim_workflow_module.fetch_eurlex_job = original_fetch
            nim_workflow_module.build_eu_doc_tables = original_build_tables
            nim_workflow_module.select_eligible_celex_acts = original_select
            nim_workflow_module._retrieve_nim_rows = original_retrieve
            if original_user is None:
                os.environ.pop("EURLEX_WS_USER", None)
            else:
                os.environ["EURLEX_WS_USER"] = original_user
            if original_pass is None:
                os.environ.pop("EURLEX_WS_PASS", None)
            else:
                os.environ["EURLEX_WS_PASS"] = original_pass

        self.assertEqual(len(result), 1)
        payload = result[0].payload
        self.assertEqual(payload["source_document_id"], "270540")
        self.assertEqual(payload["title"], "Bekendtgorelse om havplanlaegning")
        self.assertEqual(
            payload["summary"],
            "National implementation measure for 32014L0089: Directive Example",
        )
        self.assertEqual(
            payload["url"],
            "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72014L0089DNK_270540",
        )
        self.assertNotIn("full_text", payload)

    def test_collect_can_skip_fulltext_and_limit_nim_rows(self) -> None:
        import policy_corpus_builder.adapters.eurlex_nim_supported.workflow as nim_workflow_module

        original_retrieve = nim_workflow_module._retrieve_nim_rows
        original_batch = nim_workflow_module.batch_fetch_nim_fulltext
        original_user = os.environ.get("EURLEX_WS_USER")
        original_pass = os.environ.get("EURLEX_WS_PASS")

        def fake_retrieve(acts_df, settings):
            return pd.DataFrame(
                [
                    {
                        "celex": "32014L0089",
                        "nim_celex": f"72014L0089DNK_{idx}",
                        "national_measure_id": str(idx),
                        "nim_date": "2016-06-01",
                        "nim_title": f"Measure {idx}",
                        "member_state_iso3": "DNK",
                        "member_state_name": "Denmark",
                        "eurlex_url": f"https://example.org/nim/{idx}",
                    }
                    for idx in (1, 2, 3)
                ]
            )

        def fake_batch(df, **kwargs):
            raise AssertionError("full-text batch should not run when fetch_full_text is false")

        nim_workflow_module._retrieve_nim_rows = fake_retrieve
        nim_workflow_module.batch_fetch_nim_fulltext = fake_batch
        os.environ["EURLEX_WS_USER"] = "demo-user"
        os.environ["EURLEX_WS_PASS"] = "demo-pass"
        try:
            adapter = EurlexNIMAdapter()
            source = SourceConfig(
                name="eurlex-nim-source",
                adapter="eurlex-nim",
                settings={
                    "fetch_full_text": False,
                    "nim_max_rows": 2,
                    "progress": True,
                },
            )
            result = adapter.collect(
                source,
                query=Query(text="32014L0089", query_id="inline-001", origin="inline"),
                base_path=Path("."),
            )
        finally:
            nim_workflow_module._retrieve_nim_rows = original_retrieve
            nim_workflow_module.batch_fetch_nim_fulltext = original_batch
            if original_user is None:
                os.environ.pop("EURLEX_WS_USER", None)
            else:
                os.environ["EURLEX_WS_USER"] = original_user
            if original_pass is None:
                os.environ.pop("EURLEX_WS_PASS", None)
            else:
                os.environ["EURLEX_WS_PASS"] = original_pass

        self.assertEqual(len(result), 2)
        self.assertEqual([item.payload["source_document_id"] for item in result], ["1", "2"])
        self.assertEqual(result[0].payload["title"], "Measure 1")
        self.assertNotIn("full_text", result[0].payload)

    def test_public_adapter_no_longer_imports_legacy_eurlex_nim_module(self) -> None:
        adapter_source = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "policy_corpus_builder"
            / "adapters"
            / "eurlex_nim_adapter.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("policy_corpus_builder.adapters.eurlex_nim import", adapter_source)


if __name__ == "__main__":
    unittest.main()
