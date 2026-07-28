import os
import sys
import threading
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch
import math
from tempfile import TemporaryDirectory

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.base import AdapterConfigError  # noqa: E402
from policy_corpus_builder.adapters.eurlex_adapter import EurlexAdapter  # noqa: E402
from policy_corpus_builder.adapters.eurlex_supported import batch_fetch_eurlex_fulltext  # noqa: E402
from policy_corpus_builder.adapters.eurlex_supported import _cache_path_for_celex  # noqa: E402
from policy_corpus_builder.adapters.eurlex_supported import fetch_eurlex_fulltext_for_row  # noqa: E402
from policy_corpus_builder.adapters.eurlex_supported import merge_and_save_fulltext_cache  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.pipeline import normalize_adapter_results  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


class EurlexAdapterTests(unittest.TestCase):
    def test_adapter_uses_supported_helper_module(self) -> None:
        import policy_corpus_builder.adapters.eurlex_adapter as eurlex_adapter_module

        self.assertEqual(
            eurlex_adapter_module.fetch_eurlex_job.__module__,
            "policy_corpus_builder.adapters.eurlex_supported",
        )

    def test_full_text_resolution_prefers_cleaned_then_raw(self) -> None:
        import policy_corpus_builder.adapters.eurlex_adapter as eurlex_adapter_module

        self.assertEqual(
            eurlex_adapter_module._resolve_full_text(
                {"full_text_clean": "Cleaned text", "full_text_raw": "<html>Raw text</html>"}
            ),
            "Cleaned text",
        )
        self.assertEqual(
            eurlex_adapter_module._resolve_full_text(
                {"full_text_clean": "", "full_text_raw": "<html>Raw text</html>"}
            ),
            "<html>Raw text</html>",
        )

    def test_cached_clean_text_normalizes_stale_xml_header_artifacts(self) -> None:
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        text = (
            "C_2023157EN.01003801.xml\n\n"
            "3.5.2023\n\n"
            "EN\n\n"
            "Official Journal of the European Union\n\n"
            "Opinion of the European Committee of the Regions on the EU Nature Restoration Law"
        )

        cleaned = eurlex_supported_module._normalize_cached_clean_text(text)

        self.assertFalse(cleaned.startswith("C_2023157EN.01003801.xml"))
        self.assertTrue(cleaned.startswith("3.5.2023"))
        self.assertIn("Official Journal of the European Union", cleaned)

    def test_validate_source_config_requires_credentials(self) -> None:
        adapter = EurlexAdapter()
        source = SourceConfig(name="eurlex-source", adapter="eurlex")
        original_user = os.environ.pop("EURLEX_WS_USER", None)
        original_pass = os.environ.pop("EURLEX_WS_PASS", None)
        original_legacy_user = os.environ.pop("EURLEX_USER", None)
        original_legacy_pass = os.environ.pop("EURLEX_WEB_PASS", None)
        try:
            with self.assertRaisesRegex(
                AdapterConfigError,
                "eurlex adapter requires EUR-Lex WebService credentials",
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

    def test_batch_fetch_rehydrates_successful_cached_rows_on_resume(self) -> None:
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        with TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir)
            text_cache_dir = cache_dir / "text_cache"
            text_cache_dir.mkdir(parents=True, exist_ok=True)

            text_value = (
                "C_2023157EN.01003801.xml\n\n"
                "Official Journal of the European Union\n\n"
                + ("Cached EUR-Lex full text. " * 30)
            )
            text_path = text_cache_dir / "32014L0089.txt"
            text_path.write_text(text_value, encoding="utf-8")
            merge_and_save_fulltext_cache(
                cache_dir,
                [
                    {
                        "celex_full": "32014L0089",
                        "celex": "32014L0089",
                        "celex_version": "",
                        "lang": "en",
                        "full_text_clean": text_value,
                        "text_source_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32014L0089",
                        "retrieval_status": 200,
                        "retrieval_error": "",
                        "text_len": len(text_value),
                    }
                ],
            )

            original_fetch = eurlex_supported_module.fetch_eurlex_fulltext_for_row

            def fail_fetch(*args, **kwargs):
                raise AssertionError("resume path should not refetch successful cached EUR-Lex rows")

            eurlex_supported_module.fetch_eurlex_fulltext_for_row = fail_fetch
            try:
                result = batch_fetch_eurlex_fulltext(
                    pd.DataFrame(
                        [
                            {
                                "celex_full": "32014L0089",
                                "celex": "32014L0089",
                                "celex_version": "",
                                "title": "Directive Example",
                                "url_fix": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                                "query_langs": '["en"]',
                            }
                        ]
                    ),
                    cache_dir=cache_dir,
                    use_cache=True,
                    verbose=False,
                    resume=True,
                )
            finally:
                eurlex_supported_module.fetch_eurlex_fulltext_for_row = original_fetch

            self.assertEqual(len(result), 1)
            row = result.to_dict(orient="records")[0]
            self.assertNotIn("C_2023157EN.01003801.xml", row["full_text_clean"])
            self.assertIn("Official Journal of the European Union", row["full_text_clean"])
            self.assertEqual(row["text_path"], str(text_path))
            self.assertEqual(
                row["text_source_url"],
                "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32014L0089",
            )
            self.assertTrue(row["fetched_from_cache"])
            self.assertEqual(row["retrieval_status"], 200)

    def test_collect_maps_supported_eurlex_workflow_rows(self) -> None:
        import policy_corpus_builder.adapters.eurlex_adapter as eurlex_adapter_module

        original_fetch = eurlex_adapter_module.fetch_eurlex_job
        original_build_tables = eurlex_adapter_module.build_eu_doc_tables
        original_filter = eurlex_adapter_module.filter_celex_types_for_fulltext
        original_batch = eurlex_adapter_module.batch_fetch_eurlex_fulltext
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
            docs = pd.DataFrame(
                [
                    {
                        "source": "EU",
                        "celex": "32014L0089",
                        "celex_full": "32014L0089",
                        "celex_version": "",
                        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                        "url_fix": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                        "title": "Directive Example",
                        "date": "2014-07-23",
                        "scopes": "[]",
                        "query_langs": '["en"]',
                        "query_term_groups": '["marine spatial planning"]',
                        "celex_descriptor_label": "Directive",
                        "fulltext_support": "supported",
                    }
                ]
            )
            return df, docs

        def fake_filter(df, *, mode):
            return df

        def fake_batch(df, **kwargs):
            return pd.DataFrame(
                [
                    {
                        "celex_full": "32014L0089",
                        "celex": "32014L0089",
                        "celex_version": "",
                        "text_source_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32014L0089",
                        "full_text_raw": "<html><body>Raw fallback text</body></html>",
                        "full_text_clean": "Full text of the directive.",
                        "retrieval_status": 200,
                        "retrieval_error": "",
                        "lang": "en",
                        "fetch_seconds": 0.1,
                        "fetched_from_cache": False,
                        "text_path": "cache/text_cache/32014L0089.txt",
                        "route_used": "cellar",
                        "content_type": float("nan"),
                        "celex_variant_used": float("nan"),
                        "fulltext_support": "supported",
                    }
                ]
            )

        eurlex_adapter_module.fetch_eurlex_job = fake_fetch
        eurlex_adapter_module.build_eu_doc_tables = fake_build_tables
        eurlex_adapter_module.filter_celex_types_for_fulltext = fake_filter
        eurlex_adapter_module.batch_fetch_eurlex_fulltext = fake_batch
        os.environ["EURLEX_WS_USER"] = "demo-user"
        os.environ["EURLEX_WS_PASS"] = "demo-pass"

        try:
            adapter = EurlexAdapter()
            source = SourceConfig(name="eurlex-source", adapter="eurlex")
            result = adapter.collect(
                source,
                query=Query(text="marine spatial planning", query_id="inline-001", origin="inline"),
                base_path=Path("."),
            )
        finally:
            eurlex_adapter_module.fetch_eurlex_job = original_fetch
            eurlex_adapter_module.build_eu_doc_tables = original_build_tables
            eurlex_adapter_module.filter_celex_types_for_fulltext = original_filter
            eurlex_adapter_module.batch_fetch_eurlex_fulltext = original_batch
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
        self.assertEqual(payload["document_id"], "eurlex-source:EU:32014L0089")
        self.assertEqual(payload["source_document_id"], "32014L0089")
        self.assertEqual(payload["title"], "Directive Example")
        self.assertEqual(payload["document_type"], "Directive")
        self.assertEqual(payload["jurisdiction"], "European Union")
        self.assertEqual(payload["full_text"], "Full text of the directive.")
        self.assertEqual(
            payload["download_url"],
            "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32014L0089",
        )
        self.assertNotIn("fulltext_support_x", payload["raw_record"])
        self.assertNotIn("fulltext_support_y", payload["raw_record"])
        self.assertEqual(payload["raw_record"]["fulltext_support"], "supported")
        self.assertEqual(payload["raw_record"]["query_langs"], ["en"])
        self.assertEqual(payload["raw_record"]["query_term_groups"], ["marine spatial planning"])
        self.assertEqual(payload["raw_record"]["scopes"], [])
        self.assertIsNone(payload["raw_record"].get("content_type"))
        self.assertNotIn("document_id", payload["raw_record"])
        self.assertNotIn("title", payload["raw_record"])

        documents = normalize_adapter_results(
            result,
            source=source,
            query=Query(text="marine spatial planning", query_id="inline-001", origin="inline"),
        )
        self.assertEqual(documents[0].full_text, "Full text of the directive.")
        self.assertFalse(_contains_nan(documents[0].to_dict()))


class _FakePostResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        pass


class _FakePostSession:
    def __init__(self, response: _FakePostResponse) -> None:
        self._response = response

    def post(self, *args, **kwargs):
        return self._response


class BatchFetchEurlexFulltextConcurrencyTests(unittest.TestCase):
    # Regression tests for a 2026-07-28 change: batch_fetch_eurlex_fulltext
    # used to fetch one document at a time in a plain sequential loop -
    # EU was consistently the slowest jurisdiction in every live run, and
    # this loop made zero use of the multiple threads/cores available
    # (unlike non-EU's add_full_texts_parallel, which already used a
    # ThreadPoolExecutor). These tests confirm real concurrency is
    # happening (not just that a max_workers parameter is silently
    # ignored) and that results are collected correctly regardless of
    # completion order.

    def _row(self, celex: str) -> dict:
        return {
            "celex_full": celex,
            "celex": celex,
            "celex_version": "",
            "title": f"Directive {celex}",
            "url_fix": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
            "query_langs": '["en"]',
        }

    def test_batch_fetch_eurlex_fulltext_runs_fetches_concurrently(self) -> None:
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        n_workers = 3
        # A barrier with parties=n_workers only releases once n_workers
        # threads have all called wait() - if batch_fetch_eurlex_fulltext
        # were still sequential (or capped below n_workers), this would
        # never reach n_workers simultaneous callers and the test would
        # time out (barrier.wait raises BrokenBarrierError on timeout
        # instead of hanging forever).
        barrier = threading.Barrier(n_workers, timeout=5)

        def fake_fetch(row, **kwargs):
            barrier.wait()
            celex = row["celex_full"]
            return {
                "celex": celex,
                "celex_full": celex,
                "celex_version": "",
                "title": row["title"],
                "url": row["url_fix"],
                "text_source_url": "https://eur-lex.europa.eu/...",
                "full_text_raw": "",
                "full_text_clean": "Full text.",
                "text_len": len("Full text."),
                "retrieval_status": 200,
                "retrieval_error": "",
                "lang": "en",
                "lang_source_fulltext": "",
                "fetch_seconds": 0.01,
                "fetched_from_cache": False,
                "text_path": "",
                "celex_variant_used": "",
                "route_used": "cellar",
                "content_type": "",
                "attempt_trace": [],
            }

        with TemporaryDirectory() as cache_dir:
            with patch.object(eurlex_supported_module, "fetch_eurlex_fulltext_for_row", side_effect=fake_fetch):
                result = batch_fetch_eurlex_fulltext(
                    pd.DataFrame([self._row(f"3201{i}L000{i}") for i in range(n_workers)]),
                    cache_dir=Path(cache_dir),
                    use_cache=False,
                    resume=False,
                    verbose=False,
                    max_workers=n_workers,
                )

        self.assertEqual(len(result), n_workers)
        self.assertEqual(
            set(result["celex_full"]),
            {f"3201{i}L000{i}" for i in range(n_workers)},
        )

    def test_batch_fetch_eurlex_fulltext_defaults_to_4_workers(self) -> None:
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        captured_max_workers = []
        original_executor = eurlex_supported_module.ThreadPoolExecutor

        def capturing_executor(*args, **kwargs):
            captured_max_workers.append(kwargs.get("max_workers", args[0] if args else None))
            return original_executor(*args, **kwargs)

        def fake_fetch(row, **kwargs):
            return {**self._row(row["celex_full"]), "full_text_clean": "", "text_len": 0}

        with TemporaryDirectory() as cache_dir:
            with (
                patch.object(eurlex_supported_module, "fetch_eurlex_fulltext_for_row", side_effect=fake_fetch),
                patch.object(eurlex_supported_module, "ThreadPoolExecutor", side_effect=capturing_executor),
            ):
                batch_fetch_eurlex_fulltext(
                    pd.DataFrame([self._row("32014L0089")]),
                    cache_dir=Path(cache_dir),
                    use_cache=False,
                    resume=False,
                    verbose=False,
                )

        self.assertEqual(captured_max_workers, [4])

    def test_worker_threads_write_to_the_jurisdiction_log_target_not_real_stdout(self) -> None:
        # Regression test for a 2026-07-28 live report: the moment EU's
        # full-text fetch was parallelized, [EURLEX TEXT] lines started
        # leaking into the MAIN job output instead of staying in
        # logs/eu.log. Cause: corpus_builder.py's _JurisdictionLogRouter
        # routes each jurisdiction's prints via a threading.local()
        # target set on the ONE thread that calls into that
        # jurisdiction's collect() - new threads a jurisdiction spawns
        # itself (this function's own ThreadPoolExecutor workers) get
        # their own fresh threading.local() with no target set, so their
        # prints fell back to the real stdout. This exercises the real
        # _JurisdictionLogRouter (not a stand-in) with two rows and
        # max_workers=2, so two genuinely different worker threads each
        # have to resolve the correct target independently.
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module
        from policy_corpus_builder.corpus_builder import _JurisdictionLogRouter

        real_stdout = StringIO()
        jurisdiction_log = StringIO()
        router = _JurisdictionLogRouter(real_stdout)

        def fake_fetch(row, **kwargs):
            print(f"[EURLEX TEXT] fake line for {row['celex_full']}", flush=True)
            return {**self._row(row["celex_full"]), "full_text_clean": "Full text.", "text_len": 10}

        original_stdout = sys.stdout
        sys.stdout = router
        try:
            with router.redirect_to(jurisdiction_log):
                with TemporaryDirectory() as cache_dir:
                    with patch.object(eurlex_supported_module, "fetch_eurlex_fulltext_for_row", side_effect=fake_fetch):
                        batch_fetch_eurlex_fulltext(
                            pd.DataFrame([self._row("32014L0089"), self._row("32014L0090")]),
                            cache_dir=Path(cache_dir),
                            use_cache=False,
                            resume=False,
                            verbose=False,
                            max_workers=2,
                        )
        finally:
            sys.stdout = original_stdout

        self.assertIn("[EURLEX TEXT] fake line for 32014L0089", jurisdiction_log.getvalue())
        self.assertIn("[EURLEX TEXT] fake line for 32014L0090", jurisdiction_log.getvalue())
        self.assertEqual(real_stdout.getvalue(), "")


class EurlexTermLabelInSearchLogTests(unittest.TestCase):
    # Regression tests for a 2026-07-28 report: Cat asked for the EU log
    # to show which query term it's currently on, the way every non-EU
    # jurisdiction's own log does (e.g. fetch_nz_documents' repeated
    # "[NZ] term='...' page=..." lines) - EU's search-phase log
    # (fetch_eurlex_job/post_eurlex_ws) previously showed page numbers,
    # HTTP status, and hit counts with no indication of which of the
    # run's many query terms any of it belonged to.

    def test_post_eurlex_ws_prints_term_label_when_given(self) -> None:
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        session = _FakePostSession(_FakePostResponse(200, "<xml/>"))
        stdout = StringIO()
        with redirect_stdout(stdout):
            eurlex_supported_module.post_eurlex_ws(
                "<payload/>",
                session=session,
                debug=True,
                term_label="nature-based solution",
            )

        self.assertIn("term='nature-based solution' POST", stdout.getvalue())

    def test_post_eurlex_ws_omits_term_label_when_not_given(self) -> None:
        # No caller passes an empty term_label today (fetch_eurlex_job's
        # two call sites always pass term_group), but post_eurlex_ws is a
        # general-purpose SOAP helper - a future caller without a term
        # shouldn't get a blank "term='' " clause cluttering its output.
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        session = _FakePostSession(_FakePostResponse(200, "<xml/>"))
        stdout = StringIO()
        with redirect_stdout(stdout):
            eurlex_supported_module.post_eurlex_ws(
                "<payload/>",
                session=session,
                debug=True,
            )

        output = stdout.getvalue()
        self.assertIn("POST", output)
        self.assertNotIn("term=", output)

    def test_fetch_eurlex_job_prints_the_query_term_on_its_debug_lines(self) -> None:
        import policy_corpus_builder.adapters.eurlex_supported as eurlex_supported_module

        stdout = StringIO()
        with (
            patch.object(
                eurlex_supported_module,
                "post_eurlex_ws",
                return_value=("<xml/>", 200, "<xml/>"),
            ),
            patch.object(
                eurlex_supported_module,
                "parse_searchresults",
                return_value=(0, 0, [], 0),
            ),
            redirect_stdout(stdout),
        ):
            eurlex_supported_module.fetch_eurlex_job(
                {
                    "scope": "ALL_ALL",
                    "expert_scope": "TI_TE",
                    "lang": "en",
                    "terms": ["nature-based solution"],
                },
                debug=True,
            )

        output = stdout.getvalue()
        self.assertIn("terms=['nature-based solution']", output)
        self.assertIn("[EURLEX] term='nature-based solution' group 1/1", output)
        self.assertIn("[EURLEX] term='nature-based solution' trying page_size=", output)
        self.assertIn("[EURLEX] term='nature-based solution' page=1 totalhits=0", output)

    def test_run_eurlex_query_pipeline_prints_a_term_marker_before_full_text_fetch(self) -> None:
        import policy_corpus_builder.adapters.eurlex_adapter as eurlex_adapter_module

        fake_rows = [
            {
                "source": "EU",
                "scope": "ALL_ALL",
                "lang": "en",
                "term_group": "nature-based solution",
                "title": "Directive Example",
                "celex": "32014L0089",
                "date": "2014-01-01",
                "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
            }
        ]
        fake_fulltext_df = pd.DataFrame(
            [
                {
                    "celex_full": "32014L0089",
                    "celex": "32014L0089",
                    "celex_version": "",
                    "text_source_url": "",
                    "full_text_raw": "",
                    "full_text_clean": "",
                    "retrieval_status": 404,
                    "retrieval_error": "not_found",
                    "lang": "en",
                    "fetch_seconds": 0.0,
                    "fetched_from_cache": False,
                    "text_path": "",
                    "route_used": "cellar",
                    "content_type": "",
                }
            ]
        )
        stdout = StringIO()
        with (
            patch.object(eurlex_adapter_module, "fetch_eurlex_job", return_value=fake_rows),
            patch.object(
                eurlex_adapter_module,
                "batch_fetch_eurlex_fulltext",
                return_value=fake_fulltext_df,
            ),
            redirect_stdout(stdout),
        ):
            eurlex_adapter_module.run_eurlex_query_pipeline(
                "nature-based solution",
                source=SourceConfig(name="eu-eurlex", adapter="eurlex", settings={}),
                base_path=Path("."),
            )

        output = stdout.getvalue()
        self.assertIn(
            "[EURLEX] term='nature-based solution' starting full-text fetch for 1 document(s).",
            output,
        )


class FetchEurlexFulltextForRowVerboseLiveFetchTests(unittest.TestCase):
    # Regression tests for a 2026-07-28 live report: logs/eu.log stayed
    # completely empty for EU's entire (multi-hour) runtime even after
    # fixing the per-jurisdiction log file's buffering, because
    # run_eurlex_query_pipeline hardcoded debug=False/verbose=False -
    # unlike every non-EU jurisdiction's own fetch_* function
    # (verbose=True by default). Separately, even with verbose=True,
    # fetch_eurlex_fulltext_for_row's cache-hit branch already printed
    # per-document progress, but its live-fetch branch (the actually
    # slow path someone tailing the log while EU is still running cares
    # about) never printed anything at all. These tests cover that
    # live-fetch branch directly.

    def _row(self) -> pd.Series:
        return pd.Series(
            {
                "celex_full": "32014L0089",
                "celex": "32014L0089",
                "celex_version": "",
                "title": "Directive Example",
                "url_fix": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                "query_langs": '["en"]',
            }
        )

    def test_verbose_prints_a_success_line_for_a_live_non_cached_fetch(self) -> None:
        with TemporaryDirectory() as cache_dir:
            fake_result = {
                "full_text_raw": "<html>Full text.</html>",
                "full_text_clean": "Full text.",
                "status": 200,
                "error": "",
                "final_url": "https://eur-lex.europa.eu/...",
                "route_used": "cellar",
                "lang": "en",
                "fetch_seconds": 0.4,
                "attempt_trace": [],
            }
            stdout = StringIO()
            with (
                patch(
                    "policy_corpus_builder.adapters.eurlex_supported.get_eurlex_text_multi",
                    return_value=fake_result,
                ),
                patch("policy_corpus_builder.adapters.eurlex_supported.time.sleep"),
                redirect_stdout(stdout),
            ):
                result = fetch_eurlex_fulltext_for_row(
                    self._row(),
                    cache_dir=Path(cache_dir),
                    use_cache=False,
                    verbose=True,
                    progress_label="1/1",
                )

        self.assertEqual(result["full_text_clean"], "Full text.")
        output = stdout.getvalue()
        self.assertIn("[EURLEX TEXT] 1/1 CELEX=32014L0089 success length=10 source=LIVE route=cellar", output)

    def test_verbose_prints_a_failure_line_for_a_live_fetch_that_returns_no_text(self) -> None:
        with TemporaryDirectory() as cache_dir:
            fake_result = {
                "full_text_raw": "",
                "full_text_clean": "",
                "status": 404,
                "error": "not_found",
                "final_url": "",
                "route_used": "cellar",
                "attempt_trace": [],
            }
            stdout = StringIO()
            with (
                patch(
                    "policy_corpus_builder.adapters.eurlex_supported.get_eurlex_text_multi",
                    return_value=fake_result,
                ),
                patch("policy_corpus_builder.adapters.eurlex_supported.time.sleep"),
                redirect_stdout(stdout),
            ):
                fetch_eurlex_fulltext_for_row(
                    self._row(),
                    cache_dir=Path(cache_dir),
                    use_cache=False,
                    verbose=True,
                    progress_label="1/1",
                )

        output = stdout.getvalue()
        self.assertIn("[EURLEX TEXT] 1/1 CELEX=32014L0089 FAILED status=404 error=not_found", output)

    def test_verbose_prints_the_full_consolidated_version_suffix_not_just_the_base_celex(self) -> None:
        # Regression test for a 2026-07-28 live report: logs/eu.log showed
        # the same base CELEX (e.g. 02014R0808, 02021R2115) printed 6-7
        # times in a row with different success lengths, which looked
        # exactly like the same document being wastefully re-fetched over
        # and over. It wasn't - each line was actually a distinct
        # consolidated version of the same base act (EUR-Lex tracks each
        # amendment date as its own document, with its own
        # "-YYYYMMDD"-suffixed celex_full), but the log was printing
        # celex (the base, with the version suffix already split off by
        # split_celex_identifier) instead of celex_full, silently hiding
        # exactly the detail that would have shown these were different
        # documents. This uses a celex_full that actually has a version
        # suffix (unlike self._row()'s unsuffixed fixture, which can't
        # distinguish "prints celex" from "prints celex_full" since
        # they're identical when there's no suffix).
        row = pd.Series(
            {
                "celex_full": "02014R0808-20210101",
                "celex": "02014R0808-20210101",
                "celex_version": "",
                "title": "Consolidated Regulation Example",
                "url_fix": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:02014R0808-20210101",
                "query_langs": '["en"]',
            }
        )
        with TemporaryDirectory() as cache_dir:
            fake_result = {
                "full_text_raw": "<html>Full text.</html>",
                "full_text_clean": "Full text.",
                "status": 200,
                "error": "",
                "final_url": "https://eur-lex.europa.eu/...",
                "route_used": "cellar",
                "lang": "en",
                "fetch_seconds": 0.4,
                "attempt_trace": [],
            }
            stdout = StringIO()
            with (
                patch(
                    "policy_corpus_builder.adapters.eurlex_supported.get_eurlex_text_multi",
                    return_value=fake_result,
                ),
                patch("policy_corpus_builder.adapters.eurlex_supported.time.sleep"),
                redirect_stdout(stdout),
            ):
                fetch_eurlex_fulltext_for_row(
                    row,
                    cache_dir=Path(cache_dir),
                    use_cache=False,
                    verbose=True,
                    progress_label="1/1",
                )

        output = stdout.getvalue()
        self.assertIn("CELEX=02014R0808-20210101", output)
        self.assertNotIn("CELEX=02014R0808 ", output)

    def test_verbose_prints_the_full_consolidated_version_suffix_on_a_cache_hit_too(self) -> None:
        # Same fix, same reasoning, for the cache-hit branch (which already
        # printed per-document progress before this fix - only the label
        # itself needed correcting from celex to celex_full).
        row = pd.Series(
            {
                "celex_full": "02014R0808-20210101",
                "celex": "02014R0808-20210101",
                "celex_version": "",
                "title": "Consolidated Regulation Example",
                "url_fix": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:02014R0808-20210101",
                "query_langs": '["en"]',
            }
        )
        with TemporaryDirectory() as cache_dir:
            text_cache_dir = Path(cache_dir) / "text_cache"
            text_cache_dir.mkdir(parents=True, exist_ok=True)
            cached_path = _cache_path_for_celex("02014R0808-20210101", text_cache_dir, "txt")
            cached_path.write_text("Cached full text.", encoding="utf-8")

            stdout = StringIO()
            with redirect_stdout(stdout):
                fetch_eurlex_fulltext_for_row(
                    row,
                    cache_dir=Path(cache_dir),
                    use_cache=True,
                    verbose=True,
                    progress_label="1/1",
                )

        output = stdout.getvalue()
        self.assertIn("CELEX=02014R0808-20210101", output)
        self.assertNotIn("CELEX=02014R0808 ", output)

    def test_verbose_false_suppresses_the_live_fetch_line(self) -> None:
        with TemporaryDirectory() as cache_dir:
            fake_result = {
                "full_text_raw": "<html>Full text.</html>",
                "full_text_clean": "Full text.",
                "status": 200,
                "error": "",
                "final_url": "https://eur-lex.europa.eu/...",
                "route_used": "cellar",
                "attempt_trace": [],
            }
            stdout = StringIO()
            with (
                patch(
                    "policy_corpus_builder.adapters.eurlex_supported.get_eurlex_text_multi",
                    return_value=fake_result,
                ),
                patch("policy_corpus_builder.adapters.eurlex_supported.time.sleep"),
                redirect_stdout(stdout),
            ):
                fetch_eurlex_fulltext_for_row(
                    self._row(),
                    cache_dir=Path(cache_dir),
                    use_cache=False,
                    verbose=False,
                    progress_label="1/1",
                )

        self.assertEqual(stdout.getvalue(), "")


class EurlexQueryPipelineVerbosityDefaultsTests(unittest.TestCase):
    def test_run_eurlex_query_pipeline_requests_verbose_search_and_fulltext_logging(self) -> None:
        # Regression test: fetch_eurlex_job's debug and
        # batch_fetch_eurlex_fulltext's verbose were both hardcoded to
        # False here, which is why logs/eu.log stayed empty for EU's
        # entire runtime even once the per-jurisdiction log file's
        # buffering was fixed - nothing was ever being printed in the
        # first place. Both should now default to on, matching every
        # non-EU jurisdiction's own fetch_* function.
        import policy_corpus_builder.adapters.eurlex_adapter as eurlex_adapter_module

        captured: dict[str, dict] = {}

        def fake_fetch_eurlex_job(job, *, fields, terms_per_query, **kwargs):
            captured["fetch_eurlex_job_kwargs"] = kwargs
            return []

        with patch.object(eurlex_adapter_module, "fetch_eurlex_job", side_effect=fake_fetch_eurlex_job):
            eurlex_adapter_module.run_eurlex_query_pipeline(
                "marine spatial planning",
                source=SourceConfig(name="eu", adapter="eurlex", settings={}),
                base_path=Path("."),
            )

        self.assertTrue(captured["fetch_eurlex_job_kwargs"]["debug"])


def _contains_nan(value: object) -> bool:
    if isinstance(value, dict):
        return any(_contains_nan(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_nan(item) for item in value)
    return isinstance(value, float) and math.isnan(value)


if __name__ == "__main__":
    unittest.main()
