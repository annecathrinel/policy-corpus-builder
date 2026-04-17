import os
import sys
import unittest
from pathlib import Path
import math
from tempfile import TemporaryDirectory

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.base import AdapterConfigError  # noqa: E402
from policy_corpus_builder.adapters.eurlex_adapter import EurlexAdapter  # noqa: E402
from policy_corpus_builder.adapters.eurlex_supported import batch_fetch_eurlex_fulltext  # noqa: E402
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


def _contains_nan(value: object) -> bool:
    if isinstance(value, dict):
        return any(_contains_nan(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_nan(item) for item in value)
    return isinstance(value, float) and math.isnan(value)


if __name__ == "__main__":
    unittest.main()
