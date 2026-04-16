import os
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.base import AdapterConfigError  # noqa: E402
from policy_corpus_builder.adapters.eurlex_adapter import EurlexAdapter  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


class EurlexAdapterTests(unittest.TestCase):
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
                        "full_text_clean": "Full text of the directive.",
                        "retrieval_status": 200,
                        "retrieval_error": "",
                        "lang": "en",
                        "fetch_seconds": 0.1,
                        "fetched_from_cache": False,
                        "text_path": "cache/text_cache/32014L0089.txt",
                        "route_used": "cellar",
                        "content_type": "text/html",
                        "celex_variant_used": "32014L0089",
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
                query=type("Query", (), {"text": "marine spatial planning"})(),
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


if __name__ == "__main__":
    unittest.main()
