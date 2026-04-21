import os
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.base import AdapterConfigError  # noqa: E402
from policy_corpus_builder.adapters.eurlex_nim_adapter import EurlexNIMAdapter  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.pipeline import normalize_adapter_results  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


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
