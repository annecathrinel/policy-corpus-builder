import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
import pandas as pd
from policy_corpus_builder.adapters.eurlex_nim_supported.overview import write_nim_overview
from policy_corpus_builder.adapters.eurlex_nim_adapter import EurlexNIMAdapter
from policy_corpus_builder.adapters.eurlex_nim_supported import workflow, surface
from policy_corpus_builder.corpus_builder import _collect_normalized_documents
from policy_corpus_builder.models import Query
from policy_corpus_builder.schemas import SourceConfig


class OverviewTests(unittest.TestCase):
    def test_counts_deduplicate_keep_unknown_year_and_distinguish_failure(self):
        acts = pd.DataFrame({"celex": ["32014L0089", "32023L0958", "32018L2001"]})
        rows = pd.DataFrame([
            {"celex": "32014L0089", "national_measure_id": "1", "member_state_iso3": "DNK", "nim_date": "2020-01-01", "year": 2014},
            {"celex": "32014L0089", "national_measure_id": "1", "member_state_iso3": "DNK", "nim_date": "2020-01-01", "year": 2014},
            {"celex": "32014L0089", "national_measure_id": "2", "member_state_iso3": "DNK", "nim_date": "", "year": 2014},
        ])
        rows.attrs["discovery_statuses"] = [{"celex": "32018L2001", "discovery_status": "failed", "discovery_error": "HTTP 500"}]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            write_nim_overview(acts, rows, out)
            counts = pd.read_csv(out / "nim_by_act_country.csv")
            self.assertEqual(counts.query("celex == '32014L0089' and member_state_iso3 == 'DNK'").iloc[0].nim_count, 2)
            self.assertTrue(counts.query("celex == '32023L0958'").nim_count.eq(0).all())
            self.assertTrue(counts.query("celex == '32018L2001'").nim_count.isna().all())
            yearly = pd.read_csv(out / "nim_by_act_country_year.csv", keep_default_na=False)
            self.assertEqual(set(yearly.nim_year), {"2020", "unknown"})
            self.assertEqual(yearly.nim_count.sum(), 2)

    def test_all_seeds_discovered_and_overview_saved_before_first_fulltext(self):
        adapter = EurlexNIMAdapter()
        events = []
        def discover(acts, settings):
            celex = acts.iloc[0].celex
            events.append("discover:" + celex)
            return pd.DataFrame([{"celex": celex, "nim_celex": "7" + celex[1:] + "DNK_" + str(i),
                "national_measure_id": str(i), "member_state_iso3": "DNK", "member_state_name": "Denmark",
                "nim_date": "2020-01-01", "nim_title": "Measure"} for i in (1, 2)])
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "overview"
            def fetch(rows, **kwargs):
                self.assertEqual(len([e for e in events if e.startswith("discover:")]), 2)
                counts = pd.read_csv(out / "nim_by_act.csv")
                self.assertEqual(list(counts.nim_count), [2, 2])
                self.assertEqual(len(rows), 1)  # limit applies only after overview
                events.append("fulltext")
                raise RuntimeError("Interrupted fulltext")
            source = SourceConfig(name="test-nim", adapter="eurlex-nim", settings={"overview_dir": str(out), "cache_dir": str(Path(tmp)/"cache"), "nim_max_rows": 1})
            queries = tuple(Query(text=c, query_id=str(i), origin="test") for i,c in enumerate(("32014L0089", "32023L0958")))
            with patch.object(adapter, "validate_source_config"), patch.object(adapter, "_activate_webservice_credentials"), patch("policy_corpus_builder.corpus_builder.get_adapter", return_value=adapter), patch.object(workflow, "_retrieve_nim_rows", side_effect=discover), patch.object(workflow, "batch_fetch_nim_fulltext", side_effect=fetch):
                with self.assertRaisesRegex(RuntimeError, "Interrupted fulltext"):
                    _collect_normalized_documents(source, queries=queries, base_path=Path(tmp))
            self.assertTrue((out / "nim_by_act_country_year.csv").exists())
            self.assertEqual(events[-1], "fulltext")

    def test_nim_no_link_does_not_hit_eurlex_202_routes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(surface, "fetch_nim_page_metadata", return_value={"page_links": {}}):
            session = Mock()
            text, meta = surface.fetch_nim_document_text("72014L0089DNK_1", try_langs2=["DA"], session=session, file_cache_dir=Path(tmp))
            session.get.assert_not_called()
            self.assertEqual(text, "")
            self.assertEqual(meta["error"], "no_national_document_link")

    def test_nim_landing_page_download_is_fetched_before_accepting_html(self):
        links = {"page_links": {"national_website_links": [{"url": "https://law.test/act", "link_type": "national_website"}]}}
        responses = [
            {"status": 200, "text": "Landing page " * 100, "html": "<html>Landing</html>", "extra_links": [{"url": "https://law.test/act.pdf", "link_type": "direct_text_pdf"}]},
            {"status": 200, "text": "Actual PDF document " * 100, "source_format": "pdf"},
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(surface, "fetch_nim_page_metadata", return_value=links), patch.object(surface, "_fetch_text_from_candidate", side_effect=responses) as fetch:
            text, meta = surface.fetch_nim_document_text("72014L0089DNK_1", try_langs2=["DA"], file_cache_dir=Path(tmp))
            self.assertEqual(fetch.call_count, 2)
            self.assertTrue(text.startswith("Actual PDF"))
            self.assertEqual(meta["route_used"], "direct_text_pdf")

    def test_nim_national_failure_is_not_masked_by_eurlex(self):
        links = {"page_links": {"national_website_links": [{"url": "https://law.test/act", "link_type": "national_website"}]}}
        with tempfile.TemporaryDirectory() as tmp, patch.object(surface, "fetch_nim_page_metadata", return_value=links), patch.object(surface, "_fetch_text_from_candidate", return_value={"status": 403, "error": "HTTP 403"}):
            text, meta = surface.fetch_nim_document_text("72014L0089DNK_1", try_langs2=["DA"], file_cache_dir=Path(tmp))
            self.assertEqual(meta["error"], "HTTP 403")
            self.assertEqual(meta["route_used"], "national_website")


if __name__ == "__main__":
    unittest.main()
