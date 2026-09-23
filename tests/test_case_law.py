import unittest
import csv
import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from contextlib import redirect_stdout
from io import StringIO
import pandas as pd

from policy_corpus_builder.classification import classify_case_law
from policy_corpus_builder.models import NormalizedDocument
from policy_corpus_builder.postprocess import clean_document_for_downstream_analysis
from policy_corpus_builder.exporters.analysis_tables import export_case_law
from policy_corpus_builder.corpus_builder import build_policy_corpus, _CollectionResult


def document(celex=None, **kwargs):
    return NormalizedDocument(document_id=celex or "test", source_name="eu-eurlex",
                              source_document_id=celex, jurisdiction="EU", **kwargs)


class ClassificationTests(unittest.TestCase):
    def test_sector_six_descriptors(self):
        for descriptor in ("CJ", "TJ", "CO", "CC", "TC", "TO"):
            with self.subTest(descriptor=descriptor):
                decision = classify_case_law(document(f"62020{descriptor}0001"))
                self.assertTrue(decision.is_case_law)
                self.assertEqual(decision.category, "eu_case_law")
                self.assertEqual(decision.basis, "celex_sector_6")

    def test_exclusions_and_generic_labels(self):
        for celex in ("12003T/TXT", "02003T0000-20040501", "12003TN02/16/C", None):
            doc = document(celex, document_type="Tribunal case")
            self.assertFalse(classify_case_law(doc).is_case_law)
            self.assertNotEqual(clean_document_for_downstream_analysis(doc).document_type, "eu_case_law")
        for title in ("Treaty of accession", "Annex II - Environment and nature protection",
                      "Protocol annexed to the treaty", "Treaty amendment",
                      "Act concerning the conditions of accession", "Consolidated treaty text",
                      "Accession environment/nature-protection annex", "Act of accession"):
            self.assertFalse(classify_case_law(document("62020CJ0001", title=title)).is_case_law)

    def test_nested_and_conflicting_metadata(self):
        for metadata in ({"celex_sector": "0"}, {"celex_sector": 1},
                         {"celex_sector_label": "Treaties"}, {"celex_sector_label": "Consolidated texts"},
                         {"celex_class": "sector_1_treaties"}, {"celex_class": "sector_0_consolidated"}):
            doc = document("62020CJ0001", raw_metadata={"raw_record": metadata}, document_type="eu_case_law")
            self.assertFalse(classify_case_law(doc).is_case_law)
            self.assertNotEqual(clean_document_for_downstream_analysis(doc).document_type, "eu_case_law")
        self.assertTrue(classify_case_law(document(raw_metadata={"raw_record": {"celex": "62020CJ0001"}})).is_case_law)

    def test_sector_eight_and_non_case_types(self):
        self.assertEqual(classify_case_law(document("82020CJ0001")).category, "national_case_law_eu_reference")
        self.assertEqual(clean_document_for_downstream_analysis(document("32020L0001", document_type="Directive")).document_type, "eu_directive")
        self.assertTrue(classify_case_law(document("62020CJ0001", title="Judgment concerning a treaty amendment")).is_case_law)

    def test_non_eu_titles_are_insufficient(self):
        for jurisdiction in ("UK", "CA", "AUS", "NZ", "US"):
            doc = replace(document(title="Court judgment, tribunal opinion and order", document_type="court case"), jurisdiction=jurisdiction)
            self.assertFalse(classify_case_law(doc).is_case_law)


class ExportTests(unittest.TestCase):
    def test_deduplicated_counts_missing_year_and_metadata(self):
        first = document("62020CJ0001", publication_date="2020-01-02", full_text="Judgment text",
                         raw_metadata={"raw_record": {"court_or_source": "Court of Justice", "celex_sector": "6"}})
        docs = (first, first, document("62020CC0001", publication_date="2020-05-01"),
                document("82020CJ0001"), document("12003T/TXT", document_type="Tribunal case"))
        with TemporaryDirectory() as tmp:
            result = export_case_law(docs, output_dir=Path(tmp), jurisdictions=("EU", "CA"))
            self.assertEqual(result.status, "written")
            self.assertEqual(result.document_count, 3)
            records = [json.loads(line) for line in result.corpus_path.read_text().splitlines()]
            self.assertTrue(all(r["source_document_id"][0] in "68" for r in records))
            self.assertTrue(all(r["full_text"] is None for r in records))
            kept = next(r for r in records if r["source_document_id"] == first.source_document_id)
            self.assertEqual(kept["raw_metadata"]["raw_record"]["court_or_source"], "Court of Justice")
            with result.count_table_paths["by_jurisdiction_year"].open(encoding="utf-8-sig") as stream:
                counts = list(csv.DictReader(stream))
            self.assertEqual([(r["year"], r["case_count"], r["unique_source_document_count"]) for r in counts], [("2020", "2", "2"), ("unknown", "1", "1")])
            with result.count_table_paths["by_document_type"].open(encoding="utf-8-sig") as stream:
                self.assertEqual(sum(int(r["case_count"]) for r in csv.DictReader(stream)), 3)
            overview = json.loads(result.overview_path.read_text())
            self.assertEqual(overview["accepted_record_count"], 4)
            self.assertEqual(overview["rejected_record_count"], 1)
            self.assertEqual(overview["missing_year_count"], 1)
            self.assertEqual(overview["duplicates_removed"], 1)
            self.assertEqual(result.unsupported_jurisdictions, ("CA",))
            original = result.corpus_path.read_bytes()
            export_case_law(docs, output_dir=Path(tmp), jurisdictions=("EU", "CA"))
            self.assertEqual(original, result.corpus_path.read_bytes())
            export_case_law(docs, output_dir=Path(tmp), jurisdictions=("EU",), fulltext=True)
            self.assertIn("Judgment text", result.corpus_path.read_text())

    def test_empty_and_unsupported_outputs_have_headers_without_zero_rows(self):
        for jurisdictions, docs, expected in ((("EU",), (), "written_empty"),
                (("UK", "CA", "AUS", "NZ", "US"), (), "unsupported_jurisdictions"),
                (("EU",), (document("32020L0001"),), "no_reliable_records")):
            with TemporaryDirectory() as tmp:
                result = export_case_law(docs, output_dir=Path(tmp), jurisdictions=jurisdictions)
                self.assertEqual(result.status, expected)
                self.assertEqual(result.corpus_path.read_text(), "")
                self.assertEqual(len(result.count_table_paths["by_jurisdiction_year"].read_text().splitlines()), 1)


class BuilderTests(unittest.TestCase):
    def test_nim_overview_paths_and_date_setting_in_result_and_manifest(self):
        from policy_corpus_builder.adapters.eurlex_nim_supported.overview import write_nim_overview
        def nim(seeds, **kwargs):
            self.assertEqual(kwargs["nim_min_valid_year"], 1960)
            write_nim_overview(pd.DataFrame({"celex": seeds}), pd.DataFrame(),
                               kwargs["output_root"] / "nim" / "overview", min_valid_year=1960)
            return ()
        with TemporaryDirectory() as tmp, patch("policy_corpus_builder.corpus_builder._run_jurisdiction",
                return_value=_CollectionResult((document("32020L0001"),), 1)), patch(
                "policy_corpus_builder.corpus_builder._run_eu_nim", side_effect=nim):
            result = build_policy_corpus(["test"], ["EU"], tmp, include_nim=True,
                                         nim_min_valid_year=1960, include_case_law=True)
            self.assertEqual(len(result.nim_overview_paths), 7)
            self.assertTrue(all(p.is_file() for p in result.nim_overview_paths.values()))
            manifest = json.loads(result.manifest_path.read_text())
            self.assertEqual(manifest["nim_overview_paths"], result.to_dict()["nim_overview_paths"])
            self.assertEqual(manifest["nim_min_valid_year"], 1960)

    def test_cli_case_law_options_and_completion(self):
        from policy_corpus_builder.cli import main
        with TemporaryDirectory() as tmp, patch("policy_corpus_builder.corpus_builder._run_jurisdiction",
                return_value=_CollectionResult((document("62020CJ0001"),), 1)), patch("sys.argv", [
                "policy-corpus-builder", "build-corpus", "--query-terms", "test", "--jurisdictions", "EU", "UK",
                "--outputs-path", tmp, "--include-case-law", "--case-law-fulltext", "--nim-min-valid-year", "1960"]):
            stdout = StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(main(), 0)
            self.assertIn("Case-law status: written", stdout.getvalue())
            self.assertIn("UK: case-law retrieval unsupported", stdout.getvalue())
            self.assertIn("Case-law by_jurisdiction_year:", stdout.getvalue())
            manifest = json.loads((Path(tmp) / "run-manifest.json").read_text())
            self.assertTrue(manifest["case_law_fulltext"])
            self.assertEqual(manifest["nim_min_valid_year"], 1960)

    def test_result_and_manifest_states(self):
        for requested, jurisdictions, docs, expected in (
            (False, ["EU"], (document("62020CJ0001"),), "not_requested"),
            (True, ["EU"], (), "written_empty"),
            (True, ["EU"], (document("32020L0001"),), "no_reliable_records"),
            (True, ["UK"], (), "unsupported_jurisdictions"),
            (True, ["EU"], (document("62020CJ0001"),), "written"),
        ):
            with self.subTest(expected=expected), TemporaryDirectory() as tmp, patch(
                    "policy_corpus_builder.corpus_builder._run_jurisdiction", return_value=_CollectionResult(docs, len(docs))):
                result = build_policy_corpus(["test"], jurisdictions, tmp, include_case_law=requested)
                payload = result.to_dict()
                manifest = json.loads(result.manifest_path.read_text())
                self.assertEqual(payload["case_law_status"], expected)
                self.assertEqual(manifest["case_law_status"], expected)
                self.assertEqual(payload["case_law_corpus_path"], manifest["case_law_corpus_path"])
                if not requested:
                    self.assertIsNone(payload["case_law_document_count"])
                    self.assertFalse((Path(tmp) / "case_law").exists())
                else:
                    self.assertTrue(result.case_law_overview_path.is_file())
                    self.assertEqual(len(manifest["case_law_count_table_paths"]), 2)

    def test_failure_is_not_reported_as_empty(self):
        with TemporaryDirectory() as tmp, patch("policy_corpus_builder.corpus_builder._run_jurisdiction", side_effect=RuntimeError("discovery failed")):
            with self.assertRaises(RuntimeError):
                build_policy_corpus(["test"], ["EU"], tmp, include_case_law=True)
            manifest = json.loads((Path(tmp) / "run-manifest.json").read_text())
            self.assertEqual(manifest["case_law_status"], "failed")
            self.assertIsNone(manifest["case_law_document_count"])

    def test_validation(self):
        for options in ({"include_case_law": "yes"}, {"case_law_fulltext": True}, {"nim_min_valid_year": True}, {"nim_min_valid_year": 0}):
            with self.assertRaises(ValueError):
                build_policy_corpus(["test"], ["EU"], "unused", **options)


class AdapterTests(unittest.TestCase):
    def test_case_metadata_survives_fulltext_filter_skip_and_failure(self):
        from policy_corpus_builder.adapters import eurlex_adapter as adapter
        from policy_corpus_builder.schemas import SourceConfig
        for fulltext in (False, True):
            docs = pd.DataFrame([{"celex_full": c, "celex": c, "celex_version": "", "title": "Judgment", "date": "2020-01-01"}
                                 for c in ("62020CJ0001", "82020CJ0001")])
            def fetch(frame, **kwargs):
                self.assertEqual(len(frame), 2 if fulltext else 0)
                # No successful full-text rows; metadata must still survive.
                return pd.DataFrame()
            with TemporaryDirectory() as tmp, patch.object(adapter, "fetch_eurlex_job", return_value=[{}]), patch.object(
                    adapter, "build_eu_doc_tables", return_value=(pd.DataFrame(), docs)), patch.object(
                    adapter, "batch_fetch_eurlex_fulltext", side_effect=fetch):
                source = SourceConfig(name="eu", adapter="eurlex", settings={
                    "include_case_law": True, "case_law_fulltext": fulltext, "fulltext_mode": "sector_3_only"})
                records = adapter.run_eurlex_query_pipeline("test", source=source, base_path=Path(tmp))
                self.assertEqual(len(records), 2)
                self.assertEqual({r["celex_sector"] for r in records}, {"6", "8"})
                self.assertTrue(all(not r["full_text"] for r in records))


if __name__ == "__main__":
    unittest.main()
