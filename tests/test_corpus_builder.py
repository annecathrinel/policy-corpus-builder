import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.eurlex_nim_supported.surface import (  # noqa: E402
    build_eligible_act_table,
    normalize_eligible_legal_act_celex,
    select_eligible_celex_acts,
)
from policy_corpus_builder.corpus_builder import (  # noqa: E402
    CorpusBuildValidationError,
    build_policy_corpus,
)
from policy_corpus_builder.exporters import JSONL_FILENAME  # noqa: E402


class _FakeAdapter:
    execution_mode = "query-aware"

    def validate_source_config(self, source, *, base_path):
        return None


class _FakeEurlexAdapter(_FakeAdapter):
    name = "eurlex"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        self._tracker["eu_queries"].append(query.text)
        return [
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": "eu-eurlex:EU:32014L0089",
                        "source_document_id": "32014L0089",
                        "title": f"EU hit for {query.text}",
                        "document_type": "directive",
                        "language": "en",
                        "jurisdiction": "European Union",
                        "publication_date": "2014-07-23",
                        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                        "download_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                        "full_text": f"Full text for {query.text}",
                        "raw_record": {"celex": "32014L0089", "celex_full": "32014L0089"},
                    }
                },
            )()
        ]


class _MixedFakeEurlexAdapter(_FakeAdapter):
    name = "eurlex"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        self._tracker["eu_queries"].append(query.text)
        return [
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": "eu-eurlex:EU:32014L0089",
                        "source_document_id": "32014L0089",
                        "title": f"Eligible EU hit for {query.text}",
                        "document_type": "directive",
                        "language": "en",
                        "jurisdiction": "European Union",
                        "publication_date": "2014-07-23",
                        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                        "download_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32014L0089",
                        "full_text": f"Eligible full text for {query.text}",
                        "raw_record": {"celex": "32014L0089", "celex_full": "32014L0089"},
                    }
                },
            )(),
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": "eu-eurlex:EU:52022AR4206",
                        "source_document_id": "52022AR4206",
                        "title": f"Ineligible EU hit for {query.text}",
                        "document_type": "opinion",
                        "language": "en",
                        "jurisdiction": "European Union",
                        "publication_date": "2023-02-09",
                        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52022AR4206",
                        "download_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52022AR4206",
                        "full_text": f"Ineligible full text for {query.text}",
                        "raw_record": {"celex": "52022AR4206", "celex_full": "52022AR4206"},
                    }
                },
            )(),
        ]


class _IneligibleOnlyFakeEurlexAdapter(_FakeAdapter):
    name = "eurlex"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        self._tracker["eu_queries"].append(query.text)
        return [
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": "eu-eurlex:EU:52022AR4206",
                        "source_document_id": "52022AR4206",
                        "title": f"Ineligible EU hit for {query.text}",
                        "document_type": "opinion",
                        "language": "en",
                        "jurisdiction": "European Union",
                        "publication_date": "2023-02-09",
                        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52022AR4206",
                        "download_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52022AR4206",
                        "full_text": f"Ineligible full text for {query.text}",
                        "raw_record": {"celex": "52022AR4206", "celex_full": "52022AR4206"},
                    }
                },
            )()
        ]


class _ReferenceEligibilityFakeEurlexAdapter(_FakeAdapter):
    name = "eurlex"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        self._tracker["eu_queries"].append(query.text)
        rows = [
            ("52022AR4206", "Committee of the Regions opinion"),
            ("52023PC0304", "Commission proposal"),
            ("32024R1991", "Nature Restoration Regulation"),
        ]
        return [
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": f"eu-eurlex:EU:{celex}",
                        "source_document_id": celex,
                        "title": title,
                        "document_type": "eu_document",
                        "language": "en",
                        "jurisdiction": "European Union",
                        "publication_date": "2024-01-01",
                        "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
                        "download_url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
                        "full_text": title,
                        "raw_record": {"celex": celex, "celex_full": celex},
                    }
                },
            )()
            for celex, title in rows
        ]


class _FakeNonEUAdapter(_FakeAdapter):
    name = "non-eu"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        country = source.settings["countries"][0]
        self._tracker["non_eu_queries"].append((country, query.text))
        return [
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": f"{source.name}:{country.lower()}-doc-1",
                        "source_document_id": f"{country.lower()}-doc-1",
                        "title": f"{country} hit for {query.text}",
                        "document_type": "policy_document",
                        "language": "en",
                        "jurisdiction": country,
                        "publication_date": "2024",
                        "url": f"https://example.org/{country.lower()}/doc-1",
                        "download_url": f"https://example.org/{country.lower()}/doc-1.txt",
                        "full_text": f"{country} full text",
                        "raw_record": {"source": country},
                    }
                },
            )()
        ]


class _FakeNIMAdapter(_FakeAdapter):
    name = "eurlex-nim"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        self._tracker["nim_queries"].append(query.text)
        self._tracker.setdefault("nim_settings", []).append(dict(source.settings))
        return [
            type(
                "Result",
                (),
                {
                    "payload": {
                        "document_id": "eu-nim:NIM:DNK:270540",
                        "source_document_id": "270540",
                        "title": "Bekendtgorelse om havplanlaegning",
                        "summary": "National implementation measure for 32014L0089",
                        "document_type": "national_implementation_measure",
                        "language": "da",
                        "jurisdiction": "Denmark",
                        "publication_date": "2016-06-01",
                        "url": "https://eur-lex.europa.eu/legal-content/DA/TXT/?uri=CELEX:72014L0089DNK_270540",
                        "download_url": "https://example.org/nim/270540.pdf",
                        "full_text": "National implementation measure full text.",
                        "raw_record": {
                            "celex": query.text,
                            "nim_celex": "72014L0089DNK_270540",
                            "national_measure_id": "270540",
                        },
                    }
                },
            )()
        ]


class _StrictFakeNIMAdapter(_FakeAdapter):
    name = "eurlex-nim"

    def __init__(self, tracker):
        self._tracker = tracker

    def collect(self, source, query, *, base_path, loaded_source=None):
        if query.text not in {"32014L0089", "32024R1991"}:
            raise AssertionError(f"ineligible CELEX leaked into NIM runtime path: {query.text}")
        return _FakeNIMAdapter(self._tracker).collect(
            source,
            query,
            base_path=base_path,
            loaded_source=loaded_source,
        )


class PolicyCorpusBuilderTests(unittest.TestCase):
    def _build_fake_get_adapter(self, tracker):
        return self._build_custom_fake_get_adapter(tracker, eurlex_adapter_class=_FakeEurlexAdapter)

    def _build_custom_fake_get_adapter(self, tracker, *, eurlex_adapter_class, nim_adapter_class=_FakeNIMAdapter):
        def _fake_get_adapter(adapter_name):
            if adapter_name == "eurlex":
                return eurlex_adapter_class(tracker)
            if adapter_name == "non-eu":
                return _FakeNonEUAdapter(tracker)
            if adapter_name == "eurlex-nim":
                return nim_adapter_class(tracker)
            raise KeyError(adapter_name)

        return _fake_get_adapter

    def test_build_policy_corpus_writes_intermediates_final_manifest_and_separate_nim(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            stdout = StringIO()
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_fake_get_adapter(tracker),
            ), redirect_stdout(stdout):
                result = build_policy_corpus(
                    query_terms=["marine spatial planning"],
                    jurisdictions=["EU", "UK"],
                    outputs_path=output_root,
                    include_translations=True,
                    translated_terms=["planification marine"],
                    include_nim=True,
                )

            eu_intermediate = output_root / "jurisdictions" / "eu" / JSONL_FILENAME
            uk_intermediate = output_root / "jurisdictions" / "uk" / JSONL_FILENAME
            final_corpus = output_root / "final" / JSONL_FILENAME
            nim_corpus = output_root / "nim" / JSONL_FILENAME
            manifest_path = output_root / "run-manifest.json"

            self.assertEqual(result.final_corpus_path, final_corpus)
            self.assertEqual(result.nim_corpus_path, nim_corpus)
            self.assertEqual(result.manifest_path, manifest_path)
            self.assertEqual(result.schema_version, "1.0")
            self.assertEqual(result.query_terms, ("marine spatial planning",))
            self.assertEqual(result.selected_jurisdictions, ("EU", "UK"))
            self.assertEqual(result.include_translations, True)
            self.assertEqual(result.translated_terms, ("planification marine",))
            self.assertEqual(result.include_nim, True)
            self.assertEqual(result.include_nim_fulltext, True)
            self.assertIsNone(result.nim_max_rows)
            self.assertEqual(result.merged_document_count, 3)
            self.assertEqual(result.final_document_count, 2)
            self.assertEqual(result.duplicates_removed, 1)
            self.assertEqual(result.nim_status, "ran")
            self.assertEqual(result.nim_seed_count, 1)
            self.assertEqual(result.nim_eligible_seed_count, 1)
            self.assertEqual(result.nim_document_count, 1)
            self.assertEqual(len(result.jurisdiction_results), 2)
            self.assertEqual(result.jurisdiction_results[0].jurisdiction_code, "EU")
            self.assertEqual(result.jurisdiction_results[0].raw_hit_count, 2)
            self.assertEqual(result.jurisdiction_results[0].document_count, 2)
            self.assertEqual(result.jurisdiction_results[0].full_text_document_count, 2)
            self.assertEqual(result.jurisdiction_results[1].jurisdiction_code, "UK")
            self.assertEqual(result.jurisdiction_results[1].raw_hit_count, 1)
            self.assertEqual(result.jurisdiction_results[1].document_count, 1)
            self.assertEqual(result.jurisdiction_results[1].full_text_document_count, 1)

            self.assertTrue((output_root / "cache").exists())
            self.assertTrue(eu_intermediate.exists())
            self.assertTrue(uk_intermediate.exists())
            self.assertTrue(final_corpus.exists())
            self.assertTrue(nim_corpus.exists())
            self.assertTrue(manifest_path.exists())

            self.assertEqual(len(eu_intermediate.read_text(encoding="utf-8").splitlines()), 2)
            self.assertEqual(len(uk_intermediate.read_text(encoding="utf-8").splitlines()), 1)
            self.assertEqual(len(final_corpus.read_text(encoding="utf-8").splitlines()), 2)
            self.assertEqual(len(nim_corpus.read_text(encoding="utf-8").splitlines()), 1)

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            result_payload = result.to_dict()
            self.assertEqual(result_payload["schema_version"], "1.0")
            self.assertEqual(result_payload["query_terms"], ["marine spatial planning"])
            self.assertEqual(result_payload["selected_jurisdictions"], ["EU", "UK"])
            self.assertEqual(result_payload["include_translations"], True)
            self.assertEqual(result_payload["include_nim"], True)
            self.assertEqual(result_payload["include_nim_fulltext"], True)
            self.assertIsNone(result_payload["nim_max_rows"])
            self.assertEqual(manifest["selected_jurisdictions"], ["EU", "UK"])
            self.assertEqual(manifest["query_terms"], ["marine spatial planning"])
            self.assertEqual(manifest["translated_terms"], ["planification marine"])
            self.assertEqual(manifest["merged_document_count"], 3)
            self.assertEqual(manifest["final_document_count"], 2)
            self.assertEqual(manifest["duplicates_removed"], 1)
            self.assertEqual(manifest["nim_status"], "ran")
            self.assertEqual(manifest["nim_seed_count"], 1)
            self.assertEqual(manifest["nim_eligible_seed_count"], 1)
            self.assertEqual(manifest["nim_document_count"], 1)
            self.assertEqual(manifest["per_jurisdiction_output_paths"]["EU"], str(eu_intermediate))
            self.assertEqual(manifest["jurisdictions"][0]["jurisdiction_code"], "EU")
            self.assertEqual(manifest["jurisdictions"][0]["raw_hit_count"], 2)
            self.assertEqual(manifest["jurisdictions"][0]["document_count"], 2)
            self.assertEqual(manifest["jurisdictions"][0]["full_text_document_count"], 2)
            self.assertEqual(manifest["jurisdictions"][1]["jurisdiction_code"], "UK")
            self.assertEqual(manifest["jurisdictions"][1]["raw_hit_count"], 1)
            self.assertEqual(manifest["jurisdictions"][1]["document_count"], 1)
            self.assertEqual(manifest["jurisdictions"][1]["full_text_document_count"], 1)
            progress = stdout.getvalue()
            self.assertIn("Starting build_policy_corpus: validating inputs.", progress)
            self.assertIn("Starting jurisdiction EU.", progress)
            self.assertIn("Running jurisdiction EU. Total hits: 2.", progress)
            self.assertIn("Finished jurisdiction UK. Unique full-text documents retrieved: 1.", progress)
            self.assertIn("Running NIM from EU CELEX results.", progress)
            self.assertIn("Number of NIM eligible EU acts: 1.", progress)
            self.assertIn("Merging jurisdiction corpora and deduplicating final corpus.", progress)
            self.assertIn("Final corpus: 2 unique documents (1 duplicates removed).", progress)
            self.assertIn("Completed build_policy_corpus: 2 final documents written.", progress)
            self.assertEqual(
                tracker["eu_queries"],
                ["marine spatial planning", "planification marine"],
            )
            self.assertEqual(tracker["non_eu_queries"], [("UK", "marine spatial planning")])
            self.assertEqual(tracker["nim_queries"], ["32014L0089"])

    def test_translated_terms_only_affect_eu_path(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_fake_get_adapter(tracker),
            ):
                build_policy_corpus(
                    query_terms=["biodiversity"],
                    jurisdictions=["UK"],
                    outputs_path=Path(tmpdir) / "corpus-output",
                    include_translations=True,
                    translated_terms=["biodiversite"],
                    include_nim=False,
                )

        self.assertEqual(tracker["eu_queries"], [])
        self.assertEqual(tracker["non_eu_queries"], [("UK", "biodiversity")])
        self.assertEqual(tracker["nim_queries"], [])

    def test_policy_corpus_build_result_exposes_stable_public_contract_without_nim(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_fake_get_adapter(tracker),
            ):
                result = build_policy_corpus(
                    query_terms=["resilience"],
                    jurisdictions=["US"],
                    outputs_path=output_root,
                )

        payload = result.to_dict()
        self.assertEqual(payload["schema_version"], "1.0")
        self.assertEqual(payload["query_terms"], ["resilience"])
        self.assertEqual(payload["selected_jurisdictions"], ["US"])
        self.assertEqual(payload["include_translations"], False)
        self.assertEqual(payload["translated_terms"], [])
        self.assertEqual(payload["include_nim"], False)
        self.assertEqual(payload["include_nim_fulltext"], True)
        self.assertEqual(payload["nim_max_rows"], None)
        self.assertEqual(payload["nim_corpus_path"], None)
        self.assertEqual(payload["merged_document_count"], 1)
        self.assertEqual(payload["final_document_count"], 1)
        self.assertEqual(payload["duplicates_removed"], 0)
        self.assertEqual(payload["nim_status"], "not_requested")
        self.assertEqual(payload["nim_seed_count"], 0)
        self.assertEqual(payload["nim_eligible_seed_count"], 0)
        self.assertEqual(payload["jurisdictions"][0]["jurisdiction_code"], "US")
        self.assertEqual(payload["jurisdictions"][0]["document_count"], 1)

    def test_nim_runtime_controls_are_passed_to_top_level_nim_source(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_fake_get_adapter(tracker),
            ):
                result = build_policy_corpus(
                    query_terms=["marine spatial planning"],
                    jurisdictions=["EU"],
                    outputs_path=output_root,
                    include_nim=True,
                    include_nim_fulltext=False,
                    nim_max_rows=5,
                )

        self.assertEqual(result.include_nim_fulltext, False)
        self.assertEqual(result.nim_max_rows, 5)
        self.assertEqual(tracker["nim_settings"][0]["fetch_full_text"], False)
        self.assertEqual(tracker["nim_settings"][0]["nim_max_rows"], 5)
        self.assertEqual(tracker["nim_settings"][0]["progress"], True)

    def test_nim_seeding_filters_ineligible_eu_celexs_from_mixed_result_set(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            stdout = StringIO()
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_custom_fake_get_adapter(
                    tracker,
                    eurlex_adapter_class=_MixedFakeEurlexAdapter,
                ),
            ), redirect_stdout(stdout):
                result = build_policy_corpus(
                    query_terms=["marine spatial planning"],
                    jurisdictions=["EU"],
                    outputs_path=output_root,
                    include_nim=True,
                )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(result.nim_status, "ran")
        self.assertEqual(result.nim_seed_count, 2)
        self.assertEqual(result.nim_eligible_seed_count, 1)
        self.assertEqual(tracker["nim_queries"], ["32014L0089"])
        self.assertEqual(manifest["nim_status"], "ran")
        self.assertEqual(manifest["nim_seed_count"], 2)
        self.assertEqual(manifest["nim_eligible_seed_count"], 1)
        self.assertIn("Running NIM from EU CELEX results.", stdout.getvalue())

    def test_nim_skips_cleanly_when_no_eligible_eu_legal_act_celexs_exist(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            stdout = StringIO()
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_custom_fake_get_adapter(
                    tracker,
                    eurlex_adapter_class=_IneligibleOnlyFakeEurlexAdapter,
                ),
            ), redirect_stdout(stdout):
                result = build_policy_corpus(
                    query_terms=["nature restoration"],
                    jurisdictions=["EU"],
                    outputs_path=output_root,
                    include_nim=True,
                )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(result.nim_status, "skipped_no_eligible_eu_legal_acts")
        self.assertEqual(result.nim_seed_count, 1)
        self.assertEqual(result.nim_eligible_seed_count, 0)
        self.assertEqual(result.nim_document_count, 0)
        self.assertIsNone(result.nim_corpus_path)
        self.assertEqual(tracker["nim_queries"], [])
        self.assertEqual(manifest["nim_status"], "skipped_no_eligible_eu_legal_acts")
        self.assertEqual(manifest["nim_seed_count"], 1)
        self.assertEqual(manifest["nim_eligible_seed_count"], 0)
        self.assertEqual(manifest["nim_document_count"], 0)
        self.assertIn(
            "Skipping NIM: EU results contained no eligible legal-act CELEX seeds.",
            stdout.getvalue(),
        )

    def test_nim_eligibility_matches_reference_examples(self):
        self.assertEqual(normalize_eligible_legal_act_celex("52022AR4206"), "")
        self.assertEqual(normalize_eligible_legal_act_celex("52023PC0304"), "")
        self.assertEqual(normalize_eligible_legal_act_celex("32024R1991"), "32024R1991")

    def test_supported_eligible_act_tables_filter_to_l_r_d_descriptors(self):
        docs = pd.DataFrame(
            [
                {"celex": "52022AR4206", "title": "Opinion"},
                {"celex": "52023PC0304", "title": "Proposal"},
                {"celex": "32024R1991", "title": "Regulation"},
                {"celex": "32014L0089", "title": "Directive"},
                {"celex": "32022D0591", "title": "Decision"},
            ]
        )

        built = build_eligible_act_table(docs)
        selected = select_eligible_celex_acts(docs)

        self.assertEqual(set(built["celex"]), {"32024R1991", "32014L0089", "32022D0591"})
        self.assertEqual(set(selected["celex"]), {"32024R1991", "32014L0089", "32022D0591"})
        self.assertNotIn("52022AR4206", set(selected["celex"]))
        self.assertNotIn("52023PC0304", set(selected["celex"]))

    def test_top_level_nim_seeding_uses_reference_eligibility_for_mixed_eu_results(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            stdout = StringIO()
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_custom_fake_get_adapter(
                    tracker,
                    eurlex_adapter_class=_ReferenceEligibilityFakeEurlexAdapter,
                    nim_adapter_class=_StrictFakeNIMAdapter,
                ),
            ), redirect_stdout(stdout):
                result = build_policy_corpus(
                    query_terms=["nature restoration"],
                    jurisdictions=["EU"],
                    outputs_path=output_root,
                    include_nim=True,
                )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(result.nim_status, "ran")
        self.assertEqual(result.nim_seed_count, 3)
        self.assertEqual(result.nim_eligible_seed_count, 1)
        self.assertEqual(tracker["nim_queries"], ["32024R1991"])
        self.assertEqual(manifest["nim_eligible_seed_count"], 1)
        self.assertIn("Running NIM from EU CELEX results.", stdout.getvalue())

    def test_runtime_nim_path_defensively_blocks_bad_seeds_even_if_earlier_filter_regresses(self):
        tracker = {"eu_queries": [], "non_eu_queries": [], "nim_queries": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "corpus-output"
            stdout = StringIO()
            with patch(
                "policy_corpus_builder.corpus_builder.get_adapter",
                side_effect=self._build_custom_fake_get_adapter(
                    tracker,
                    eurlex_adapter_class=_IneligibleOnlyFakeEurlexAdapter,
                    nim_adapter_class=_StrictFakeNIMAdapter,
                ),
            ), patch(
                "policy_corpus_builder.corpus_builder._filter_eligible_nim_celex_seeds",
                return_value=("52022AR4206",),
            ), redirect_stdout(stdout):
                result = build_policy_corpus(
                    query_terms=["nature restoration"],
                    jurisdictions=["EU"],
                    outputs_path=output_root,
                    include_nim=True,
                )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(result.nim_status, "skipped_no_eligible_eu_legal_acts")
        self.assertEqual(result.nim_document_count, 0)
        self.assertIsNone(result.nim_corpus_path)
        self.assertEqual(tracker["nim_queries"], [])
        self.assertEqual(manifest["nim_status"], "skipped_no_eligible_eu_legal_acts")
        self.assertIn(
            "Skipping NIM: EU results contained no eligible legal-act CELEX seeds.",
            stdout.getvalue(),
        )

    def test_invalid_jurisdiction_fails_validation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(CorpusBuildValidationError, "Unsupported jurisdiction"):
                build_policy_corpus(
                    query_terms=["energy security"],
                    jurisdictions=["DK"],
                    outputs_path=Path(tmpdir) / "corpus-output",
                )

    def test_empty_query_terms_fail_validation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(CorpusBuildValidationError, "query_terms"):
                build_policy_corpus(
                    query_terms=[],
                    jurisdictions=["EU"],
                    outputs_path=Path(tmpdir) / "corpus-output",
                )


if __name__ == "__main__":
    unittest.main()
