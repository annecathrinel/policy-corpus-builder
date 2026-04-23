import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.models import NormalizedDocument  # noqa: E402
from policy_corpus_builder.orchestration import run_in_memory  # noqa: E402
from policy_corpus_builder.postprocess import (  # noqa: E402
    build_deduplication_key,
    clean_document_for_downstream_analysis,
    deduplicate_documents,
)
from policy_corpus_builder.schemas import (  # noqa: E402
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)


class DeduplicationTests(unittest.TestCase):
    def test_downstream_cleaning_standardizes_core_fields(self) -> None:
        document = NormalizedDocument(
            document_id="doc-1",
            source_name="eu-eurlex",
            title="  Proposal\n\nfor a   Regulation\xa0 ",
            document_type="Regulation",
            language="EN",
            jurisdiction="eu",
            publication_date="2024",
            effective_date="23.07.2014",
            full_text=(
                "Consolidated TEXT: 32024R1991 — EN — 01.01.2025\n\n"
                "02024R1991 — EN — 01.01.2025 — 001.001\n\n"
                "This text is meant purely as a documentation tool and has no legal effect. "
                "The Union's institutions do not assume any liability for its contents.\n\n"
                "Article 1\n\nPolicy text."
            ),
        )

        cleaned = clean_document_for_downstream_analysis(
            document,
            expected_jurisdiction_code="EU",
        )

        self.assertEqual(cleaned.title, "Proposal for a Regulation")
        self.assertEqual(cleaned.document_type, "eu_regulation")
        self.assertEqual(cleaned.language, "en")
        self.assertEqual(cleaned.jurisdiction, "European Union")
        self.assertEqual(cleaned.publication_date, "2024-01-01")
        self.assertEqual(cleaned.effective_date, "2014-07-23")
        self.assertEqual(cleaned.full_text, "Article 1\n\nPolicy text.")
        self.assertEqual(cleaned.raw_metadata["_original_title"], document.title)
        self.assertEqual(cleaned.raw_metadata["_publication_date_precision"], "year")
        self.assertEqual(cleaned.raw_metadata["_effective_date_precision"], "day")

    def test_downstream_cleaning_removes_obvious_schema_boilerplate_text(self) -> None:
        document = NormalizedDocument(
            document_id="doc-1",
            source_name="eurlex-nim-supported",
            document_type="national_implementation_measure",
            full_text=(
                "XML Schema XML Schema 15 October 2014 Table of contents "
                "Introduction Resources Introduction This document describes the XML Schema"
            ),
        )

        cleaned = clean_document_for_downstream_analysis(document)

        self.assertIsNone(cleaned.full_text)
        self.assertEqual(
            cleaned.raw_metadata["_full_text_removed_reason"],
            "source_boilerplate_or_empty_after_cleaning",
        )

    def test_exact_duplicates_are_removed(self) -> None:
        documents = (
            NormalizedDocument(document_id="doc-1", source_name="source-a", title="Same"),
            NormalizedDocument(document_id="doc-1", source_name="source-a", title="Same"),
            NormalizedDocument(document_id="doc-2", source_name="source-a", title="Other"),
        )

        result = deduplicate_documents(
            documents,
            config=NormalizationConfig(
                deduplicate=True,
                deduplicate_fields=("document_id", "source_name", "title"),
            ),
        )

        self.assertEqual(len(result.documents), 2)
        self.assertEqual(result.duplicates_removed, 1)
        self.assertEqual(result.documents[0].document_id, "doc-1")

    def test_duplicates_are_removed_using_selected_fields(self) -> None:
        documents = (
            NormalizedDocument(document_id="doc-1", source_name="source-a", title="Shared", url="https://a"),
            NormalizedDocument(document_id="doc-2", source_name="source-b", title="Shared", url="https://a"),
        )

        result = deduplicate_documents(
            documents,
            config=NormalizationConfig(
                deduplicate=True,
                deduplicate_fields=("title", "url"),
            ),
        )

        self.assertEqual(len(result.documents), 1)
        self.assertEqual(result.documents[0].document_id, "doc-1")

    def test_deduplication_is_skipped_when_disabled(self) -> None:
        documents = (
            NormalizedDocument(document_id="doc-1", source_name="source-a", title="Same"),
            NormalizedDocument(document_id="doc-1", source_name="source-a", title="Same"),
        )

        result = deduplicate_documents(
            documents,
            config=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=("document_id",),
            ),
        )

        self.assertEqual(result.documents, documents)
        self.assertEqual(result.duplicates_removed, 0)

    def test_deduplication_is_stable_across_repeated_runs(self) -> None:
        documents = (
            NormalizedDocument(document_id="doc-1", source_name="source-a", title="Same"),
            NormalizedDocument(document_id="doc-2", source_name="source-a", title="Same"),
            NormalizedDocument(document_id="doc-3", source_name="source-a", title="Other"),
        )
        config = NormalizationConfig(
            deduplicate=True,
            deduplicate_fields=("title",),
        )

        result_one = deduplicate_documents(documents, config=config)
        result_two = deduplicate_documents(documents, config=config)

        self.assertEqual(result_one.documents, result_two.documents)
        self.assertEqual(result_one.duplicates_removed, result_two.duplicates_removed)

    def test_missing_values_participate_as_none_in_deduplication_key(self) -> None:
        document = NormalizedDocument(document_id="doc-1", source_name="source-a", title=None)
        key = build_deduplication_key(document, ("title", "url"))

        self.assertEqual(key, (("title", None), ("url", None)))

        result = deduplicate_documents(
            (
                document,
                NormalizedDocument(document_id="doc-2", source_name="source-b", title=None),
            ),
            config=NormalizationConfig(
                deduplicate=True,
                deduplicate_fields=("title",),
            ),
        )

        self.assertEqual(len(result.documents), 1)
        self.assertEqual(result.documents[0].document_id, "doc-1")

    def test_deduplication_happens_before_jsonl_export(self) -> None:
        class DuplicateAdapter:
            name = "duplicate"
            execution_mode = "query-aware"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(
                self,
                source: SourceConfig,
                query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list:
                return [
                    type("Result", (), {"payload": {"document_id": "doc-1", "title": "Shared"}})(),
                    type("Result", (), {"payload": {"document_id": "doc-2", "title": "Shared"}})(),
                ]

        from policy_corpus_builder.adapters import register_adapter  # noqa: E402
        from policy_corpus_builder.exporters import JSONL_FILENAME  # noqa: E402

        register_adapter(DuplicateAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/dedup-export"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="duplicate-source", adapter="duplicate"),),
            normalization=NormalizationConfig(
                deduplicate=True,
                deduplicate_fields=("title",),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            run_result = run_in_memory(config, base_path=base_path, write_exports=True)
            output_path = base_path / "outputs" / "dedup-export" / JSONL_FILENAME
            lines = output_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(run_result.summary.raw_normalized_document_count, 2)
        self.assertEqual(run_result.summary.final_document_count, 1)
        self.assertEqual(run_result.summary.duplicates_removed, 1)
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["document_id"], "doc-1")


if __name__ == "__main__":
    unittest.main()
