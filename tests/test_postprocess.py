import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.models import NormalizedDocument  # noqa: E402
from policy_corpus_builder.orchestration import run_in_memory  # noqa: E402
from policy_corpus_builder.postprocess import build_deduplication_key, deduplicate_documents  # noqa: E402
from policy_corpus_builder.schemas import (  # noqa: E402
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)


class DeduplicationTests(unittest.TestCase):
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

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(self, source: SourceConfig, query, *, base_path: Path) -> list:
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
