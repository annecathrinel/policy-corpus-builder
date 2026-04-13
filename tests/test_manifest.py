import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.exporters import JSONL_FILENAME, MANIFEST_FILENAME  # noqa: E402
from policy_corpus_builder.orchestration import run_in_memory  # noqa: E402
from policy_corpus_builder.schemas import (  # noqa: E402
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)


class ManifestTests(unittest.TestCase):
    def test_manifest_is_created_during_normal_run(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/manifest-demo"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="placeholder-source", adapter="placeholder"),),
            normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            run_result = run_in_memory(
                config,
                base_path=base_path,
                write_exports=True,
                config_path=base_path / "config.toml",
            )
            manifest_path = (base_path / "outputs" / "manifest-demo" / MANIFEST_FILENAME).resolve()

            self.assertTrue(manifest_path.exists())
            self.assertIn(manifest_path, run_result.exported_paths)

    def test_manifest_contains_expected_fields(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/manifest-fields"),
            queries=QueriesConfig(items=("energy security", "resilience policy")),
            sources=(SourceConfig(name="placeholder-source", adapter="placeholder"),),
            normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            config_path = (base_path / "config.toml").resolve()
            run_in_memory(
                config,
                base_path=base_path,
                write_exports=True,
                config_path=config_path,
            )
            manifest_path = base_path / "outputs" / "manifest-fields" / MANIFEST_FILENAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(manifest["project_name"], "demo")
            self.assertEqual(manifest["config_path"], str(config_path))
            self.assertEqual(
                manifest["output_directory"],
                str((base_path / "outputs" / "manifest-fields").resolve()),
            )
            self.assertEqual(manifest["enabled_export_formats"], ["jsonl"])
            self.assertEqual(
                manifest["exported_files_written"],
                [JSONL_FILENAME, MANIFEST_FILENAME],
            )
            self.assertEqual(manifest["source_names_used"], ["placeholder-source"])
            self.assertEqual(manifest["query_source_type"], "items")
            self.assertEqual(manifest["query_count"], 2)
            self.assertEqual(manifest["raw_result_count"], 2)
            self.assertEqual(manifest["raw_normalized_document_count"], 2)
            self.assertEqual(manifest["final_document_count"], 2)
            self.assertEqual(manifest["duplicates_removed"], 0)
            self.assertIn("timestamp_utc", manifest)
            self.assertIn("tool_version", manifest)

    def test_manifest_is_stable_when_no_documents_are_returned(self) -> None:
        class EmptyAdapter:
            name = "empty-manifest"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(self, source: SourceConfig, query, *, base_path: Path) -> list:
                return []

        from policy_corpus_builder.adapters import register_adapter  # noqa: E402

        register_adapter(EmptyAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/manifest-empty"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="empty-source", adapter="empty-manifest"),),
            normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            run_in_memory(config, base_path=base_path, write_exports=True)
            manifest_path = base_path / "outputs" / "manifest-empty" / MANIFEST_FILENAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(manifest["raw_result_count"], 0)
            self.assertEqual(manifest["raw_normalized_document_count"], 0)
            self.assertEqual(manifest["final_document_count"], 0)
            self.assertEqual(manifest["duplicates_removed"], 0)

    def test_manifest_is_not_written_when_exports_are_disabled(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/manifest-disabled"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="placeholder-source", adapter="placeholder"),),
            normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            run_result = run_in_memory(config, base_path=base_path, write_exports=False)
            manifest_path = base_path / "outputs" / "manifest-disabled" / MANIFEST_FILENAME

            self.assertFalse(manifest_path.exists())
            self.assertEqual(run_result.exported_paths, tuple())


if __name__ == "__main__":
    unittest.main()
