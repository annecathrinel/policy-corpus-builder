import json
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.cli import main as cli_main  # noqa: E402
from policy_corpus_builder.exporters import JSONL_FILENAME, export_documents_jsonl  # noqa: E402
from policy_corpus_builder.models import NormalizedDocument  # noqa: E402
from policy_corpus_builder.orchestration import run_in_memory  # noqa: E402
from policy_corpus_builder.schemas import (  # noqa: E402
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)


class JsonlExportTests(unittest.TestCase):
    def test_export_documents_jsonl_writes_expected_records(self) -> None:
        documents = (
            NormalizedDocument(
                document_id="doc-1",
                source_name="source-a",
                title="First",
                query="energy security",
            ),
            NormalizedDocument(
                document_id="doc-2",
                source_name="source-a",
                title="Second",
                query="resilience policy",
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = export_documents_jsonl(documents, output_dir=Path(tmpdir))
            lines = output_path.read_text(encoding="utf-8").splitlines()

            self.assertEqual(output_path.name, JSONL_FILENAME)
            self.assertEqual(len(lines), 2)
            first_record = json.loads(lines[0])
            self.assertEqual(first_record["document_id"], "doc-1")
            self.assertEqual(first_record["source_name"], "source-a")
            self.assertEqual(first_record["query"], "energy security")
            self.assertIn("raw_metadata", first_record)

    def test_export_documents_jsonl_supports_empty_streams(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = export_documents_jsonl((), output_dir=Path(tmpdir))
            contents = output_path.read_text(encoding="utf-8")

            self.assertEqual(contents, "")

    def test_run_writes_jsonl_to_configured_output_directory(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo-run"),
            queries=QueriesConfig(items=("energy security", "resilience policy")),
            sources=(SourceConfig(name="placeholder-source", adapter="placeholder"),),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            run_result = run_in_memory(config, base_path=base_path, write_exports=True)
            output_path = (base_path / "outputs" / "demo-run" / JSONL_FILENAME).resolve()
            lines = output_path.read_text(encoding="utf-8").splitlines()

            self.assertEqual(run_result.exported_paths, (output_path,))
            self.assertTrue(output_path.exists())
            self.assertEqual(len(lines), 2)
            self.assertEqual(run_result.summary.exported_files, (JSONL_FILENAME,))

    def test_run_does_not_write_jsonl_when_not_enabled(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/no-jsonl"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="placeholder-source", adapter="placeholder"),),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("csv",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            run_result = run_in_memory(config, base_path=base_path, write_exports=True)
            output_path = base_path / "outputs" / "no-jsonl" / JSONL_FILENAME

            self.assertEqual(run_result.exported_paths, tuple())
            self.assertFalse(output_path.exists())
            self.assertEqual(run_result.summary.exported_files, tuple())

    def test_cli_run_writes_jsonl_when_enabled(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            queries_dir = base_path / "queries"
            queries_dir.mkdir()
            (queries_dir / "inventory.txt").write_text("energy security\n", encoding="utf-8")
            config_path = base_path / "config.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [project]
                    name = "demo"
                    output_dir = "outputs/demo"

                    [queries]
                    inventory = "queries/inventory.txt"

                    [[sources]]
                    name = "placeholder-source"
                    adapter = "placeholder"

                    [normalization]
                    deduplicate = true
                    deduplicate_fields = ["title"]

                    [export]
                    formats = ["jsonl"]
                    """
                ).strip(),
                encoding="utf-8",
            )

            try:
                sys.argv = [
                    "policy-corpus-builder",
                    "run",
                    "--config",
                    str(config_path),
                ]
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
            finally:
                sys.argv = original_argv

            output_path = base_path / "outputs" / "demo" / JSONL_FILENAME
            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertTrue(output_path.exists())
            self.assertIn("Exported files: documents.jsonl", stdout.getvalue())

    def test_cli_run_skips_jsonl_when_not_enabled(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            config_path = base_path / "config.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [project]
                    name = "demo"
                    output_dir = "outputs/demo"

                    [queries]
                    items = ["energy security"]

                    [[sources]]
                    name = "placeholder-source"
                    adapter = "placeholder"

                    [normalization]
                    deduplicate = false
                    deduplicate_fields = []

                    [export]
                    formats = ["csv"]
                    """
                ).strip(),
                encoding="utf-8",
            )

            try:
                sys.argv = [
                    "policy-corpus-builder",
                    "run",
                    "--config",
                    str(config_path),
                ]
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
            finally:
                sys.argv = original_argv

            output_path = base_path / "outputs" / "demo" / JSONL_FILENAME
            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertFalse(output_path.exists())
            self.assertIn("Exported files: none", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
