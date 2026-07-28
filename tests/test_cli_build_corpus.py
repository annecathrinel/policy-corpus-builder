import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder import CorpusBuildValidationError  # noqa: E402
from policy_corpus_builder.cli import main as cli_main  # noqa: E402


@dataclass(frozen=True)
class FakeBuildResult:
    final_corpus_path: Path
    manifest_path: Path
    final_document_count: int
    nim_corpus_path: Path | None = None
    outputs_path: Path = Path("outputs/demo")
    jurisdiction_log_paths: dict = field(default_factory=dict)


class BuildCorpusCliTests(unittest.TestCase):
    def test_build_corpus_cli_forwards_top_level_arguments(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=12,
            nim_corpus_path=Path("outputs/demo/nim/documents.jsonl"),
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "marine spatial planning",
                "offshore renewable energy",
                "--jurisdictions",
                "EU",
                "UK",
                "--outputs-path",
                "outputs/demo",
                "--include-translations",
                "--translated-terms",
                "planification de l'espace maritime",
                "energie renouvelable en mer",
                "--include-nim",
                "--no-nim-fulltext",
                "--nim-max-rows",
                "25",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        build_policy_corpus.assert_called_once_with(
            query_terms=["marine spatial planning", "offshore renewable energy"],
            jurisdictions=["EU", "UK"],
            outputs_path=Path("outputs/demo"),
            include_translations=True,
            translated_terms=[
                "planification de l'espace maritime",
                "energie renouvelable en mer",
            ],
            include_nim=True,
            include_nim_fulltext=False,
            nim_max_rows=25,
            max_jurisdiction_workers=None,
            non_eu_max_per_term=None,
            non_eu_max_workers=None,
            write_jurisdiction_logs=True,
        )
        self.assertIn("Corpus build completed successfully.", stdout.getvalue())
        # Use str(Path(...)) rather than a hardcoded separator so this assertion
        # passes on both Windows (dev machine) and POSIX (Linux HPC/CI).
        self.assertIn(f"Final corpus: {fake_result.final_corpus_path}", stdout.getvalue())
        self.assertIn(f"NIM corpus: {fake_result.nim_corpus_path}", stdout.getvalue())

    def test_build_corpus_cli_preserves_default_nim_fulltext(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "EU",
                "--outputs-path",
                "outputs/demo",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            build_policy_corpus.call_args.kwargs["include_nim_fulltext"],
            True,
        )
        self.assertEqual(build_policy_corpus.call_args.kwargs["translated_terms"], None)
        self.assertNotIn("NIM corpus:", stdout.getvalue())

    def test_build_corpus_cli_forwards_max_jurisdiction_workers(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "EU",
                "UK",
                "--outputs-path",
                "outputs/demo",
                "--max-jurisdiction-workers",
                "2",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            build_policy_corpus.call_args.kwargs["max_jurisdiction_workers"],
            2,
        )

    def test_build_corpus_cli_forwards_max_per_term(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "NZ",
                "--outputs-path",
                "outputs/demo",
                "--max-per-term",
                "1200",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            build_policy_corpus.call_args.kwargs["non_eu_max_per_term"],
            1200,
        )

    def test_build_corpus_cli_max_per_term_defaults_to_none(self) -> None:
        # None here means "let build_policy_corpus apply its own default
        # (500)" - the CLI itself doesn't hardcode a value, it just forwards
        # whatever argparse gives it.
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "NZ",
                "--outputs-path",
                "outputs/demo",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertIsNone(build_policy_corpus.call_args.kwargs["non_eu_max_per_term"])

    def test_build_corpus_cli_forwards_max_workers(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "CA",
                "--outputs-path",
                "outputs/demo",
                "--max-workers",
                "10",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            build_policy_corpus.call_args.kwargs["non_eu_max_workers"],
            10,
        )

    def test_build_corpus_cli_max_workers_defaults_to_none(self) -> None:
        # None here means "let build_policy_corpus apply its own default
        # (8)" - the CLI itself doesn't hardcode a value, it just forwards
        # whatever argparse gives it.
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "NZ",
                "--outputs-path",
                "outputs/demo",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertIsNone(build_policy_corpus.call_args.kwargs["non_eu_max_workers"])

    def test_build_corpus_cli_no_jurisdiction_logs_forwards_false(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "EU",
                "--outputs-path",
                "outputs/demo",
                "--no-jurisdiction-logs",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            build_policy_corpus.call_args.kwargs["write_jurisdiction_logs"],
            False,
        )

    def test_build_corpus_cli_defaults_to_writing_jurisdiction_logs(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "EU",
                "--outputs-path",
                "outputs/demo",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ) as build_policy_corpus:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            build_policy_corpus.call_args.kwargs["write_jurisdiction_logs"],
            True,
        )

    def test_build_corpus_cli_prints_jurisdiction_logs_location_when_present(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]
        fake_result = FakeBuildResult(
            final_corpus_path=Path("outputs/demo/final/documents.jsonl"),
            manifest_path=Path("outputs/demo/run-manifest.json"),
            final_document_count=3,
            outputs_path=Path("outputs/demo"),
            jurisdiction_log_paths={"EU": Path("outputs/demo/logs/eu.log")},
        )

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "EU",
                "--outputs-path",
                "outputs/demo",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                return_value=fake_result,
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertIn(f"Jurisdiction logs: {Path('outputs/demo/logs')}", stdout.getvalue())

    def test_build_corpus_cli_reports_builder_validation_errors(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]

        try:
            sys.argv = [
                "policy-corpus-builder",
                "build-corpus",
                "--query-terms",
                "energy",
                "--jurisdictions",
                "XX",
                "--outputs-path",
                "outputs/demo",
            ]
            with patch(
                "policy_corpus_builder.cli.build_policy_corpus",
                side_effect=CorpusBuildValidationError("Unsupported jurisdiction: XX"),
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn(
            "Corpus build failed: Unsupported jurisdiction: XX",
            stderr.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()
