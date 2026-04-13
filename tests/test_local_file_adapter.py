import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.local_file import LocalFileAdapter  # noqa: E402
from policy_corpus_builder.adapters.base import AdapterConfigError, AdapterDataError  # noqa: E402
from policy_corpus_builder.exporters import JSONL_FILENAME, MANIFEST_FILENAME  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.orchestration import run_from_config_path  # noqa: E402
from policy_corpus_builder.pipeline import normalize_adapter_results  # noqa: E402
from policy_corpus_builder.schemas import SourceConfig  # noqa: E402


class LocalFileAdapterTests(unittest.TestCase):
    def test_adapter_config_validation_requires_fixture_path(self) -> None:
        adapter = LocalFileAdapter()
        source = SourceConfig(name="fixture", adapter="local-file", settings={})

        with self.assertRaisesRegex(
            AdapterConfigError,
            "source.settings.path",
        ):
            adapter.validate_source_config(source, base_path=Path("."))

    def test_jsonl_fixture_loading_returns_records(self) -> None:
        adapter = LocalFileAdapter()

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            fixture_path = base_path / "records.jsonl"
            fixture_path.write_text(
                '\n'.join(
                    [
                        json.dumps({"id": "doc-1", "title": "First"}),
                        json.dumps({"id": "doc-2", "title": "Second"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            source = SourceConfig(
                name="fixture",
                adapter="local-file",
                settings={"path": "records.jsonl"},
            )

            records = adapter._load_records(source, base_path=base_path)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["id"], "doc-1")

    def test_json_fixture_loading_supports_records_wrapper(self) -> None:
        adapter = LocalFileAdapter()

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            fixture_path = base_path / "records.json"
            fixture_path.write_text(
                json.dumps({"records": [{"id": "doc-1", "title": "First"}]}),
                encoding="utf-8",
            )
            source = SourceConfig(
                name="fixture",
                adapter="local-file",
                settings={"path": "records.json"},
            )

            records = adapter._load_records(source, base_path=base_path)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["title"], "First")

    def test_collect_filters_records_by_query_field(self) -> None:
        adapter = LocalFileAdapter()
        query = Query(text="energy security", query_id="inline-001", origin="inline")

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            fixture_path = base_path / "records.jsonl"
            fixture_path.write_text(
                '\n'.join(
                    [
                        json.dumps({"id": "doc-1", "title": "Energy", "queries": ["energy security"]}),
                        json.dumps({"id": "doc-2", "title": "Other", "queries": ["other topic"]}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            source = SourceConfig(
                name="fixture",
                adapter="local-file",
                settings={"path": "records.jsonl", "query_field": "queries"},
            )

            results = adapter.collect(source, query, base_path=base_path)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].payload["document_id"], "fixture:doc-1")
        self.assertEqual(results[0].payload["source_document_id"], "doc-1")

    def test_adapter_output_normalizes_into_documents(self) -> None:
        adapter = LocalFileAdapter()
        query = Query(text="energy security", query_id="inline-001", origin="inline")

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            (base_path / "records.jsonl").write_text(
                json.dumps(
                    {
                        "id": "doc-1",
                        "title": "Energy Security Strategy",
                        "summary": "Strategy summary",
                        "document_type": "strategy",
                        "language": "en",
                        "url": "https://example.invalid/doc-1",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            source = SourceConfig(
                name="fixture",
                adapter="local-file",
                settings={"path": "records.jsonl"},
            )

            results = adapter.collect(source, query, base_path=base_path)
            documents = normalize_adapter_results(results, source=source, query=query)

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0].document_id, "fixture:doc-1")
        self.assertEqual(documents[0].title, "Energy Security Strategy")
        self.assertEqual(documents[0].summary, "Strategy summary")
        self.assertEqual(documents[0].query, "energy security")
        self.assertEqual(
            documents[0].raw_metadata["raw_record"]["url"],
            "https://example.invalid/doc-1",
        )

    def test_invalid_record_structure_raises_adapter_data_error(self) -> None:
        adapter = LocalFileAdapter()
        query = Query(text="energy security", query_id="inline-001", origin="inline")

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            (base_path / "records.jsonl").write_text(
                json.dumps({"title": "Missing id"}) + "\n",
                encoding="utf-8",
            )
            source = SourceConfig(
                name="fixture",
                adapter="local-file",
                settings={"path": "records.jsonl"},
            )

            with self.assertRaisesRegex(AdapterDataError, "require a non-empty string 'id'"):
                adapter.collect(source, query, base_path=base_path)

    def test_end_to_end_run_with_local_file_adapter_writes_exports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            fixtures_dir = base_path / "fixtures"
            fixtures_dir.mkdir()
            fixture_path = fixtures_dir / "records.jsonl"
            fixture_path.write_text(
                '\n'.join(
                    [
                        json.dumps(
                            {
                                "id": "doc-1",
                                "title": "Energy Security Strategy",
                                "summary": "Strategy summary",
                                "document_type": "strategy",
                                "language": "en",
                                "url": "https://example.invalid/doc-1",
                                "queries": ["energy security"],
                            }
                        ),
                        json.dumps(
                            {
                                "id": "doc-2",
                                "title": "National Resilience Plan",
                                "summary": "Plan summary",
                                "document_type": "plan",
                                "language": "en",
                                "url": "https://example.invalid/doc-2",
                                "queries": ["resilience policy"],
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            config_path = base_path / "config.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [project]
                    name = "fixture-run"
                    output_dir = "outputs/fixture-run"

                    [queries]
                    items = ["energy security", "resilience policy"]

                    [[sources]]
                    name = "fixture-source"
                    adapter = "local-file"

                    [sources.settings]
                    path = "fixtures/records.jsonl"
                    format = "jsonl"
                    query_field = "queries"

                    [normalization]
                    deduplicate = true
                    deduplicate_fields = ["title", "url"]

                    [export]
                    formats = ["jsonl"]
                    """
                ).strip(),
                encoding="utf-8",
            )

            run_result = run_from_config_path(config_path)
            output_dir = base_path / "outputs" / "fixture-run"
            jsonl_path = output_dir / JSONL_FILENAME
            manifest_path = output_dir / MANIFEST_FILENAME

            self.assertEqual(run_result.summary.final_document_count, 2)
            self.assertTrue(jsonl_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertIn("fixture-source", json.loads(manifest_path.read_text(encoding="utf-8"))["source_names_used"])


if __name__ == "__main__":
    unittest.main()
