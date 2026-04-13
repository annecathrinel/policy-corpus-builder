import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters import available_adapters, get_adapter, register_adapter  # noqa: E402
from policy_corpus_builder.adapters.base import AdapterResult  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.pipeline import NormalizationError, normalize_adapter_results  # noqa: E402
from policy_corpus_builder.queries import load_queries  # noqa: E402
from policy_corpus_builder.schemas import (  # noqa: E402
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)


class QueryAndPipelineTests(unittest.TestCase):
    def test_inventory_queries_are_loaded_and_normalized(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(inventory="queries.txt"),
            sources=(SourceConfig(name="demo-source", adapter="placeholder"),),
            normalization=NormalizationConfig(
                deduplicate=True,
                deduplicate_fields=("title",),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            (base_path / "queries.txt").write_text(
                textwrap.dedent(
                    """
                    # comment
                    energy security

                    resilience policy
                    """
                ).strip(),
                encoding="utf-8",
            )

            queries = load_queries(config, base_path=base_path)

        self.assertEqual(
            [query.text for query in queries],
            ["energy security", "resilience policy"],
        )
        self.assertEqual([query.origin for query in queries], ["inventory", "inventory"])
        self.assertEqual(queries[0].query_id, "inventory-002")
        self.assertEqual(queries[0].source_path, "queries.txt")

    def test_inline_queries_are_loaded_and_normalized(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(items=("critical infrastructure", "civil preparedness")),
            sources=(SourceConfig(name="demo-source", adapter="placeholder"),),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        queries = load_queries(config, base_path=Path("."))

        self.assertEqual(
            [query.text for query in queries],
            ["critical infrastructure", "civil preparedness"],
        )
        self.assertEqual([query.query_id for query in queries], ["inline-001", "inline-002"])
        self.assertTrue(all(query.origin == "inline" for query in queries))
        self.assertTrue(all(query.source_path is None for query in queries))

    def test_adapter_registry_exposes_and_instantiates_placeholder(self) -> None:
        self.assertIn("placeholder", available_adapters())
        adapter = get_adapter("placeholder")
        self.assertEqual(adapter.name, "placeholder")

    def test_adapter_registry_allows_registration(self) -> None:
        class DemoAdapter:
            name = "demo"

            def validate_source_config(self, source: SourceConfig) -> None:
                return None

            def collect(self, source: SourceConfig, query: Query) -> list[AdapterResult]:
                return []

        register_adapter(DemoAdapter)
        self.assertIn("demo", available_adapters())
        self.assertEqual(get_adapter("demo").name, "demo")

    def test_placeholder_adapter_output_converts_to_normalized_document(self) -> None:
        source = SourceConfig(name="placeholder-source", adapter="placeholder")
        query = Query(text="energy security", query_id="inline-001", origin="inline")
        adapter = get_adapter("placeholder")

        raw_results = adapter.collect(source, query)
        documents = normalize_adapter_results(raw_results, source=source, query=query)

        self.assertEqual(len(documents), 1)
        document = documents[0]
        self.assertEqual(document.document_id, "placeholder-source:inline-001")
        self.assertEqual(document.source_name, "placeholder-source")
        self.assertEqual(document.query, "energy security")
        self.assertEqual(document.raw_metadata["_query_id"], "inline-001")
        self.assertEqual(document.raw_metadata["_query_origin"], "inline")

    def test_normalization_rejects_missing_document_id(self) -> None:
        source = SourceConfig(name="placeholder-source", adapter="placeholder")
        query = Query(text="energy security", query_id="inline-001", origin="inline")

        with self.assertRaisesRegex(
            NormalizationError,
            "Adapter result field 'document_id' must be a non-empty string",
        ):
            normalize_adapter_results(
                [AdapterResult(payload={"title": "Missing id"})],
                source=source,
                query=query,
            )


if __name__ == "__main__":
    unittest.main()
