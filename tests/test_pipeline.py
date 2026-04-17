import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters import available_adapters, get_adapter, register_adapter  # noqa: E402
from policy_corpus_builder.adapters.base import AdapterResult  # noqa: E402
from policy_corpus_builder.cli import main as cli_main  # noqa: E402
from policy_corpus_builder.models import Query  # noqa: E402
from policy_corpus_builder.orchestration import format_run_summary, run_from_config_path, run_in_memory  # noqa: E402
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
            execution_mode = "query-aware"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(
                self,
                source: SourceConfig,
                query: Query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list[AdapterResult]:
                return []

        register_adapter(DemoAdapter)
        self.assertIn("demo", available_adapters())
        self.assertEqual(get_adapter("demo").name, "demo")

    def test_orchestration_runs_multiple_sources_and_queries(self) -> None:
        class AltAdapter:
            name = "alt"
            execution_mode = "query-aware"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(
                self,
                source: SourceConfig,
                query: Query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list[AdapterResult]:
                return [
                    AdapterResult(
                        payload={
                            "document_id": f"{source.name}:{query.query_id}:alt",
                            "title": f"Alt result for {query.text}",
                        }
                    )
                ]

        register_adapter(AltAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(items=("energy security", "resilience policy")),
            sources=(
                SourceConfig(name="source-a", adapter="placeholder"),
                SourceConfig(name="source-b", adapter="alt"),
            ),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        run_result = run_in_memory(config, base_path=Path("."))

        self.assertEqual(run_result.summary.query_count, 2)
        self.assertEqual(run_result.summary.enabled_source_count, 2)
        self.assertEqual(run_result.summary.source_query_pairs, 4)
        self.assertEqual(run_result.summary.raw_result_count, 4)
        self.assertEqual(run_result.summary.raw_normalized_document_count, 4)
        self.assertEqual(run_result.summary.final_document_count, 4)
        self.assertEqual(run_result.summary.duplicates_removed, 0)
        self.assertEqual(len(run_result.documents), 4)

    def test_disabled_sources_are_skipped(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(
                SourceConfig(name="active-source", adapter="placeholder", enabled=True),
                SourceConfig(name="disabled-source", adapter="placeholder", enabled=False),
            ),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        run_result = run_in_memory(config, base_path=Path("."))

        self.assertEqual(run_result.summary.enabled_source_count, 1)
        self.assertEqual(run_result.summary.source_query_pairs, 1)
        self.assertEqual(run_result.summary.final_document_count, 1)
        self.assertEqual(run_result.documents[0].source_name, "active-source")

    def test_empty_adapter_results_are_supported(self) -> None:
        class EmptyAdapter:
            name = "empty"
            execution_mode = "query-aware"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(
                self,
                source: SourceConfig,
                query: Query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list[AdapterResult]:
                return []

        register_adapter(EmptyAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(items=("energy security", "resilience policy")),
            sources=(SourceConfig(name="empty-source", adapter="empty"),),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        run_result = run_in_memory(config, base_path=Path("."))

        self.assertEqual(run_result.summary.raw_result_count, 0)
        self.assertEqual(run_result.summary.raw_normalized_document_count, 0)
        self.assertEqual(run_result.summary.final_document_count, 0)
        self.assertEqual(run_result.documents, tuple())

    def test_invalid_adapter_output_fails_cleanly(self) -> None:
        class BrokenAdapter:
            name = "broken"
            execution_mode = "query-aware"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(
                self,
                source: SourceConfig,
                query: Query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list[AdapterResult]:
                return [AdapterResult(payload={"title": "No document id"})]

        register_adapter(BrokenAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="broken-source", adapter="broken"),),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        with self.assertRaisesRegex(
            NormalizationError,
            "Adapter result field 'document_id' must be a non-empty string",
        ):
            run_in_memory(config, base_path=Path("."))

    def test_run_from_config_path_supports_inventory_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            queries_dir = base_path / "queries"
            queries_dir.mkdir()
            (queries_dir / "inventory.txt").write_text(
                "energy security\nresilience policy\n",
                encoding="utf-8",
            )
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

            run_result = run_from_config_path(config_path)

        self.assertEqual(run_result.summary.query_count, 2)
        self.assertEqual(run_result.summary.final_document_count, 2)

    def test_run_summary_is_concise_and_readable(self) -> None:
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/demo"),
            queries=QueriesConfig(items=("energy security",)),
            sources=(SourceConfig(name="placeholder-source", adapter="placeholder"),),
            normalization=NormalizationConfig(
                deduplicate=False,
                deduplicate_fields=(),
            ),
            export=ExportConfig(formats=("jsonl",)),
        )

        run_result = run_in_memory(config, base_path=Path("."))
        summary = format_run_summary(run_result.summary)

        self.assertIn("Run completed successfully.", summary)
        self.assertIn("Project: demo", summary)
        self.assertIn("Documents after deduplication: 1", summary)

    def test_cli_run_reports_success_for_placeholder_pipeline(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        original_argv = sys.argv[:]

        try:
            sys.argv = [
                "policy-corpus-builder",
                "run",
                "--config",
                "examples/minimal.toml",
            ]
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = cli_main()
        finally:
            sys.argv = original_argv

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertIn("Run completed successfully.", stdout.getvalue())
        self.assertIn("Documents after deduplication: 3", stdout.getvalue())

    def test_placeholder_adapter_output_converts_to_normalized_document(self) -> None:
        source = SourceConfig(name="placeholder-source", adapter="placeholder")
        query = Query(text="energy security", query_id="inline-001", origin="inline")
        adapter = get_adapter("placeholder")

        raw_results = adapter.collect(source, query, base_path=Path("."))
        documents = normalize_adapter_results(raw_results, source=source, query=query)

        self.assertEqual(len(documents), 1)
        document = documents[0]
        self.assertEqual(document.document_id, "placeholder-source:inline-001")
        self.assertEqual(document.source_name, "placeholder-source")
        self.assertEqual(document.query, "energy security")
        self.assertEqual(document.raw_metadata["_query_id"], "inline-001")
        self.assertEqual(document.raw_metadata["_query_origin"], "inline")

    def test_normalization_promotes_full_text_when_present(self) -> None:
        source = SourceConfig(name="placeholder-source", adapter="placeholder")
        query = Query(text="energy security", query_id="inline-001", origin="inline")

        documents = normalize_adapter_results(
            [
                AdapterResult(
                    payload={
                        "document_id": "doc-1",
                        "title": "Document",
                        "full_text": "Cleaned full text body.",
                    }
                )
            ],
            source=source,
            query=query,
        )

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0].full_text, "Cleaned full text body.")

    def test_eurlex_normalization_slims_raw_metadata_and_lightly_cleans_full_text(self) -> None:
        source = SourceConfig(name="eurlex-source", adapter="eurlex")
        query = Query(
            text="nature restoration",
            query_id="inventory-001",
            origin="inventory",
            source_path="queries/inventory.txt",
        )

        documents = normalize_adapter_results(
            [
                AdapterResult(
                    payload={
                        "document_id": "eurlex-source:EU:52022AR4206",
                        "source_document_id": "52022AR4206",
                        "title": "Opinion of the European Committee of the Regions on the EU Nature Restoration Law",
                        "document_type": "Committee of the Regions opinion",
                        "language": "en",
                        "jurisdiction": "European Union",
                        "publication_date": "2023-02-09",
                        "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52022AR4206",
                        "download_url": "http://publications.europa.eu/resource/cellar/example/DOC_1",
                        "full_text": (
                            "C_2023157EN.01003801.xml\n\n"
                            "3.5.2023   \n\n"
                            "EN\n\n"
                            "Official Journal of the European Union\n\n\n"
                            "Opinion of the European Committee of the Regions on the EU Nature Restoration Law"
                        ),
                        "retrieved_at": "2026-04-17T10:01:01.411396Z",
                        "content_path": "cache/text_cache/52022AR4206.txt",
                        "raw_record": {
                            "celex": "52022AR4206",
                            "celex_full": "52022AR4206",
                            "query_langs": ["en"],
                            "date": "2023-02-09",
                            "text_path": "cache/text_cache/52022AR4206.txt",
                            "text_source_url": "http://publications.europa.eu/resource/cellar/example/DOC_1",
                            "retrieval_status": 200,
                            "route_used": "cache_resume",
                        },
                    }
                )
            ],
            source=source,
            query=query,
        )

        self.assertEqual(len(documents), 1)
        document = documents[0]
        self.assertFalse(document.full_text.startswith("C_2023157EN.01003801.xml"))
        self.assertTrue(document.full_text.startswith("3.5.2023"))
        self.assertIn("Official Journal of the European Union", document.full_text)
        self.assertEqual(
            document.raw_metadata,
            {
                "_query_id": "inventory-001",
                "_query_origin": "inventory",
                "_adapter_result_index": 0,
                "_adapter_name": "eurlex",
                "_query_source_path": "queries/inventory.txt",
                "raw_record": {
                    "celex": "52022AR4206",
                    "celex_full": "52022AR4206",
                    "query_langs": ["en"],
                    "text_path": "cache/text_cache/52022AR4206.txt",
                    "text_source_url": "http://publications.europa.eu/resource/cellar/example/DOC_1",
                    "retrieval_status": 200,
                    "route_used": "cache_resume",
                },
            },
        )

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
