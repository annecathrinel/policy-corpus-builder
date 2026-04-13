import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters import register_adapter  # noqa: E402
from policy_corpus_builder.adapters.base import AdapterResult  # noqa: E402
from policy_corpus_builder.orchestration import run_in_memory  # noqa: E402
from policy_corpus_builder.schemas import (  # noqa: E402
    BuilderConfig,
    ExportConfig,
    NormalizationConfig,
    ProjectConfig,
    QueriesConfig,
    SourceConfig,
)


class AdapterExecutionTests(unittest.TestCase):
    def test_query_aware_execution_collects_per_query(self) -> None:
        class QueryAwareAdapter:
            name = "query-aware-demo"
            execution_mode = "query-aware"

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def load_source(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def collect(
                self,
                source: SourceConfig,
                query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list[AdapterResult]:
                return [
                    AdapterResult(
                        payload={
                            "document_id": f"{source.name}:{query.query_id}",
                            "title": f"Result for {query.text}",
                        }
                    )
                ]

        register_adapter(QueryAwareAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/query-aware"),
            queries=QueriesConfig(items=("energy security", "resilience policy")),
            sources=(SourceConfig(name="demo-source", adapter="query-aware-demo"),),
            normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
            export=ExportConfig(formats=("jsonl",)),
        )

        run_result = run_in_memory(config, base_path=Path("."))

        self.assertEqual(run_result.summary.raw_result_count, 2)
        self.assertEqual(
            [doc.query for doc in run_result.documents],
            ["energy security", "resilience policy"],
        )

    def test_query_agnostic_execution_loads_once_for_many_queries(self) -> None:
        class QueryAgnosticAdapter:
            name = "query-agnostic-demo"
            execution_mode = "query-agnostic"
            load_calls = 0

            def validate_source_config(self, source: SourceConfig, *, base_path: Path) -> None:
                return None

            def load_source(self, source: SourceConfig, *, base_path: Path) -> list[dict[str, str]]:
                type(self).load_calls += 1
                return [
                    {"id": "doc-1", "title": "Energy Security Strategy", "query_key": "energy security"},
                    {"id": "doc-2", "title": "National Resilience Plan", "query_key": "resilience policy"},
                ]

            def collect(
                self,
                source: SourceConfig,
                query,
                *,
                base_path: Path,
                loaded_source: object | None = None,
            ) -> list[AdapterResult]:
                assert isinstance(loaded_source, list)
                return [
                    AdapterResult(
                        payload={
                            "document_id": f"{source.name}:{record['id']}",
                            "title": record["title"],
                        }
                    )
                    for record in loaded_source
                    if record["query_key"] == query.text
                ]

        QueryAgnosticAdapter.load_calls = 0
        register_adapter(QueryAgnosticAdapter)
        config = BuilderConfig(
            project=ProjectConfig(name="demo", output_dir="outputs/query-agnostic"),
            queries=QueriesConfig(items=("energy security", "resilience policy")),
            sources=(SourceConfig(name="demo-source", adapter="query-agnostic-demo"),),
            normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
            export=ExportConfig(formats=("jsonl",)),
        )

        run_result = run_in_memory(config, base_path=Path("."))

        self.assertEqual(QueryAgnosticAdapter.load_calls, 1)
        self.assertEqual(run_result.summary.raw_result_count, 2)
        self.assertEqual(
            [doc.query for doc in run_result.documents],
            ["energy security", "resilience policy"],
        )

    def test_local_file_query_agnostic_mode_avoids_repeated_reads(self) -> None:
        from policy_corpus_builder.adapters.local_file import LocalFileAdapter  # noqa: E402

        original_load_records = LocalFileAdapter._load_records
        load_calls = {"count": 0}

        def counted_load_records(self, source: SourceConfig, *, base_path: Path):
            load_calls["count"] += 1
            return original_load_records(self, source, base_path=base_path)

        LocalFileAdapter._load_records = counted_load_records
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                base_path = Path(tmpdir)
                fixture_path = base_path / "records.jsonl"
                fixture_path.write_text(
                    "\n".join(
                        [
                            '{"id":"doc-1","title":"Energy Security Strategy","queries":["energy security"]}',
                            '{"id":"doc-2","title":"National Resilience Plan","queries":["resilience policy"]}',
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                config = BuilderConfig(
                    project=ProjectConfig(name="demo", output_dir="outputs/local-file-mode"),
                    queries=QueriesConfig(items=("energy security", "resilience policy")),
                    sources=(
                        SourceConfig(
                            name="fixture-source",
                            adapter="local-file",
                            settings={"path": "records.jsonl", "query_field": "queries"},
                        ),
                    ),
                    normalization=NormalizationConfig(deduplicate=False, deduplicate_fields=()),
                    export=ExportConfig(formats=("jsonl",)),
                )

                run_result = run_in_memory(config, base_path=base_path)
        finally:
            LocalFileAdapter._load_records = original_load_records

        self.assertEqual(load_calls["count"], 1)
        self.assertEqual(run_result.summary.raw_result_count, 2)
        self.assertEqual(
            [doc.query for doc in run_result.documents],
            ["energy security", "resilience policy"],
        )


if __name__ == "__main__":
    unittest.main()
