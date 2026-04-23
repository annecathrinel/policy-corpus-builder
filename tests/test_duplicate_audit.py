import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.exporters.duplicate_audit import (  # noqa: E402
    DUPLICATE_AUDIT_CSV_FILENAME,
    DUPLICATE_AUDIT_JSONL_FILENAME,
    DUPLICATE_GROUPS_SUMMARY_CSV_FILENAME,
    DUPLICATE_GROUPS_SUMMARY_JSON_FILENAME,
    build_duplicate_audit_rows,
    build_duplicate_group_aggregate_summary,
    build_duplicate_group_summary_rows,
    export_duplicate_audit,
)
from policy_corpus_builder.models import NormalizedDocument  # noqa: E402


class DuplicateAuditTests(unittest.TestCase):
    def test_duplicate_audit_groups_conservative_exact_signals(self) -> None:
        documents = (
            NormalizedDocument(
                document_id="doc-1",
                source_name="source-a",
                source_document_id="32014L0089",
                title="Marine Spatial Planning Directive",
                url="https://www.example.org/legal/doc?b=2&a=1#fragment",
                jurisdiction="European Union",
                publication_date="2014-07-23",
                raw_metadata={"raw_record": {"celex": "32014L0089"}},
            ),
            NormalizedDocument(
                document_id="doc-2",
                source_name="source-b",
                source_document_id=" 32014l0089 ",
                title="  Marine   Spatial Planning Directive ",
                url="https://example.org/legal/doc?a=1&b=2",
                jurisdiction="European Union",
                publication_date="2014-07-23",
                raw_metadata={"raw_record": {"celex_full": "32014L0089"}},
            ),
            NormalizedDocument(
                document_id="doc-3",
                source_name="source-b",
                source_document_id="unique",
                title="Different Act",
                url="https://example.org/legal/other",
                jurisdiction="European Union",
            ),
        )

        rows = build_duplicate_audit_rows(documents)

        self.assertEqual({row["group_size"] for row in rows}, {2})
        self.assertEqual(
            {row["signal"] for row in rows},
            {"celex", "normalized_title", "normalized_url", "source_document_id"},
        )
        self.assertNotIn("doc-3", {row["document_id"] for row in rows})
        self.assertIn(
            "https://example.org/legal/doc?a=1&b=2",
            {row["representative_value"] for row in rows},
        )

    def test_export_duplicate_audit_writes_csv_and_jsonl_with_headers_for_empty_results(self) -> None:
        documents = (
            NormalizedDocument(
                document_id="doc-1",
                source_name="source-a",
                title="Unique first document",
                url="https://example.org/one",
            ),
            NormalizedDocument(
                document_id="doc-2",
                source_name="source-a",
                title="Unique second document",
                url="https://example.org/two",
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            csv_path, jsonl_path = export_duplicate_audit(
                documents,
                output_dir=output_dir,
            )
            summary_csv_path = output_dir / DUPLICATE_GROUPS_SUMMARY_CSV_FILENAME
            summary_json_path = output_dir / DUPLICATE_GROUPS_SUMMARY_JSON_FILENAME
            with csv_path.open(encoding="utf-8", newline="") as fh:
                csv_rows = list(csv.DictReader(fh))
            jsonl_lines = jsonl_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(csv_path.name, DUPLICATE_AUDIT_CSV_FILENAME)
        self.assertEqual(jsonl_path.name, DUPLICATE_AUDIT_JSONL_FILENAME)
        self.assertEqual(summary_csv_path.name, DUPLICATE_GROUPS_SUMMARY_CSV_FILENAME)
        self.assertEqual(summary_json_path.name, DUPLICATE_GROUPS_SUMMARY_JSON_FILENAME)
        self.assertEqual(csv_rows, [])
        self.assertEqual(jsonl_lines, [])

    def test_export_duplicate_audit_jsonl_matches_csv_rows(self) -> None:
        documents = (
            NormalizedDocument(
                document_id="doc-1",
                source_name="source-a",
                title="Shared Planning Title",
            ),
            NormalizedDocument(
                document_id="doc-2",
                source_name="source-b",
                title="shared planning title",
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path, jsonl_path = export_duplicate_audit(
                documents,
                output_dir=Path(tmpdir),
            )
            with csv_path.open(encoding="utf-8", newline="") as fh:
                csv_rows = list(csv.DictReader(fh))
            jsonl_rows = [
                json.loads(line)
                for line in jsonl_path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(len(csv_rows), 2)
        self.assertEqual(len(jsonl_rows), 2)
        self.assertEqual(csv_rows[0]["duplicate_group_id"], jsonl_rows[0]["duplicate_group_id"])
        self.assertEqual(csv_rows[0]["signal"], "normalized_title")

    def test_duplicate_group_summary_prioritizes_manual_review_signals(self) -> None:
        documents = (
            NormalizedDocument(
                document_id="doc-1",
                source_name="source-a",
                title="Shared Marine Planning Policy",
                jurisdiction="United Kingdom",
                publication_date="2020-01-01",
            ),
            NormalizedDocument(
                document_id="doc-2",
                source_name="source-b",
                title="shared marine planning policy",
                jurisdiction="Canada",
                publication_date="2021-01-01",
            ),
            NormalizedDocument(
                document_id="doc-3",
                source_name="source-b",
                title="shared marine planning policy",
                jurisdiction="Canada",
                publication_date="2021-01-01",
            ),
        )

        audit_rows = build_duplicate_audit_rows(documents)
        summary_rows = build_duplicate_group_summary_rows(audit_rows)
        aggregate_summary = build_duplicate_group_aggregate_summary(summary_rows)

        self.assertEqual(len(summary_rows), 1)
        summary = summary_rows[0]
        self.assertEqual(summary["review_rank"], 1)
        self.assertEqual(summary["signal"], "normalized_title")
        self.assertEqual(summary["group_size"], 3)
        self.assertEqual(summary["jurisdictions"], "Canada | United Kingdom")
        self.assertEqual(summary["source_names"], "source-a | source-b")
        self.assertEqual(summary["publication_date_min"], "2020-01-01")
        self.assertEqual(summary["publication_date_max"], "2021-01-01")
        self.assertEqual(summary["spans_multiple_jurisdictions"], True)
        self.assertEqual(summary["spans_multiple_source_names"], True)
        self.assertIn("spans_multiple_jurisdictions", summary["review_interest_reasons"])
        self.assertEqual(aggregate_summary["duplicate_group_count"], 1)
        self.assertEqual(aggregate_summary["document_count"], 3)
        self.assertEqual(
            aggregate_summary["groups_by_signal"],
            [{"signal": "normalized_title", "group_count": 1}],
        )
        self.assertEqual(
            aggregate_summary["groups_by_jurisdiction"],
            [
                {"jurisdiction": "Canada", "group_count": 1},
                {"jurisdiction": "United Kingdom", "group_count": 1},
            ],
        )
        self.assertEqual(
            aggregate_summary["groups_by_source_name"],
            [
                {"source_name": "source-a", "group_count": 1},
                {"source_name": "source-b", "group_count": 1},
            ],
        )

    def test_export_duplicate_audit_writes_group_summary_artifacts(self) -> None:
        documents = (
            NormalizedDocument(
                document_id="doc-1",
                source_name="source-a",
                title="Shared Planning Title",
                jurisdiction="United Kingdom",
            ),
            NormalizedDocument(
                document_id="doc-2",
                source_name="source-b",
                title="shared planning title",
                jurisdiction="Canada",
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            _csv_path, _jsonl_path = export_duplicate_audit(
                documents,
                output_dir=output_dir,
            )
            summary_csv_path = output_dir / DUPLICATE_GROUPS_SUMMARY_CSV_FILENAME
            summary_json_path = output_dir / DUPLICATE_GROUPS_SUMMARY_JSON_FILENAME
            with summary_csv_path.open(encoding="utf-8", newline="") as fh:
                summary_rows = list(csv.DictReader(fh))
            aggregate_summary = json.loads(summary_json_path.read_text(encoding="utf-8"))

        self.assertEqual(len(summary_rows), 1)
        self.assertEqual(summary_rows[0]["duplicate_group_id"], "dup-000001")
        self.assertEqual(summary_rows[0]["spans_multiple_jurisdictions"], "True")
        self.assertEqual(aggregate_summary["duplicate_group_count"], 1)
        self.assertEqual(aggregate_summary["document_count"], 2)
        self.assertEqual(
            aggregate_summary["top_review_candidates"][0]["duplicate_group_id"],
            "dup-000001",
        )


if __name__ == "__main__":
    unittest.main()
