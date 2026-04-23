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
    build_duplicate_audit_rows,
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
            csv_path, jsonl_path = export_duplicate_audit(
                documents,
                output_dir=Path(tmpdir),
            )
            with csv_path.open(encoding="utf-8", newline="") as fh:
                csv_rows = list(csv.DictReader(fh))
            jsonl_lines = jsonl_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(csv_path.name, DUPLICATE_AUDIT_CSV_FILENAME)
        self.assertEqual(jsonl_path.name, DUPLICATE_AUDIT_JSONL_FILENAME)
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


if __name__ == "__main__":
    unittest.main()

