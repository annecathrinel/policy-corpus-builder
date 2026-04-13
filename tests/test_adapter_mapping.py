import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.adapters.base import AdapterDataError  # noqa: E402
from policy_corpus_builder.adapters.mapping import build_adapter_result  # noqa: E402


class AdapterMappingTests(unittest.TestCase):
    def test_build_adapter_result_maps_required_and_optional_fields(self) -> None:
        result = build_adapter_result(
            {
                "id": "doc-1",
                "title_text": "Energy Security Strategy",
                "summary_text": "Strategy summary",
                "link": "https://example.invalid/doc-1",
            },
            field_mapping={
                "document_id": "id",
                "title": "title_text",
                "summary": "summary_text",
                "url": "link",
            },
        )

        self.assertEqual(result.payload["document_id"], "doc-1")
        self.assertEqual(result.payload["title"], "Energy Security Strategy")
        self.assertEqual(result.payload["summary"], "Strategy summary")
        self.assertEqual(result.payload["url"], "https://example.invalid/doc-1")

    def test_build_adapter_result_enforces_required_fields(self) -> None:
        with self.assertRaisesRegex(
            AdapterDataError,
            "Mapped normalized field 'document_id' is required",
        ):
            build_adapter_result(
                {"title_text": "Missing id"},
                field_mapping={
                    "document_id": "id",
                    "title": "title_text",
                },
            )

    def test_build_adapter_result_uses_defaults_for_missing_optional_fields(self) -> None:
        result = build_adapter_result(
            {
                "id": "doc-1",
                "title_text": "Energy Security Strategy",
            },
            field_mapping={
                "document_id": "id",
                "source_document_id": "source_id",
                "title": "title_text",
            },
            defaults={"source_document_id": "doc-1"},
        )

        self.assertEqual(result.payload["source_document_id"], "doc-1")

    def test_unmapped_raw_fields_are_preserved_in_raw_record(self) -> None:
        result = build_adapter_result(
            {
                "id": "doc-1",
                "title_text": "Energy Security Strategy",
                "vendor_field": "keep-me",
            },
            field_mapping={
                "document_id": "id",
                "title": "title_text",
            },
        )

        self.assertEqual(result.payload["raw_record"]["vendor_field"], "keep-me")


if __name__ == "__main__":
    unittest.main()
