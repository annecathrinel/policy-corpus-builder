import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.config import (  # noqa: E402
    ConfigValidationError,
    format_config_summary,
    load_and_validate_config,
    validate_config_dict,
)


class ConfigValidationTests(unittest.TestCase):
    def test_valid_example_config_loads(self) -> None:
        config = load_and_validate_config(Path("examples/minimal.toml"))

        self.assertEqual(config.project.name, "example-corpus")
        self.assertEqual(config.project.output_dir, "outputs/example-corpus")
        self.assertEqual(config.queries.inventory, "queries/example_queries.txt")
        self.assertEqual(len(config.sources), 1)
        self.assertEqual(config.sources[0].adapter, "placeholder")
        self.assertEqual(config.export.formats, ("jsonl",))

    def test_config_summary_is_useful(self) -> None:
        config = load_and_validate_config(Path("examples/minimal.toml"))
        summary = format_config_summary(config)

        self.assertIn("Config validation successful.", summary)
        self.assertIn("Project: example-corpus", summary)
        self.assertIn("Queries: inventory file (queries/example_queries.txt)", summary)
        self.assertIn("Adapters: placeholder", summary)

    def test_missing_required_sections_raise_clear_error(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "Missing required top-level sections: export, normalization, sources",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                }
            )

    def test_invalid_export_format_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "export.formats contains unsupported values: xml",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [{"name": "demo-source", "adapter": "placeholder"}],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["xml"]},
                }
            )

    def test_invalid_adapter_reference_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "references unknown adapter 'missing-adapter'",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [{"name": "demo-source", "adapter": "missing-adapter"}],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )

    def test_invalid_normalization_fields_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "contains unknown metadata fields: made_up_field",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [{"name": "demo-source", "adapter": "placeholder"}],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["made_up_field"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )

    def test_queries_inventory_path_must_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "config.toml"
            config_file.write_text(
                textwrap.dedent(
                    """
                    [project]
                    name = "demo"
                    output_dir = "outputs/demo"

                    [queries]
                    inventory = "missing_queries.txt"

                    [[sources]]
                    name = "demo-source"
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

            with self.assertRaisesRegex(
                ConfigValidationError,
                "queries.inventory file does not exist: missing_queries.txt",
            ):
                load_and_validate_config(config_file)

    def test_queries_cannot_define_inventory_and_inline_items(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "queries cannot define both 'inventory' and 'items'",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"inventory": "queries.txt", "items": ["policy"]},
                    "sources": [{"name": "demo-source", "adapter": "placeholder"}],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )

    def test_invalid_boolean_type_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "normalization.deduplicate must be a boolean",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [{"name": "demo-source", "adapter": "placeholder"}],
                    "normalization": {
                        "deduplicate": "yes",
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )


if __name__ == "__main__":
    unittest.main()
