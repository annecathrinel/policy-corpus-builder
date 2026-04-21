import os
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch
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
        self.assertEqual(config.queries.items, tuple())
        self.assertEqual(len(config.sources), 1)
        self.assertEqual(config.sources[0].adapter, "placeholder")
        self.assertEqual(config.export.formats, ("jsonl",))

    def test_valid_eurlex_example_config_loads(self) -> None:
        with patch.dict(
            "os.environ",
            {"EURLEX_WS_USER": "demo-user", "EURLEX_WS_PASS": "demo-pass"},
            clear=False,
        ):
            config = load_and_validate_config(Path("examples/eu.toml"))

        self.assertEqual(config.project.name, "eurlex-example")
        self.assertEqual(config.queries.inventory, "queries/eu_queries.txt")
        self.assertEqual(config.sources[0].adapter, "eurlex")
        self.assertEqual(config.sources[0].settings["fulltext_mode"], "supported_only")
        self.assertEqual(config.export.formats, ("jsonl",))

    def test_valid_eurlex_nim_example_config_loads(self) -> None:
        with patch.dict(
            "os.environ",
            {"EURLEX_WS_USER": "demo-user", "EURLEX_WS_PASS": "demo-pass"},
            clear=False,
        ):
            config = load_and_validate_config(Path("examples/eu_nim.toml"))

        self.assertEqual(config.project.name, "eurlex-nim-example")
        self.assertEqual(config.sources[0].adapter, "eurlex-nim")
        self.assertEqual(config.export.formats, ("jsonl",))

    def test_valid_supported_non_eu_nz_example_loads(self) -> None:
        with patch.dict(
            "os.environ",
            {"NZ_LEGISLATION_API_KEY": "nz-demo-key"},
            clear=False,
        ):
            config = load_and_validate_config(Path("examples/non_eu_new_zealand.toml"))

        self.assertEqual(config.project.name, "non-eu-new-zealand-example")
        self.assertEqual(config.sources[0].adapter, "non-eu")
        self.assertEqual(config.sources[0].settings["nz_mode"], "api")

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

    def test_non_eu_nz_requires_supported_api_mode_by_default(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "supports New Zealand only in API mode by default",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [
                        {
                            "name": "nz-source",
                            "adapter": "non-eu",
                            "settings": {"countries": ["NZ"], "nz_mode": "auto"},
                        }
                    ],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )

    def test_non_eu_nz_auto_mode_can_be_explicitly_allowed_for_internal_use(self) -> None:
        config = validate_config_dict(
            {
                "project": {"name": "demo", "output_dir": "outputs/demo"},
                "queries": {"items": ["policy"]},
                "sources": [
                    {
                        "name": "nz-source",
                        "adapter": "non-eu",
                        "settings": {
                            "countries": ["NZ"],
                            "nz_mode": "auto",
                            "allow_internal": True,
                        },
                    }
                ],
                "normalization": {
                    "deduplicate": True,
                    "deduplicate_fields": ["title"],
                },
                "export": {"formats": ["jsonl"]},
            }
        )

        self.assertTrue(config.sources[0].settings["allow_internal"])

    def test_non_eu_multi_country_config_requires_internal_escape_hatch(self) -> None:
        with self.assertRaisesRegex(
            ConfigValidationError,
            "supports only these documented workflows by default",
        ):
            validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [
                        {
                            "name": "non-eu-source",
                            "adapter": "non-eu",
                            "settings": {"countries": ["UK", "CA"]},
                        }
                    ],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )

    def test_supported_non_eu_us_requires_api_key_during_config_validation(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(
                ConfigValidationError,
                "requires a US API key for the supported US workflow",
            ):
                validate_config_dict(
                    {
                        "project": {"name": "demo", "output_dir": "outputs/demo"},
                        "queries": {"items": ["policy"]},
                        "sources": [
                            {
                                "name": "us-source",
                                "adapter": "non-eu",
                                "settings": {"countries": ["US"]},
                            }
                        ],
                        "normalization": {
                            "deduplicate": True,
                            "deduplicate_fields": ["title"],
                        },
                        "export": {"formats": ["jsonl"]},
                    }
                )

    def test_supported_non_eu_us_with_api_key_passes_config_validation(self) -> None:
        with patch.dict(
            "os.environ",
            {"REGULATIONS_GOV_API_KEY": "us-demo-key"},
            clear=False,
        ):
            config = validate_config_dict(
                {
                    "project": {"name": "demo", "output_dir": "outputs/demo"},
                    "queries": {"items": ["policy"]},
                    "sources": [
                        {
                            "name": "us-source",
                            "adapter": "non-eu",
                            "settings": {"countries": ["US"]},
                        }
                    ],
                    "normalization": {
                        "deduplicate": True,
                        "deduplicate_fields": ["title"],
                    },
                    "export": {"formats": ["jsonl"]},
                }
            )

        self.assertEqual(config.sources[0].settings["countries"], ["US"])


if __name__ == "__main__":
    unittest.main()
