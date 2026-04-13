from pathlib import Path

from policy_corpus_builder.adapters import available_adapters
from policy_corpus_builder.config import load_config


def test_placeholder_adapter_is_exposed() -> None:
    assert "placeholder" in available_adapters()


def test_example_config_loads() -> None:
    config_path = Path("examples/minimal.toml")
    config = load_config(config_path)
    assert config["project"]["name"] == "example-corpus"
