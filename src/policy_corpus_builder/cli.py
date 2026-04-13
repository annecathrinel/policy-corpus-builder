"""Minimal CLI for bootstrap and future pipeline commands."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

from policy_corpus_builder.adapters import available_adapters, get_adapter
from policy_corpus_builder.adapters.base import AdapterError
from policy_corpus_builder.config import (
    ConfigValidationError,
    format_config_summary,
    load_and_validate_config,
)
from policy_corpus_builder.orchestration import format_run_summary, run_from_config_path
from policy_corpus_builder.pipeline import NormalizationError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="policy-corpus-builder")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-adapters", help="List registered source adapters.")

    validate_parser = subparsers.add_parser(
        "validate-config",
        help="Load and validate a TOML configuration file.",
    )
    validate_parser.add_argument("--config", required=True, type=Path)

    run_parser = subparsers.add_parser(
        "run",
        help="Reserved entry point for future retrieval pipeline execution.",
    )
    run_parser.add_argument("--config", required=True, type=Path)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list-adapters":
        for adapter_name in available_adapters():
            print(adapter_name)
        return 0

    if args.command == "validate-config":
        try:
            config = load_and_validate_config(args.config)
            for source in config.sources:
                get_adapter(source.adapter).validate_source_config(
                    source,
                    base_path=args.config.parent,
                )
        except (ConfigValidationError, FileNotFoundError, tomllib.TOMLDecodeError) as exc:
            print(f"Config validation failed: {exc}", file=sys.stderr)
            return 1
        except AdapterError as exc:
            print(f"Config validation failed: {exc}", file=sys.stderr)
            return 1

        print(format_config_summary(config))
        return 0

    if args.command == "run":
        try:
            run_result = run_from_config_path(args.config)
        except (
            AdapterError,
            ConfigValidationError,
            FileNotFoundError,
            NormalizationError,
            tomllib.TOMLDecodeError,
        ) as exc:
            print(f"Run failed: {exc}", file=sys.stderr)
            return 1

        print(format_run_summary(run_result.summary))
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
