"""Minimal CLI for bootstrap and future pipeline commands."""

from __future__ import annotations

import argparse
from pathlib import Path

from policy_corpus_builder.adapters import available_adapters
from policy_corpus_builder.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="policy-corpus-builder")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-adapters", help="List registered source adapters.")

    validate_parser = subparsers.add_parser(
        "validate-config",
        help="Load and lightly validate a TOML configuration file.",
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
        config = load_config(args.config)
        print(
            f"Config loaded successfully: {args.config} "
            f"(sections: {', '.join(sorted(config.keys())) or 'none'})"
        )
        return 0

    if args.command == "run":
        load_config(args.config)
        print("Pipeline execution is not implemented yet.")
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
