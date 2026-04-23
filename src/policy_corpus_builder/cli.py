"""Minimal CLI for bootstrap and future pipeline commands."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

from policy_corpus_builder import CorpusBuildValidationError, build_policy_corpus
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

    build_corpus_parser = subparsers.add_parser(
        "build-corpus",
        help="Build a final policy corpus with the top-level happy-path builder.",
    )
    build_corpus_parser.add_argument(
        "--query-terms",
        required=True,
        nargs="+",
        help="One or more query terms to run across the selected jurisdictions.",
    )
    build_corpus_parser.add_argument(
        "--jurisdictions",
        required=True,
        nargs="+",
        help="One or more jurisdictions: EU, UK, CA, AUS, NZ, US.",
    )
    build_corpus_parser.add_argument(
        "--outputs-path",
        required=True,
        type=Path,
        help="Directory where cache, intermediate corpora, final corpus, and manifest are written.",
    )
    build_corpus_parser.add_argument(
        "--include-translations",
        action="store_true",
        help="Also run translated terms through the EU path.",
    )
    build_corpus_parser.add_argument(
        "--translated-terms",
        nargs="+",
        help="One or more translated terms for the EU path.",
    )
    build_corpus_parser.add_argument(
        "--include-nim",
        action="store_true",
        help="Run EUR-Lex NIM for eligible EU legal-act CELEX seeds.",
    )
    nim_fulltext_group = build_corpus_parser.add_mutually_exclusive_group()
    nim_fulltext_group.add_argument(
        "--include-nim-fulltext",
        dest="include_nim_fulltext",
        action="store_true",
        default=True,
        help="Retrieve NIM full text when NIM is enabled. This is the default.",
    )
    nim_fulltext_group.add_argument(
        "--no-nim-fulltext",
        dest="include_nim_fulltext",
        action="store_false",
        help="Write normalized NIM measure records without the slower NIM full-text stage.",
    )
    build_corpus_parser.add_argument(
        "--nim-max-rows",
        type=int,
        help="Limit the number of NIM rows processed per eligible EU legal-act seed.",
    )

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

    if args.command == "build-corpus":
        try:
            result = build_policy_corpus(
                query_terms=args.query_terms,
                jurisdictions=args.jurisdictions,
                outputs_path=args.outputs_path,
                include_translations=args.include_translations,
                translated_terms=args.translated_terms,
                include_nim=args.include_nim,
                include_nim_fulltext=args.include_nim_fulltext,
                nim_max_rows=args.nim_max_rows,
            )
        except (
            AdapterError,
            CorpusBuildValidationError,
            ConfigValidationError,
            FileNotFoundError,
            NormalizationError,
            ValueError,
        ) as exc:
            print(f"Corpus build failed: {exc}", file=sys.stderr)
            return 1

        print("Corpus build completed successfully.")
        print(f"Final corpus: {result.final_corpus_path}")
        print(f"Manifest: {result.manifest_path}")
        print(f"Final documents: {result.final_document_count}")
        if result.nim_corpus_path is not None:
            print(f"NIM corpus: {result.nim_corpus_path}")
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
