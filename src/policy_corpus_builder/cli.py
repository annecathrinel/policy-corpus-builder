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
    build_corpus_parser.add_argument(
        "--max-jurisdiction-workers",
        type=int,
        help=(
            "Maximum number of jurisdictions to collect concurrently. Each "
            "jurisdiction hits a fully separate external API, so this "
            "defaults to one worker per requested jurisdiction. Lower this "
            "if you want to throttle how many external services are hit at "
            "once."
        ),
    )
    build_corpus_parser.add_argument(
        "--max-per-term",
        type=int,
        help=(
            "Maximum documents kept per query term for non-EU jurisdictions "
            "(UK, AUS, CA, NZ, US). Defaults to 500. The underlying adapter "
            "itself falls back to 100 when this isn't set explicitly, so "
            "omitting this flag no longer means 'unlimited' - it means 500."
        ),
    )
    jurisdiction_logs_group = build_corpus_parser.add_mutually_exclusive_group()
    jurisdiction_logs_group.add_argument(
        "--jurisdiction-logs",
        dest="write_jurisdiction_logs",
        action="store_true",
        default=True,
        help=(
            "Write each jurisdiction's (and NIM's) own diagnostic/progress "
            "output to <outputs-path>/logs/<jurisdiction>.log instead of "
            "the main job output. This is the default - it's what keeps "
            "the main job output down to just the "
            "[policy-corpus-builder] summary lines."
        ),
    )
    jurisdiction_logs_group.add_argument(
        "--no-jurisdiction-logs",
        dest="write_jurisdiction_logs",
        action="store_false",
        help=(
            "Print every jurisdiction's (and NIM's) internal diagnostic "
            "output inline in the main job output instead of splitting it "
            "into per-jurisdiction log files. Useful for debugging one "
            "jurisdiction interactively."
        ),
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
                max_jurisdiction_workers=args.max_jurisdiction_workers,
                non_eu_max_per_term=args.max_per_term,
                write_jurisdiction_logs=args.write_jurisdiction_logs,
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
        if result.jurisdiction_log_paths:
            print(f"Jurisdiction logs: {result.outputs_path / 'logs'}")
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
