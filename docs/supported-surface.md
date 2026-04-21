# Supported Surface

This page defines the currently supported workflows and the implementation surface that downstream users should treat as public.

## Supported Workflows

- UK via `adapter = "non-eu"` with `countries = ["UK"]`
- Canada via `adapter = "non-eu"` with `countries = ["CA"]`
- Australia via `adapter = "non-eu"` with `countries = ["AUS"]`
- US via `adapter = "non-eu"` with `countries = ["US"]`
- New Zealand API mode via `adapter = "non-eu"` with `countries = ["NZ"]` and `nz_mode = "api"`
- ordinary EUR-Lex via `adapter = "eurlex"`
- EUR-Lex NIM via `adapter = "eurlex-nim"`

## Supported Public Entry Points

Supported adapter entry points:

- `get_adapter` in `src/policy_corpus_builder/adapters/__init__.py`
- `LocalFileAdapter` in `src/policy_corpus_builder/adapters/local_file.py`
- `NonEUAdapter` in `src/policy_corpus_builder/adapters/non_eu_adapter.py`
- `EurlexAdapter` in `src/policy_corpus_builder/adapters/eurlex_adapter.py`
- `EurlexNIMAdapter` in `src/policy_corpus_builder/adapters/eurlex_nim_adapter.py`

Supported shared pipeline entry points:

- `load_and_validate_config`
- `load_queries`
- `normalize_adapter_results`
- `deduplicate_documents`
- `export_documents_jsonl`
- `run_from_config_path`

The adapter registry plus these wrapper modules are the supported import surface. Internal helper modules behind them may change without notice.

## Required Credentials By Workflow

- UK: no required credential; set `POLICY_CORPUS_BUILDER_USER_AGENT` or `source.settings.user_agent`
- Canada: no required credential
- Australia: no required credential
- US: `REGULATIONS_GOV_API_KEY`
- New Zealand API mode: `NZ_LEGISLATION_API_KEY`
- ordinary EUR-Lex: `EURLEX_WS_USER` and `EURLEX_WS_PASS`
- EUR-Lex NIM: `EURLEX_WS_USER` and `EURLEX_WS_PASS`

Legacy EUR-Lex credential names `EURLEX_USER` and `EURLEX_WEB_PASS` are still accepted for compatibility, but they are compatibility aliases rather than the primary supported names.

## Provisional Or Internal Modules

These modules are not supported public implementation surfaces:

- `src/policy_corpus_builder/adapters/eurlex.py`
- `src/policy_corpus_builder/adapters/eurlex_nim.py`
- `src/policy_corpus_builder/adapters/non_eu.py`

In particular, treat the following as internal/provisional:

- legacy migrated EUR-Lex and EUR-Lex NIM helpers
- notebook-era diagnostics
- notebook-era bulk loaders
- notebook-era cache/summary helpers
- manual inspection/debug helpers

These remain useful implementation building blocks, but downstream code should not import them as stable APIs.

## Provisional Workflows

The following workflows or modes are not part of the supported contract:

- New Zealand `nz_mode = "auto"` because it can silently route into the fallback scraper path
- New Zealand `nz_mode = "scrape"` because it is a no-key fallback mode tied to the public website
- placeholder/demo-only retrieval surfaces such as the `placeholder` adapter

For explicit internal developer use, provisional `non-eu` modes can still be validated by setting `source.settings.allow_internal = true`. That escape hatch is intentionally opt-in and is not part of the default supported surface.

## Examples And Notebooks

Examples and notebooks are user guidance, not stable import surfaces.

- `examples/non_eu_uk.toml`
- `examples/non_eu_canada.toml`
- `examples/non_eu_australia.toml`
- `examples/non_eu_new_zealand.toml` with `nz_mode = "api"`
- `examples/non_eu_us.toml`
- `examples/eu.toml`
- `examples/eu_nim.toml`

`examples/minimal.toml` and the notebook directory are retained for internal structure, smoke coverage, and documentation, but they should not be read as expanding the supported workflow surface.
