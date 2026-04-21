# policy-corpus-builder

`policy-corpus-builder` is a small Python toolkit for building clean policy document corpora.

Version `0.1` is intentionally narrow:

- load queries from a config file
- read structured local policy records with the `local-file` adapter
- run one supported ordinary EUR-Lex workflow through the `eurlex` adapter
- run one supported EUR-Lex NIM workflow through the `eurlex-nim` adapter
- run supported live non-EU workflows for UK legislation, Canada publications, Australia legislation, API-backed New Zealand legislation, and US Regulations.gov documents
- normalize records into one shared `NormalizedDocument` model
- deduplicate deterministically
- export the final corpus to JSONL

The package is library-first. The CLI is a thin convenience layer on top of the same workflow.

## What v0.1 Includes

- one normalized document model: [src/policy_corpus_builder/models.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/models.py)
- one real adapter: `local-file`
- one supported ordinary EUR-Lex adapter path: `eurlex`
- one supported EUR-Lex NIM adapter path: `eurlex-nim`
- supported live non-EU paths: `non-eu` with `countries = ["UK"]`, `countries = ["CA"]`, `countries = ["AUS"]`, `countries = ["NZ"]` with an API key, and `countries = ["US"]` with `REGULATIONS_GOV_API_KEY`
- deterministic deduplication using configured normalized fields
- one export format: JSONL
- one local-file notebook walkthrough example: [examples/notebooks/local_file_end_to_end.ipynb](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/notebooks/local_file_end_to_end.ipynb)
- one minimal CLI for config validation and running the same workflow

## Supported Surface

The currently supported workflows are:

- UK via `adapter = "non-eu"` with `countries = ["UK"]`
- Canada via `adapter = "non-eu"` with `countries = ["CA"]`
- Australia via `adapter = "non-eu"` with `countries = ["AUS"]`
- US via `adapter = "non-eu"` with `countries = ["US"]`
- New Zealand API mode via `adapter = "non-eu"` with `countries = ["NZ"]` and `nz_mode = "api"`
- ordinary EUR-Lex via `adapter = "eurlex"`
- EUR-Lex NIM via `adapter = "eurlex-nim"`

The supported adapter entry points are:

- `get_adapter` in [src/policy_corpus_builder/adapters/__init__.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/__init__.py)
- `LocalFileAdapter` in [src/policy_corpus_builder/adapters/local_file.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/local_file.py)
- `NonEUAdapter` in [src/policy_corpus_builder/adapters/non_eu_adapter.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/non_eu_adapter.py)
- `EurlexAdapter` in [src/policy_corpus_builder/adapters/eurlex_adapter.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex_adapter.py)
- `EurlexNIMAdapter` in [src/policy_corpus_builder/adapters/eurlex_nim_adapter.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex_nim_adapter.py)

The public implementation surface is the adapter registry plus those adapter wrapper modules. Importing deeper helper modules directly is not part of the supported API.

For a stable summary of supported versus provisional code paths, see [docs/supported-surface.md](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/docs/supported-surface.md).

## Small Public API

These are the main functions and modules worth treating as the v0.1 public surface:

- `load_and_validate_config` in [src/policy_corpus_builder/config.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/config.py)
- `load_queries` in [src/policy_corpus_builder/queries.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/queries.py)
- `get_adapter` in [src/policy_corpus_builder/adapters/__init__.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/__init__.py)
- `normalize_adapter_results` in [src/policy_corpus_builder/pipeline.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/pipeline.py)
- `deduplicate_documents` in [src/policy_corpus_builder/postprocess.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/postprocess.py)
- `export_documents_jsonl` in [src/policy_corpus_builder/exporters/jsonl.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/exporters/jsonl.py)
- `run_from_config_path` in [src/policy_corpus_builder/orchestration.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/orchestration.py)

The adapter wrapper modules listed in the supported surface section above are also part of the supported import surface for workflow-specific integrations.

## Install

```bash
pip install -e .
```

Python `3.11+` is required.

## Local Credentials

For local-only secrets, copy `.env.example` to `.env`, fill in your real credentials, and keep `.env` untracked.

```bash
cp .env.example .env
```

Supported workflow credentials and environment variables:

- UK via `non-eu`: no required credential; set `POLICY_CORPUS_BUILDER_USER_AGENT` or `source.settings.user_agent` for responsible access
- Canada via `non-eu`: no required credential
- Australia via `non-eu`: no required credential
- US via `non-eu`: `REGULATIONS_GOV_API_KEY`
- New Zealand API mode via `non-eu`: `NZ_LEGISLATION_API_KEY`
- ordinary EUR-Lex via `eurlex`: `EURLEX_WS_USER` and `EURLEX_WS_PASS`
- EUR-Lex NIM via `eurlex-nim`: `EURLEX_WS_USER` and `EURLEX_WS_PASS`

Compatibility environment variables:

- `EURLEX_USER`
- `EURLEX_WEB_PASS`

The package will load `.env` automatically if it is present in the repository root (or a parent directory when importing the package locally). Never commit `.env`.

## Provisional And Internal Surface

The following modules and workflows are intentionally not part of the supported public surface:

- legacy migrated [src/policy_corpus_builder/adapters/eurlex.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex.py): internal migrated helper module with notebook-era ports, diagnostics, cache summaries, and manual inspection helpers
- legacy migrated [src/policy_corpus_builder/adapters/eurlex_nim.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex_nim.py): internal migrated helper module with notebook-era ports, summaries, and bulk helper logic
- [src/policy_corpus_builder/adapters/non_eu.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/non_eu.py): internal implementation module behind the supported `non-eu` adapter wrapper
- notebook-era diagnostics, bulk loaders, cache summaries, and summary helpers inside those migrated modules: internal and subject to change without notice
- New Zealand `nz_mode = "auto"`: convenience mode only; not the supported contract because it can route into fallback scraping
- New Zealand `nz_mode = "scrape"`: provisional no-key fallback only
- placeholder and minimal/demo-only surfaces such as `placeholder` and [examples/minimal.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/minimal.toml): internal scaffolding rather than a supported retrieval workflow

Examples and notebooks are documentation aids, not stable implementation entry points. The supported implementation surface is the adapter registry, the adapter wrapper modules, the shared pipeline/orchestration functions, and the normalized model/export pipeline documented here.

For explicit internal developer use, provisional `non-eu` modes can still be validated by setting `source.settings.allow_internal = true`. That escape hatch is intentionally opt-in and outside the default supported surface.

## Happy Path

The simplest end-to-end example uses the bundled local fixture-backed adapter and config:

- config: [examples/local_file.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/local_file.toml)
- fixture data: [examples/fixtures/policies.jsonl](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/fixtures/policies.jsonl)
- notebook walkthrough: [examples/notebooks/local_file_end_to_end.ipynb](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/notebooks/local_file_end_to_end.ipynb)

### Library Usage

```python
from pathlib import Path

from policy_corpus_builder.adapters import get_adapter
from policy_corpus_builder.config import load_and_validate_config
from policy_corpus_builder.exporters.jsonl import export_documents_jsonl
from policy_corpus_builder.pipeline import normalize_adapter_results
from policy_corpus_builder.postprocess import deduplicate_documents
from policy_corpus_builder.queries import load_queries

repo_root = Path.cwd()
config_path = repo_root / "examples" / "local_file.toml"
config = load_and_validate_config(config_path)

queries = load_queries(config, base_path=config_path.parent)
source = config.sources[0]
adapter = get_adapter(source.adapter)
loaded_source = adapter.load_source(source, base_path=config_path.parent)

documents = []
for query in queries:
    raw_results = adapter.collect(
        source,
        query,
        base_path=config_path.parent,
        loaded_source=loaded_source,
    )
    documents.extend(
        normalize_adapter_results(raw_results, source=source, query=query)
    )

deduped = deduplicate_documents(tuple(documents), config=config.normalization)
output_path = export_documents_jsonl(
    deduped.documents,
    output_dir=repo_root / "examples" / "outputs" / "readme-demo",
)
print(output_path)
```

### CLI Usage

```bash
policy-corpus-builder validate-config --config examples/local_file.toml
policy-corpus-builder run --config examples/local_file.toml
```

The CLI runs the same in-memory pipeline and writes `documents.jsonl` when `jsonl` is enabled in the config.

## Supported UK Workflow

The current supported UK live workflow uses the `non-eu` adapter with `countries = ["UK"]`.

What it supports today:

- query-driven UK legislation discovery
- normalized metadata export
- official `legislation.gov.uk` API representations as the preferred full-text backend

What it does not guarantee today:

- successful full-text extraction for every UK record

`policy-corpus-builder` now prefers the official UK API representations such as `data.xml` for full text. However, upstream access controls on `legislation.gov.uk` may still challenge automated requests. In those cases:

- the run is still considered successful if discovery and normalization complete
- exported records may contain `full_text = null`
- source-specific diagnostics remain in `raw_metadata.raw_record`, including `full_text_error = "waf_challenge"` and `retrieval_status = "upstream_blocked"`

For responsible use, set a clear contact-bearing user agent either through the environment variable `POLICY_CORPUS_BUILDER_USER_AGENT` or in `source.settings.user_agent` for the `non-eu` adapter.

## Supported Canada Workflow

The current supported Canada live workflow uses the `non-eu` adapter with `countries = ["CA"]`.

What it supports today:

- Canada Open Government / CKAN API-first discovery for document-like resources
- resource-metadata-based selection of direct PDF or HTML assets
- publications.gc.ca landing-page extraction as fallback only when API metadata is insufficient
- normalized JSONL export through the shared document model

The supported Canada example config is [examples/non_eu_canada.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/non_eu_canada.toml).

The current Canada path is the strongest non-UK migrated workflow in this repository and is the next explicitly supported live non-EU path after UK.

## Supported Australia Workflow

The current supported Australia live workflow uses the `non-eu` adapter with `countries = ["AUS"]`.

What it supports today:

- query-driven Australia legislation discovery
- normalized JSONL export through the shared document model

What it requires:

- no credential at present

The supported Australia example config is [examples/non_eu_australia.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/non_eu_australia.toml).

## Supported New Zealand Workflow

The current supported New Zealand live workflow uses the `non-eu` adapter with `countries = ["NZ"]`.

What it supports today:

- official `api.legislation.govt.nz` discovery via `/v0/works`
- API-returned version format selection for XML, PDF, and HTML
- normalized JSONL export through the shared document model

What it requires:

- for fully supported API mode, an `NZ_LEGISLATION_API_KEY`

The supported New Zealand example config is [examples/non_eu_new_zealand.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/non_eu_new_zealand.toml). Treat `nz_mode = "api"` as the supported contract.

New Zealand now supports two modes:

- `nz_mode = "auto"`: use the official API when `NZ_LEGISLATION_API_KEY` is present; otherwise fall back to the legacy website scraper
- `nz_mode = "api"`: require the API key and fail cleanly if it is missing
- `nz_mode = "scrape"`: force the legacy no-key scraper path

API mode is the preferred and fully supported path. Scraper mode is fallback-only and provisional because it still depends on the public site remaining accessible to scripted requests.

## Supported US Workflow

The current supported US live workflow uses the `non-eu` adapter with `countries = ["US"]`.

What it supports today:

- Regulations.gov document discovery through the official `/v4/documents` API
- API-backed document metadata retrieval for normalized corpus text
- normalized JSONL export through the shared document model

What it requires:

- `REGULATIONS_GOV_API_KEY`

The supported US example config is [examples/non_eu_us.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/non_eu_us.toml).

## Normalized Document Model

The shared normalized record is [NormalizedDocument](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/models.py:14).

Core fields include:

- `document_id`
- `source_name`
- `source_document_id`
- `title`
- `summary`
- `document_type`
- `language`
- `jurisdiction`
- `publication_date`
- `effective_date`
- `url`
- `download_url`
- `query`
- `full_text`
- `retrieved_at`
- `checksum`
- `content_path`
- `raw_metadata`

Source-specific leftovers stay in `raw_metadata`.

## Local-File Adapter

The bundled v0.1 adapter is `local-file` in [src/policy_corpus_builder/adapters/local_file.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/local_file.py).

It reads local JSON or JSONL files from `source.settings.path`.

Supported input shapes:

- JSONL: one JSON object per line
- JSON: a top-level list of objects
- JSON: an object with `records: [...]`

Required record fields:

- `id`
- `title`

Optional fields:

- `summary`
- `document_type`
- `language`
- `jurisdiction`
- `publication_date`
- `effective_date`
- `url`
- `download_url`
- `retrieved_at`
- `checksum`
- `content_path`
- `source_document_id`

Optional query matching:

- by default the adapter looks for a `queries` field on each record
- `queries` can be a string or list of strings
- matching records are emitted for the active query
- if the field is missing, the record is treated as query-agnostic

## Config Shape

v0.1 uses TOML with five top-level sections:

- `[project]`
- `[queries]`
- `[[sources]]`
- `[normalization]`
- `[export]`

The bundled example config is [examples/local_file.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/local_file.toml).

```toml
[project]
name = "local-file-example"
output_dir = "outputs/local-file-example"

[queries]
inventory = "queries/example_queries.txt"

[[sources]]
name = "fixture-policies"
adapter = "local-file"

[sources.settings]
path = "fixtures/policies.jsonl"
format = "jsonl"
query_field = "queries"

[normalization]
deduplicate = true
deduplicate_fields = ["title", "publication_date", "url"]

[export]
formats = ["jsonl"]
```

The live UK example is [examples/non_eu_uk.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/non_eu_uk.toml). For that workflow, `source.settings.user_agent` is optional but recommended.

## Supported EUR-Lex Workflow

The current supported EUR-Lex live workflow uses the `eurlex` adapter with the ordinary EU WebService search path plus ordinary EU CELEX full-text retrieval.

What it supports today:

- query-inventory-driven EUR-Lex WebService search
- CELEX-based ordinary EU document consolidation
- CELEX full-text retrieval for supported document types
- normalized JSONL export through the shared document model

What it requires:

- `EURLEX_WS_USER` and `EURLEX_WS_PASS`

Legacy names `EURLEX_USER` and `EURLEX_WEB_PASS` are still accepted for compatibility.

The supported EUR-Lex example config is [examples/eu.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/eu.toml).

## Supported EUR-Lex NIM Workflow

The current supported EUR-Lex NIM live workflow uses the `eurlex-nim` adapter.

What it supports today:

- CELEX-seeded national implementation retrieval for one EU legal act
- query-seeded EUR-Lex act lookup followed by NIM retrieval for eligible legal acts
- national measure record normalization into the shared document model
- optional NIM full-text retrieval through the migrated EUR-Lex/NIM helper subset
- normalized JSONL export through the shared document model

What it requires:

- `EURLEX_WS_USER` and `EURLEX_WS_PASS`

Legacy names `EURLEX_USER` and `EURLEX_WEB_PASS` are still accepted for compatibility.

The supported EUR-Lex NIM example config is [examples/eu_nim.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/eu_nim.toml).

## Output

The v0.1 export format is JSONL. Each line is one normalized document record produced by `NormalizedDocument.to_dict()`.

Typical run output goes under the configured project output directory, for example:

- [examples/outputs/local-file-example/documents.jsonl](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/outputs/local-file-example/documents.jsonl)

The current implementation also writes a small run manifest alongside corpus exports, but that is a secondary artifact rather than the main v0.1 feature.

## Repository Scope

This repository is intentionally separate from `NiD-Policy-Analysis-clean`.

`policy-corpus-builder` is for:

- query loading
- source access
- normalization
- corpus cleaning
- export

It is not for:

- research questions
- project-specific dictionaries
- notebooks tied to one analysis project
- report generation
