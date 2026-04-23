# policy-corpus-builder

`policy-corpus-builder` is a Python toolkit for building clean policy document corpora across supported jurisdictions.

The main happy-path public entry point is:

`build_policy_corpus(...)`

Use that function first when you want one final normalized corpus written to disk from a simple top-level call. The lower-level adapter and config surfaces are still available, but they are now secondary to the top-level builder workflow.

## Main Happy Path

The main user-facing function is:

```python
build_policy_corpus(
    query_terms: list[str],
    jurisdictions: list[str],
    outputs_path: str | Path,
    include_translations: bool = False,
    translated_terms: list[str] | None = None,
    include_nim: bool = False,
    include_nim_fulltext: bool = True,
    nim_max_rows: int | None = None,
) -> PolicyCorpusBuildResult
```

It is available directly from the package root:

```python
from policy_corpus_builder import build_policy_corpus
```

## Copy-Paste Example

```python
from pathlib import Path

from policy_corpus_builder import build_policy_corpus

result = build_policy_corpus(
    query_terms=[
        "marine spatial planning",
        "offshore renewable energy",
    ],
    jurisdictions=["EU", "UK", "CA"],
    outputs_path=Path("outputs/policy-corpus-demo"),
    include_translations=True,
    translated_terms=[
        "planification de l'espace maritime",
        "energie renouvelable en mer",
    ],
    include_nim=True,
    include_nim_fulltext=True,
    nim_max_rows=None,
)

print(result.final_corpus_path)
print(result.manifest_path)
print(result.final_document_count)
```

`build_policy_corpus(...)` prints a lightweight progress stream while it runs, writes intermediate and final corpus artifacts to disk, and returns a stable `PolicyCorpusBuildResult` object for programmatic use.

## What It Writes To Disk

Given `outputs_path="outputs/policy-corpus-demo"`, the top-level builder writes:

- `outputs/policy-corpus-demo/cache/`
- `outputs/policy-corpus-demo/jurisdictions/eu/documents.jsonl`
- `outputs/policy-corpus-demo/jurisdictions/uk/documents.jsonl`
- `outputs/policy-corpus-demo/jurisdictions/ca/documents.jsonl`
- `outputs/policy-corpus-demo/jurisdictions/aus/documents.jsonl` when selected
- `outputs/policy-corpus-demo/jurisdictions/nz/documents.jsonl` when selected
- `outputs/policy-corpus-demo/jurisdictions/us/documents.jsonl` when selected
- `outputs/policy-corpus-demo/final/documents.jsonl`
- `outputs/policy-corpus-demo/nim/documents.jsonl` when `include_nim=True` and NIM results are produced
- `outputs/policy-corpus-demo/run-manifest.json`

The final merged corpus is always written to `final/documents.jsonl`.

## How `include_translations` Works

`include_translations` only affects the EU path.

- If `include_translations=False`, the EU branch runs only `query_terms`.
- If `include_translations=True`, the EU branch runs both `query_terms` and `translated_terms`.
- Non-EU jurisdictions continue to run only `query_terms`.

This keeps the top-level API simple while preserving the current supported workflow boundary.

## How `include_nim` Works

`include_nim` only does anything when `EU` is included in `jurisdictions`.

- The main EU corpus is built first through the ordinary EUR-Lex path.
- CELEX identifiers are extracted from the EU results.
- Those CELEX identifiers are filtered to eligible EU legal acts only.
- Only eligible legal-act CELEXs compatible with the supported NIM path are used to seed the existing EUR-Lex NIM workflow.
- In practice, NIM seeds must normalize to sector-3 legal acts with descriptor `L`, `R`, or `D`.
- NIM results are written to a separate corpus under `nim/documents.jsonl`.

NIM is not merged into the main final corpus. The main final corpus remains the merged, deduplicated jurisdiction corpus only.

If the EU result set contains no eligible legal-act CELEXs, NIM is skipped cleanly. In that case the top-level builder still succeeds, reports the skip in progress output and the run manifest, and does not write a NIM corpus file.

NIM runtime can be controlled with two optional top-level arguments:

- `include_nim_fulltext=True` preserves the default behavior and retrieves NIM full text.
- `include_nim_fulltext=False` still retrieves and writes normalized NIM measure records, but skips the slower NIM full-text retrieval stage.
- `nim_max_rows=None` preserves the default behavior and processes all NIM measure rows returned by the supported workflow.
- `nim_max_rows=100` limits NIM processing to the first 100 national measure rows per NIM seed, which is useful for quick inspection runs.

## Public Result Object

`build_policy_corpus(...)` returns a stable `PolicyCorpusBuildResult` object. It includes:

- selected jurisdictions
- query terms
- whether EU translations were included
- whether NIM was included
- per-jurisdiction output paths and document counts
- final corpus path
- NIM corpus path when produced
- merged document count before final deduplication
- final document count
- duplicates removed
- manifest path

For programmatic consumption, `PolicyCorpusBuildResult.to_dict()` returns a stable summary payload, and the run manifest on disk mirrors that same top-level contract.

## Progress Output

The builder emits lightweight progress messages directly from `build_policy_corpus(...)`.

At minimum it reports:

- pipeline start and input validation
- each selected jurisdiction starting
- each selected jurisdiction raw hit count
- each selected jurisdiction finishing with normalized and full-text document counts
- whether NIM is running or skipped
- NIM seed candidate and eligible seed counts
- NIM national measure counts before full-text retrieval
- NIM full-text retrieval progress when enabled
- final merge and deduplication
- duplicates removed, final document count, final output write, and completion

The goal is readable build-stage visibility, not verbose logging.

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

## What v0.1 Includes

- one normalized document model: [src/policy_corpus_builder/models.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/models.py)
- one supported top-level builder: `build_policy_corpus(...)`
- one supported ordinary EUR-Lex adapter path: `eurlex`
- one supported EUR-Lex NIM adapter path: `eurlex-nim`
- supported live non-EU paths: `non-eu` with `countries = ["UK"]`, `countries = ["CA"]`, `countries = ["AUS"]`, `countries = ["NZ"]` with an API key, and `countries = ["US"]` with `REGULATIONS_GOV_API_KEY`
- deterministic final deduplication
- JSONL corpus export
- machine-readable run manifest export

## Supported Surface

The currently supported workflows are:

- UK via `adapter = "non-eu"` with `countries = ["UK"]`
- Canada via `adapter = "non-eu"` with `countries = ["CA"]`
- Australia via `adapter = "non-eu"` with `countries = ["AUS"]`
- US via `adapter = "non-eu"` with `countries = ["US"]`
- New Zealand API mode via `adapter = "non-eu"` with `countries = ["NZ"]` and `nz_mode = "api"`
- ordinary EUR-Lex via `adapter = "eurlex"`
- EUR-Lex NIM via `adapter = "eurlex-nim"`

The main public API worth treating as stable is:

- `build_policy_corpus` in [src/policy_corpus_builder/corpus_builder.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/corpus_builder.py)
- `PolicyCorpusBuildResult` in [src/policy_corpus_builder/corpus_builder.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/corpus_builder.py)
- `get_adapter` in [src/policy_corpus_builder/adapters/__init__.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/__init__.py)
- `LocalFileAdapter` in [src/policy_corpus_builder/adapters/local_file.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/local_file.py)
- `NonEUAdapter` in [src/policy_corpus_builder/adapters/non_eu_adapter.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/non_eu_adapter.py)
- `EurlexAdapter` in [src/policy_corpus_builder/adapters/eurlex_adapter.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex_adapter.py)
- `EurlexNIMAdapter` in [src/policy_corpus_builder/adapters/eurlex_nim_adapter.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex_nim_adapter.py)

The lower-level adapter/config workflow remains supported for advanced integrations, but it is no longer the first workflow users should reach for.

For a stable summary of supported versus provisional code paths, see [docs/supported-surface.md](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/docs/supported-surface.md).

## Lower-Level Config And Adapter Usage

If you need direct control over config files, adapters, query loading, normalization, or exports, those surfaces still exist. They are now secondary to `build_policy_corpus(...)`.

The most relevant lower-level functions are:

- `load_and_validate_config` in [src/policy_corpus_builder/config.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/config.py)
- `load_queries` in [src/policy_corpus_builder/queries.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/queries.py)
- `normalize_adapter_results` in [src/policy_corpus_builder/pipeline.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/pipeline.py)
- `deduplicate_documents` in [src/policy_corpus_builder/postprocess.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/postprocess.py)
- `export_documents_jsonl` in [src/policy_corpus_builder/exporters/jsonl.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/exporters/jsonl.py)
- `run_from_config_path` in [src/policy_corpus_builder/orchestration.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/orchestration.py)

### Example: Lower-Level Library Usage

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

The CLI remains config-oriented and runs the lower-level config pipeline rather than the new top-level builder.

## Provisional And Internal Surface

The following modules and workflows are intentionally not part of the supported public surface:

- legacy migrated [src/policy_corpus_builder/adapters/eurlex.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex.py)
- legacy migrated [src/policy_corpus_builder/adapters/eurlex_nim.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/eurlex_nim.py)
- [src/policy_corpus_builder/adapters/non_eu.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/non_eu.py)
- notebook-era diagnostics, bulk loaders, cache summaries, and summary helpers inside those migrated modules
- New Zealand `nz_mode = "auto"` as a supported contract
- New Zealand `nz_mode = "scrape"` as a supported contract
- placeholder and demo-only surfaces such as `placeholder` and [examples/minimal.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/minimal.toml)

Examples and notebooks are documentation aids, not stable implementation entry points.

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

## Supported Australia Workflow

The current supported Australia live workflow uses the `non-eu` adapter with `countries = ["AUS"]`.

What it supports today:

- query-driven Australia legislation discovery
- normalized JSONL export through the shared document model

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

## Supported US Workflow

The current supported US live workflow uses the `non-eu` adapter with `countries = ["US"]`.

What it supports today:

- Regulations.gov document discovery through the official `/v4/documents` API
- API-backed document metadata retrieval for normalized corpus text
- normalized JSONL export through the shared document model

What it requires:

- `REGULATIONS_GOV_API_KEY`

The supported US example config is [examples/non_eu_us.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/non_eu_us.toml).

## Supported EUR-Lex Workflow

The current supported EUR-Lex live workflow uses the `eurlex` adapter with the ordinary EU WebService search path plus ordinary EU CELEX full-text retrieval.

What it supports today:

- query-driven EUR-Lex WebService search
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

## Config Shape

The lower-level config system still uses TOML with five top-level sections:

- `[project]`
- `[queries]`
- `[[sources]]`
- `[normalization]`
- `[export]`

The bundled example config is [examples/local_file.toml](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/local_file.toml).

## Repository Scope

This repository is intentionally separate from project-specific downstream analysis work.

`policy-corpus-builder` is for:

- source access
- normalization
- corpus cleaning
- corpus export
- stable top-level corpus building

It is not for:

- research questions
- project-specific dictionaries
- report generation
- project-specific analysis logic
