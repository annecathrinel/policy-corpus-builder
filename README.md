# policy-corpus-builder

`policy-corpus-builder` is a small Python toolkit for building clean policy document corpora.

Version `0.1` is intentionally narrow:

- load queries from a config file
- read structured local policy records with the `local-file` adapter
- normalize records into one shared `NormalizedDocument` model
- deduplicate deterministically
- export the final corpus to JSONL

The package is library-first. The CLI is a thin convenience layer on top of the same workflow.

## What v0.1 Includes

- one normalized document model: [src/policy_corpus_builder/models.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/models.py)
- one real adapter: `local-file`
- deterministic deduplication using configured normalized fields
- one export format: JSONL
- one end-to-end notebook example: [examples/notebooks/local_file_end_to_end.ipynb](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/examples/notebooks/local_file_end_to_end.ipynb)
- one minimal CLI for config validation and running the same workflow

## Small Public API

These are the main functions and modules worth treating as the v0.1 public surface:

- `load_and_validate_config` in [src/policy_corpus_builder/config.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/config.py)
- `load_queries` in [src/policy_corpus_builder/queries.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/queries.py)
- `get_adapter` in [src/policy_corpus_builder/adapters/__init__.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/adapters/__init__.py)
- `normalize_adapter_results` in [src/policy_corpus_builder/pipeline.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/pipeline.py)
- `deduplicate_documents` in [src/policy_corpus_builder/postprocess.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/postprocess.py)
- `export_documents_jsonl` in [src/policy_corpus_builder/exporters/jsonl.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/exporters/jsonl.py)
- `run_from_config_path` in [src/policy_corpus_builder/orchestration.py](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/src/policy_corpus_builder/orchestration.py)

## Install

```bash
pip install -e .
```

Python `3.11+` is required.

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
