# policy-corpus-builder

`policy-corpus-builder` is a reusable, public-facing tool for policy document querying, retrieval, metadata normalization, deduplication, and corpus export.

This repository is intentionally separate from `NiD-Policy-Analysis-clean`. The goal here is to provide a clean retrieval and corpus-building layer that can support many downstream analysis projects without embedding project-specific research logic.

## Current Scope

This bootstrap pass adds:

- a lightweight Python package skeleton
- a minimal CLI entry point
- a TOML-based configuration approach
- placeholder source adapter and schema modules
- example/config/test directories

This pass does **not** implement retrieval adapters, crawling, deduplication, or export pipelines yet.

## Design Direction

### Tool Responsibilities

`policy-corpus-builder` should own:

- configurable query inventories
- source adapter interfaces
- retrieval/crawling orchestration
- metadata normalization into a shared schema
- deduplication utilities
- export to common corpus formats

`NiD-Policy-Analysis-clean` should own:

- research questions and report generation
- domain dictionaries and coding schemes
- analysis notebooks and statistical workflows
- project-specific filtering, aggregation, and interpretation

### Proposed Package Layout

```text
src/policy_corpus_builder/
  __init__.py
  cli.py
  config.py
  models.py
  adapters/
    __init__.py
    base.py
  exporters/
    __init__.py
  schemas/
    __init__.py
```

### Proposed CLI Surface

Planned commands:

- `policy-corpus-builder run --config examples/minimal.toml`
- `policy-corpus-builder validate-config --config examples/minimal.toml`
- `policy-corpus-builder list-adapters`
- `policy-corpus-builder export --config ...`

Only `validate-config` and `list-adapters` are minimally stubbed right now. `run` exists as a placeholder to reserve the public interface.

### Config Approach

Use TOML for human-readable, versionable configuration with no mandatory external dependency on modern Python.

Validated top-level sections:

- `[project]`: run name, description, output directory
- `[queries]`: either an inventory path or inline query strings
- `[[sources]]`: source definitions, adapter names, and optional settings
- `[normalization]`: metadata mapping/dedup settings
- `[export]`: enabled output formats and destinations

Current validation rules include:

- all five top-level sections are required
- unknown top-level keys are rejected
- `queries` must define exactly one of `inventory` or `items`
- `queries.inventory` must point to an existing file, relative to the config file
- each source must reference a known adapter
- `normalization.deduplicate` must be boolean
- `normalization.deduplicate_fields` must reference known normalized metadata fields
- `export.formats` must be a non-empty subset of `jsonl`, `csv`, and `parquet`

### Metadata Schema

The normalized document model should stay intentionally generic and source-agnostic. Initial core fields:

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

The tool should allow source-specific metadata to remain in `raw_metadata` while mapping stable cross-source fields into the normalized layer.

### Export Formats

Recommended initial targets:

- `jsonl` for flexible pipelines
- `csv` for quick inspection and interoperability
- `parquet` for efficient larger corpora

Not all formats need to land first. `jsonl` is the safest initial implementation target.

### Source Adapter Structure

Each source adapter should eventually provide:

- a stable adapter name
- config validation
- query execution or crawl entry points
- raw result parsing
- normalization into the shared metadata schema

This bootstrap includes a base adapter protocol, one placeholder adapter, and one real local fixture-backed adapter.

Current bundled adapters:

- `placeholder`: deterministic non-network stub adapter for structure and testing
- `local-file`: fixture-backed adapter that reads local JSON or JSONL records from `source.settings.path`

The `local-file` adapter expects object records with:

- required fields: `id`, `title`
- optional fields: `summary`, `document_type`, `language`, `jurisdiction`, `publication_date`, `effective_date`, `url`, `download_url`, `retrieved_at`, `checksum`, `content_path`, `source_document_id`
- optional query matching field: `queries` by default, configurable with `source.settings.query_field`

When `queries` is present, records are emitted only for matching query text. When it is missing, records are treated as query-agnostic and may appear for every configured query. The normalized document `query` field is attached later by the shared pipeline using the active query that produced the record.

Adapter author notes now live in [docs/adapter-authors.md](C:/Users/acali/OneDrive%20-%20Danmarks%20Tekniske%20Universitet/PostDoc/Code/policy-corpus-builder/docs/adapter-authors.md).

## Minimal Usage

```bash
python -m policy_corpus_builder.cli list-adapters
python -m policy_corpus_builder.cli validate-config --config examples/minimal.toml
```

## Assumptions

- Python 3.11+ is acceptable, which lets us rely on `tomllib`.
- The first real implementation pass should prioritize config loading, normalized metadata models, and one end-to-end adapter prototype.
- Extraction from `NiD-Policy-Analysis-clean` should happen selectively later, after this repo defines stable public abstractions.
