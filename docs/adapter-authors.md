# Adapter Author Guide

Adapters should convert source-specific raw records into `AdapterResult` payloads that match the shared normalization contract.

## Where Mapping Lives

The reusable mapping helper lives in `src/policy_corpus_builder/adapters/mapping.py`.

Mappings are adapter-defined in code, not config-driven. Each adapter declares:

- which raw field supplies each normalized field
- any explicit defaults
- any source-specific validation it needs before mapping

## Required Normalized Fields

Adapters must supply these normalized fields:

- `document_id`
- `title`

In practice, adapters often also provide:

- `source_document_id`

The shared pipeline later adds:

- `source_name`
- `query`
- query provenance fields in `raw_metadata`

## Optional Normalized Fields

Adapters may map any of these when available:

- `source_document_id`
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

## How Field Mapping Works

Use `build_adapter_result(raw_record, field_mapping=..., defaults=...)`.

- `field_mapping` maps normalized field names to raw source field names
- `defaults` can fill a normalized field when the mapped raw field is absent
- required normalized fields are enforced by the helper
- optional normalized fields are omitted when missing or blank

## Missing Values

- required mapped fields must resolve to non-empty strings
- optional mapped fields may be missing
- optional mapped fields with blank string values are dropped
- adapters can still do pre-validation for clearer source-specific errors

## Unmapped Raw Fields

Unmapped source fields are preserved under `raw_record` inside `raw_metadata`.

This keeps source-specific leftovers available without polluting the shared normalized schema.
