# policy-corpus-builder

Build normalized policy-document corpora from EUR-Lex and supported government sources. The toolkit handles discovery, full-text retrieval, normalization, deduplication, and JSONL export. Project-specific analysis stays outside this repository.

Start with `build_policy_corpus(...)` in Python or `policy-corpus-builder build-corpus` in the terminal. EUR-Lex is the primary supported retrieval path; all live sources depend on upstream availability and access controls.

## Install

Requires Python **3.11+**. From this repository, install into your Python environment:

```bash
python -m pip install -e .
```

For the optional browser fallback used by NZ full-text retrieval:

```bash
python -m pip install -e ".[browser]"
python -m playwright install chromium
```

The browser fallback can help with access challenges, but does not guarantee successful downloads.

## Sources and credentials

| Code | Source | Required environment variables |
| --- | --- | --- |
| `EU` | EUR-Lex WebService and CELEX full text | `EURLEX_WS_USER`, `EURLEX_WS_PASS` |
| `UK` | legislation.gov.uk | None |
| `CA` | Government of Canada Publications | None |
| `AUS` | Federal Register of Legislation | None |
| `NZ` | New Zealand Legislation API | `NZ_LEGISLATION_API_KEY` |
| `US` | Regulations.gov API | `REGULATIONS_GOV_API_KEY` |

Set credentials in the environment. For an editable checkout, you can also copy [.env.example](.env.example) to an untracked `.env` in the repository root; the package loads it automatically. Existing environment values take precedence. Never commit credentials.

Legacy EUR-Lex names `EURLEX_USER` and `EURLEX_WEB_PASS` are also accepted. National implementation measures (NIM) use the same EUR-Lex credentials. Set `POLICY_CORPUS_BUILDER_USER_AGENT` to identify your application and contact details for non-EU requests.

## Quick start

### Python

```python
from policy_corpus_builder import build_policy_corpus

result = build_policy_corpus(
    query_terms=["marine spatial planning", "nature-based"],
    jurisdictions=["EU", "UK", "CA"],
    outputs_path="outputs/policy-corpus",
)

print(result.final_corpus_path)
print(result.final_document_count)
print(result.manifest_path)
```

### Terminal

```bash
policy-corpus-builder build-corpus --query-terms "marine spatial planning" "nature-based" --jurisdictions EU UK CA --outputs-path outputs/policy-corpus
```

Select only the jurisdictions you need. For a credential-free first run, use `--jurisdictions CA`.

```bash
policy-corpus-builder build-corpus --help
```

## Limits, concurrency, and logs

| Python option | CLI option | Default |
| --- | --- | --- |
| `non_eu_max_per_term` | `--max-per-term` | `None`: no non-EU per-term cap |
| `non_eu_max_workers` | `--max-workers` | 8 full-text workers per non-EU jurisdiction |
| `eu_max_workers` | `--eu-max-workers` | 4 EU full-text workers |
| `max_jurisdiction_workers` | `--max-jurisdiction-workers` | One worker per selected jurisdiction |
| `write_jurisdiction_logs` | `--jurisdiction-logs` / `--no-jurisdiction-logs` | Separate logs enabled |

An explicit positive `--max-per-term` still caps non-EU results. In a lower-level TOML config, omit `source.settings.max_per_term` for unlimited retrieval; an existing `max_per_term = 500` still imposes that limit. Unlimited removes the package's document cap, not upstream search or pagination restrictions, and does not change EU limits.

Worker counts control concurrent requests, not CPU allocation. Lower them to reduce load on upstream services.

The terminal shows run summaries by default. Detailed output goes to `logs/eu.log`, `logs/ca.log`, and equivalent files for other selected jurisdictions. Use `--no-jurisdiction-logs` to print those details inline.

### Cache and source behavior

- **EU:** successful full-text files are reused across terms and subsequent runs using the same output/cache directory. Missing cached text is fetched again. The lower-level EUR-Lex setting `use_cache = false` forces fresh text retrieval.
- **Non-EU:** successful full-text fetches are reused across terms within the same adapter run. Failed fetches remain eligible for another attempt.
- **Canada:** search uses `https://publications.gc.ca` without `www`, follows next-page links, and deduplicates results across pages. Verbose request failures include the underlying exception.
- **New Zealand:** multi-word and hyphenated terms such as `nature-based` are quoted for search; existing outer quotes are preserved. An API key is required.

Search matches do not guarantee relevant or complete full text. Inspect logs and `raw_metadata.raw_record` for retrieval errors and `term_verified` diagnostics. A completed run can retain metadata with missing full text when a source blocks or fails a download. Source search semantics vary; no cross-source exact-phrase guarantee is implied.

## Outputs

A top-level build writes under `outputs_path`:

```text
outputs/policy-corpus/
  cache/                        Reusable retrieval cache
  jurisdictions/<code>/documents.jsonl
  final/documents.jsonl         Merged, deduplicated corpus
  audit/                        Likely-duplicate review files
  logs/                         Per-jurisdiction logs
  run-manifest.json             Run settings, counts, and output paths
  nim/                          Optional national implementation outputs
  case_law/                     Optional case-law outputs
```

Each JSONL line is a [normalized document](src/policy_corpus_builder/models.py), with identifiers, title, source, jurisdiction, language, dates, URLs, query provenance, full text where available, and `raw_metadata` for source-specific details.

The builder standardizes whitespace, dates, language and document-type labels, and selected text boilerplate. Original values and date precision are retained in metadata where applicable.

The [duplicate audit](docs/duplicate-audit.md) flags likely duplicates for review; it does not remove additional records. `result.to_dict()` provides the run summary programmatically, including counts and artifact paths.

## Optional EU workflows

### Translated queries

Pass `include_translations=True` and `translated_terms=[...]` to search additional supplied terms in the EU path. Non-EU sources still use only `query_terms`. The toolkit does not generate translations.

CLI equivalents: `--include-translations --translated-terms "translated phrase"`.

### National implementation measures

Add `include_nim=True` to retrieve national implementation measures for eligible EU legal acts found by the main search. Include `EU` in `jurisdictions`.

```python
result = build_policy_corpus(
    query_terms=["marine spatial planning"],
    jurisdictions=["EU"],
    outputs_path="outputs/eu-with-nim",
    include_nim=True,
    include_nim_fulltext=False,
)
```

NIM full-text retrieval is enabled by default when NIM is requested. Use `include_nim_fulltext=False` or `--no-nim-fulltext` for metadata only. `nim_max_rows` / `--nim-max-rows` optionally limits processed measures per eligible EU act.

NIM records are written separately from the final corpus. Runs without eligible seeds skip NIM. Overview tables summarize acts, countries, and dates; failed discovery is distinguished from zero results, and page-limited counts are lower bounds. `nim_min_valid_year` (default 1950) affects timing summaries only, preserving raw dates and counts.

### Case law

Set `include_case_law=True` or `--include-case-law` to export case law identified in ordinary EU query results. This is not a targeted or comprehensive court search. CELEX sector 6 records and experimental sector 8 references remain separate categories; non-EU case-law retrieval is unsupported.

Full text is opt-in through `case_law_fulltext=True` / `--case-law-fulltext`, which requires case-law export to be enabled. Missing text does not discard discovered metadata. Counts describe exported source documents, not distinct proceedings or jurisdiction-wide totals.

See [supported workflows and output semantics](docs/supported-surface.md) for case-law rules and NIM overview details.

## Config-driven workflows

Use TOML when you need source-specific settings, a query inventory file, or local-file input:

```bash
policy-corpus-builder list-adapters
policy-corpus-builder validate-config --config examples/local_file.toml
policy-corpus-builder run --config examples/local_file.toml
```

Configs contain `[project]`, `[queries]`, `[[sources]]`, `[normalization]`, and `[export]`. Query inventories contain one term per line; blank lines and `#` comments are ignored, and literal quotes are preserved. Inventory paths resolve relative to the config file.

Start from an example: [local files](examples/local_file.toml), [EU](examples/eu.toml), [NIM](examples/eu_nim.toml), [UK](examples/non_eu_uk.toml), [Canada](examples/non_eu_canada.toml), [Australia](examples/non_eu_australia.toml), [NZ](examples/non_eu_new_zealand.toml), or [US](examples/non_eu_us.toml). Review their explicit limits before a production run; examples may intentionally restrict retrieval.

## Development and reference

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
```

- [Supported public interfaces and limitations](docs/supported-surface.md)
- [Writing adapters](docs/adapter-authors.md)
- [Examples](examples/README.md)
- [Build API and result object](src/policy_corpus_builder/corpus_builder.py)

Use the public builder and adapters for integrations. Legacy helpers and notebook-era internals are not stable public interfaces. Keep research dictionaries, reports, and downstream analysis in a separate project workspace.
