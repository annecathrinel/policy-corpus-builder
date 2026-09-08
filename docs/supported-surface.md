# Supported Surface

This page defines the currently supported workflows and the implementation surface that downstream users should treat as public.

The repository is first and foremost built around EUR-Lex / EU retrieval. The ordinary EUR-Lex workflow is currently the primary, best-supported, and most robust live retrieval surface.

All live retrieval workflows depend on external systems outside this package. Government websites, public APIs, search endpoints, page structures, robots policies, anti-bot controls, access restrictions, and other upstream behavior can change without notice. When that happens, retrieval may degrade or stop working until the relevant adapter is updated.

## Supported Workflows

- ordinary EUR-Lex via `adapter = "eurlex"`; primary and strongest retrieval surface today
- EUR-Lex NIM via `adapter = "eurlex-nim"`
- UK via `adapter = "non-eu"` with `countries = ["UK"]`
- Canada via `adapter = "non-eu"` with `countries = ["CA"]`
- Australia via `adapter = "non-eu"` with `countries = ["AUS"]`
- US via `adapter = "non-eu"` with `countries = ["US"]`
- New Zealand API mode via `adapter = "non-eu"` with `countries = ["NZ"]` and `nz_mode = "api"`

The non-EU workflows listed here are supported live workflows. They are generally more contingent on each jurisdiction's current government website or API behavior, including service availability, response schemas, page markup, search endpoint behavior, scraping policies, and access controls.

## Retrieval Stability Expectations

Treat ordinary EUR-Lex retrieval as the strongest supported path today. It is the main workflow this repository is organized around.

Treat EUR-Lex NIM retrieval as supported, but narrower: it is seeded from EU legal acts and depends on NIM eligibility and the current EUR-Lex/NIM upstream behavior.

Treat non-EU retrieval as supported but more externally fragile. These workflows may require adapter updates when jurisdiction platforms change their APIs, websites, document structures, robots or scraping policies, or access restrictions.

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

### EUR-Lex full-text recovery

EU full-text retrieval tries encoded HTTPS Cellar URLs for XHTML and PDF,
then the EUR-Lex HTML and PDF endpoints
for each identifier/language. Parenthesized CELEX suffixes are preserved and
URL-encoded. PDFs use the existing pypdf dependency; scanned PDFs without
extractable text remain failures. Browser-verification responses and unavailable
representations are not exported as text.

NIM retrieval reads national document links from RDF link predicates, ignoring
schema/datatype URLs and unrelated navigation links. EUR-Lex NIM metadata pages
are not national legislation. A failed full-text fetch still retains the measure's
metadata, with no full text. National sites requiring JavaScript may remain unavailable.

Rerun the existing build to retry failed EU texts and refresh old NIM text caches.
NIM text files without a `.validated-v3` sidecar are fetched again once; validated
cache hits are included in resumed results. Existing cache files are retained.
Restart an active notebook kernel or Python process before rerunning to load these
changes. Previously exported corpora must be rebuilt to replace false successes.

### NIM overview before full text

`build_policy_corpus` discovers NIM metadata for **all** eligible CELEX seeds
before starting any NIM full-text downloads. It saves the following files under
`<outputs_path>/nim/overview/`:

- `nim_by_act_country.csv`: distinct NIM counts for each CELEX and country,
  including zero counts for EU countries with no discovered measures.
- `nim_by_act_country_year.csv`: distinct NIM counts by CELEX, country and NIM
  document year. Missing/invalid dates are `unknown`; absent combinations are omitted.
- `nim_by_act.csv`: act totals and discovery status/error. Failed discovery has
  blank counts rather than zero; a configured metadata page limit is flagged.
- `nim_inventory.csv`: the metadata records underlying the counts.
- `overview.json`: counting definitions and file names.

The year comes from `nim_date` (the national measure's document date), not the
EU act year, notification year, or full-text retrieval year. Counts deduplicate
national measure IDs within each CELEX/country and precede `nim_max_rows` limits.
The same measure implementing multiple EU acts counts once for each act.
The overview remains on disk if text retrieval is interrupted and is also created
with `include_nim_fulltext=False`. Direct single-query adapter calls save their
query overview in the adapter cache's `overview/` directory.

NIM retrieval follows offered document downloads before accepting national
landing-page text. It no longer tries EUR-Lex metadata pages as full-text fallbacks:
missing national links, short text and national-site HTTP failures are reported
directly. The v3 cache marker refreshes older landing-page results once. Sites
requiring JavaScript or blocking requests can still fail.
