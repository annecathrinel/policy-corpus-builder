This directory holds the small public-facing examples used by the v0.1 workflow.

Recommended starting points:

- `local_file.toml`: the main end-to-end example config
- `non_eu_uk.toml`: supported UK live retrieval example using the supported `non-eu` adapter
- `non_eu_canada.toml`: supported Canada live retrieval example using the supported `non-eu` adapter
- `non_eu_australia.toml`: supported Australia live retrieval example using the supported `non-eu` adapter
- `non_eu_new_zealand.toml`: supported New Zealand example only when run in API mode with `NZ_LEGISLATION_API_KEY`
- `non_eu_us.toml`: supported US live retrieval example using the supported `non-eu` adapter
- `eu.toml`: supported ordinary EUR-Lex example using the supported `eurlex` adapter
- `eu_nim.toml`: supported EUR-Lex NIM example using the supported `eurlex-nim` adapter
- `fixtures/policies.jsonl`: fixture data for the `local-file` adapter
- `queries/example_queries.txt`: example query inventory
- `queries/non_eu_canada_queries.txt`: small live query inventory for the Canada workflow
- `queries/non_eu_new_zealand_queries.txt`: small live query inventory for the New Zealand workflow
- `queries/non_eu_us_queries.txt`: small live query inventory for the US workflow
- `queries/non_eu_uk_queries.txt`: small live query inventory for the UK workflow
- `notebooks/local_file_end_to_end.ipynb`: notebook walkthrough of the library-first workflow

Also included:

- `minimal.toml`: a smaller placeholder-oriented config kept for internal structure and tests, not a supported retrieval workflow
- `fixtures/policies.json`: the same fixture data in JSON form

Notes:

- `non_eu_uk.toml` is a live network example and may take time to run.
- `non_eu_canada.toml` is a live network example and may take time to run.
- `non_eu_australia.toml` is a live network example and may take time to run.
- `non_eu_new_zealand.toml` should be treated as supported only in `nz_mode = "api"` with `NZ_LEGISLATION_API_KEY`. `auto` and `scrape` remain provisional because they can route into the legacy no-key fallback.
- `non_eu_us.toml` is a live network example and requires `REGULATIONS_GOV_API_KEY`.
- `eu.toml` is a live network example and requires `EURLEX_WS_USER` and `EURLEX_WS_PASS`. `EURLEX_USER` and `EURLEX_WEB_PASS` are accepted as compatibility aliases.
- `eu_nim.toml` is a live network example and requires `EURLEX_WS_USER` and `EURLEX_WS_PASS`. `EURLEX_USER` and `EURLEX_WEB_PASS` are accepted as compatibility aliases.
- The bundled US example query inventory is chosen to demonstrate file-backed Regulations.gov results rather than metadata-only fallback.
- The supported non-EU workflows are UK, Canada, Australia, US, and New Zealand API mode through the `non-eu` adapter.
- The Canada workflow now prefers the Open Government / CKAN API for discovery and resource metadata, and only falls back to `publications.gc.ca` landing-page extraction when needed.
- The UK workflow should be treated as discovery/metadata-supported first. It prefers the official `legislation.gov.uk` API for full text, but exported records may still have empty `full_text` when upstream access is challenged.
- The New Zealand workflow prefers the official `api.legislation.govt.nz` API for discovery. Without an API key it can still fall back to the older scraper path, but that fallback is provisional and outside the supported surface.
- A UK run can still be successful without full text. In that case, check `raw_metadata.raw_record.full_text_error` and `retrieval_status` in the exported JSONL.
- The notebook directory is documentation-oriented. It is useful for walkthroughs, but notebook files are not supported public implementation surfaces.
