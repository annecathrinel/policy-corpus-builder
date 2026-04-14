This directory holds the small public-facing examples used by the v0.1 workflow.

Recommended starting points:

- `local_file.toml`: the main end-to-end example config
- `non_eu_uk.toml`: live non-EU retrieval example using the supported `non-eu` adapter
- `fixtures/policies.jsonl`: fixture data for the `local-file` adapter
- `queries/example_queries.txt`: example query inventory
- `queries/non_eu_uk_queries.txt`: small live query inventory for the non-EU workflow
- `notebooks/local_file_end_to_end.ipynb`: notebook walkthrough of the library-first workflow

Also included:

- `minimal.toml`: a smaller placeholder-oriented config kept for internal structure and tests
- `fixtures/policies.json`: the same fixture data in JSON form

Notes:

- `non_eu_uk.toml` is a live network example and may take time to run.
- The first supported non-EU workflow is intentionally narrow: it uses the `non-eu` adapter with `countries = ["UK"]`.
- The UK workflow should be treated as discovery/metadata-supported first. It prefers the official `legislation.gov.uk` API for full text, but exported records may still have empty `full_text` when upstream access is challenged.
- A UK run can still be successful without full text. In that case, check `raw_metadata.raw_record.full_text_error` and `retrieval_status` in the exported JSONL.
