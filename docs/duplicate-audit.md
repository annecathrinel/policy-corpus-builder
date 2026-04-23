# Duplicate Audit

The duplicate audit is observational only. It does not change retrieval, merging, or exact deduplication behavior.

Top-level corpus builds write four audit artifacts under `outputs_path/audit/`:

- `likely_duplicates.csv`: one row per document per likely duplicate group
- `likely_duplicates.jsonl`: JSONL equivalent of the row-level CSV audit
- `duplicate_groups_summary.csv`: one compact row per likely duplicate group for manual review
- `duplicate_groups_summary.json`: aggregate review counts and top candidate groups

The grouped CSV includes the duplicate group ID, signal, group size, representative value, involved jurisdictions and source names, publication date range, cross-jurisdiction/source flags, document IDs, and review-interest reasons. The JSON summary includes total group and document counts, groups by signal, individual jurisdiction/source involvement, jurisdiction/source combinations, largest groups, and top review candidates.
