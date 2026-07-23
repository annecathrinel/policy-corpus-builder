# Contributing to policy-corpus-builder

Thanks for your interest in contributing. This project is primarily maintained
as part of the NiD4Ocean research project, but external contributions, bug
reports, and adapter improvements are welcome.

## Getting started

1. Fork and clone the repository.
2. Create a virtual environment with **Python 3.11 or newer** (the package
   relies on the standard-library `tomllib`, added in 3.11).
3. Install the package in editable mode with its test dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

4. Run the test suite before making changes, to confirm a clean baseline:

   ```bash
   pytest
   ```

## Making changes

- Keep changes scoped: prefer several small, reviewable commits over one
  large one.
- Add or update tests for any behavior change. Tests live in `tests/` and
  mirror the module they cover (e.g. `tests/test_non_eu_uk.py` for
  `src/policy_corpus_builder/adapters/non_eu.py`'s UK retrieval code).
- If you change retrieval behavior for a jurisdiction/adapter, check whether
  `docs/supported-surface.md` needs updating — it documents which
  jurisdictions and workflow modes are considered stable/supported versus
  provisional/internal.
- If you're adding a new adapter or jurisdiction, start with
  `docs/adapter-authors.md`, which documents the adapter extension contract.
- Run `pytest` again before opening a pull request. CI will also run the
  suite automatically on your PR.

## Reporting bugs

Please open a GitHub issue with:

- The command or config you ran.
- The full error/traceback.
- Which jurisdiction/adapter was involved, if applicable (live retrieval
  behavior depends on upstream government sites and APIs, which can change
  without notice — it's helpful to know which upstream source was involved).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you agree to abide by its terms.
