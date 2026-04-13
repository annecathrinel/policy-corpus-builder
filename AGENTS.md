# AGENTS.md

## Repository Intent

This repository is a reusable public tool, not a project-specific research workspace.

When contributing here:

- prefer generic policy-corpus abstractions over source- or project-specific logic
- keep retrieval concerns separate from downstream analysis
- avoid embedding `NiD` assumptions, research questions, or report logic
- keep changes small, reviewable, and easy to reuse outside the original project

## Current Bootstrap Constraints

- do not implement full retrieval pipelines unless explicitly requested
- do not add source-specific crawlers beyond minimal placeholders
- prefer stdlib-first choices when they keep the tool simple
- treat the CLI and normalized metadata schema as public interfaces

## Near-Term Priorities

1. stabilize config loading and validation
2. define the normalized document metadata schema
3. add a first real adapter behind a generic adapter interface
4. add export and deduplication layers without coupling to any analysis repo
