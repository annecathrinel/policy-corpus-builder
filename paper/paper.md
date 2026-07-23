---
title: 'policy-corpus-builder: A Python toolkit for reproducible, multi-jurisdiction policy and legal document corpus building'
tags:
  - Python
  - policy analysis
  - legal informatics
  - text corpus
  - EUR-Lex
  - environmental governance
  - research software
authors:
  - name: Anne Cathrine Linder
    affiliation: 1
affiliations:
  - name: DTU Aqua, National Institute of Aquatic Resources, Technical University of Denmark, Denmark
    index: 1
date: 23 July 2026
bibliography: paper.bib
---

# Summary

`policy-corpus-builder` is a Python toolkit for retrieving, cleaning, and
exporting policy and legal documents into reproducible text corpora for
research. It provides a single top-level function, `build_policy_corpus`,
and an equivalent command-line interface, that take a list of search terms
and jurisdictions and return a normalized, deduplicated corpus on disk. A
pluggable adapter layer covers EU law through EUR-Lex, including national
implementing measures (NIM) for EU directives, alongside UK, Canadian,
Australian, US, and New Zealand government sources. Regardless of source,
every retrieved document is mapped onto one shared schema, deduplicated
using transparent, auditable rules, and exported as JSON Lines alongside a
machine-readable manifest recording exactly what was retrieved, when, and
from where. The package is designed to be reused across research projects
rather than tied to one study: it deliberately does not include any
project-specific dictionaries, research questions, or analysis logic.

# Statement of need

Empirical research on environmental, energy, and biodiversity governance
increasingly depends on large, systematically retrieved corpora of
legislation and regulatory documents, for example to measure how ambition
in high-level policy commitments does or does not translate into
operational design requirements. In practice, this retrieval work is
usually done with disposable, project-specific notebooks and scripts:
one-off scraping code that is difficult to rerun, difficult to validate,
and difficult to extend to a new jurisdiction. `policy-corpus-builder`
addresses this gap directly. It grew out of exactly this kind of ad hoc
retrieval work, carried out for the governance-analysis work package of
the NiD4Ocean project on nature-inclusive design in offshore renewable
energy, and was deliberately generalized into standalone, reusable
infrastructure rather than kept as project-internal code. Its target
users are researchers in legal informatics, environmental and energy
governance, and computational social science who need a documented,
tested, and reproducible pipeline from live legal sources to an
analysis-ready corpus, without needing to write and maintain bespoke
retrieval code for each jurisdiction themselves.

# State of the field

Existing tools address adjacent but distinct parts of this problem.
`eurlex` [@Ovadek2021] is an R package that wraps the EU Publication
Office's SPARQL and REST endpoints, and is the closest existing analogue
for EU-law retrieval; it is R-based, EU-only, and oriented toward bulk
metadata rather than a normalized, multi-jurisdiction, full-text corpus
with deduplication and export tooling. `LexNLP` [@Bommarito2018] is a
Python package for extracting structured information from legal and
regulatory text, but it assumes the text has already been acquired, and
its models and examples are drawn primarily from US regulatory and
judicial sources rather than comparative, multi-jurisdiction retrieval.
Beyond these, jurisdiction-specific retrieval is typically done directly
against government APIs and websites (EUR-Lex, UK's legislation.gov.uk,
US regulations.gov, and equivalents in Canada, Australia, and New
Zealand) with no shared normalization layer across them. `policy-corpus-builder`
was built rather than extended from `eurlex` or `LexNLP` because neither
addresses the combination this project required: a Python-native,
config-driven or single-function retrieval-to-corpus pipeline that spans
EU and non-EU jurisdictions, normalizes heterogeneous source formats
(SOAP/XML, REST JSON, and HTML with anti-bot handling) into one document
schema, and produces auditable, reproducible outputs suitable for citing
in downstream research.

# Software design

The package is organized around a pluggable adapter interface: each
jurisdiction or source implements retrieval and full-text extraction
behind a common contract, and all adapter output is normalized into a
single `NormalizedDocument` model before deduplication and export, so
that downstream analysis code never needs to know which adapter produced
a given record. Two usage layers are exposed deliberately: a single
top-level `build_policy_corpus` function/CLI command for the common case,
and a lower-level, TOML-config-driven pipeline for advanced or
non-standard retrieval configurations. Because live retrieval against
external government systems is inherently less reliable than most
software dependencies, the design treats reliability as something to be
documented and monitored rather than assumed: a supported-surface
document distinguishes stable retrieval paths from provisional ones, HTTP
calls to flaky endpoints (including the EUR-Lex NIM SOAP service and its
LexUriServ full-text backend) use explicit retry/backoff logic informed
by real production failures rather than only handling the common
success case, and every run writes a manifest plus a transparent,
non-authoritative duplicate-audit report rather than deduplicating
silently. Retrieval, normalization, and export logic are also
intentionally kept separate from any project-specific research
dictionaries or analysis code, which is what allows the same package to
be reused unchanged across projects with different research questions.

# Research impact statement

`policy-corpus-builder` currently underpins the corpus-construction stage
of the NiD4Ocean project's WP5 governance analysis of nature-inclusive
design in offshore renewable energy. A production run of the package
retrieved and deduplicated 12,558 raw hits into a 7,758-document corpus
across five jurisdictions (EU, UK, Canada, Australia, US), and that
corpus is the basis for a completed baseline analysis (RQ0-RQ4) whose
headline finding — that 96.0% of EU documents show biodiversity/restoration
ambition but only 8.9% show design-level specificity, and 0.2% use
explicit nature-inclusive-design language — is now being prepared for
publication. A second, independent robustness check against the full
EUR-Lex legal-acts dump (43,466 acts) reproduced the same pattern at
scale. As a first tagged release, the package does not yet have external
citations or a public user base beyond its originating project; its
near-term community-readiness case rests on concrete, verifiable
artifacts rather than aspiration: a documented, versioned public API, an
automated test suite (152 tests as of this release) exercised in
continuous integration, and machine-readable run manifests that make
every corpus it produces independently auditable.

# AI usage disclosure

Generative AI (Claude, Anthropic) was used during parts of this
software's development, including diagnosing test failures, writing and
extending the automated test suite, adding retry/backoff handling for
external HTTP calls, and drafting repository scaffolding such as the
continuous integration configuration, contributor documentation, and an
earlier draft of this manuscript. All AI-assisted code changes were
reviewed by the author and validated against the automated test suite
before being accepted; changes to live retrieval behavior were
additionally verified against real runs on production HPC infrastructure
before being treated as confirmed.

# Acknowledgements

This work was supported by the NiD4Ocean project. We acknowledge the use
of DTU's high-performance computing cluster for production corpus-build
validation.

# References
