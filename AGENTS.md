# AusData AI Harness Working Notes

## Purpose

This repo builds Nisaba: an agentic Australian data harness.

The core goal is not generic economic QA.
The goal is deep, grounded, detailed retrieval over Australian public data, with global macro sources available mainly for context and comparison.

## Current retrieval model

The harness has two top-level retrieval routes:

- `abs`
  - Australian domestic retrieval
  - includes both ABS API datasets and curated custom Australian sources
- `macro`
  - global macro retrieval
  - includes sources such as OECD, World Bank, and IMF

Important:

- `abs` no longer means ABS-only
- it is the broader Australian domestic route
- custom Australian sources should fit into the same domestic shortlist and retrieval flow where possible

## Australian domestic architecture

Australian domestic retrieval currently works like this:

1. the model routes to `abs`
2. the backend prepares a domestic shortlist
3. the shortlist searches across:
   - `ABS_DATAFLOWS_FULL.json`
   - `CUSTOM_AUS_DATAFLOWS.json`
4. SQLite FTS is used over the merged domestic catalog
5. retrieval then branches by source type behind the same domestic tool contract

Current merged FTS database:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/AUS_DOMESTIC_DATAFLOWS_FTS.sqlite3`

Current domestic source files:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/ABS_DATAFLOWS_FULL.json`
- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CUSTOM_AUS_DATAFLOWS.json`

## Custom Australian data

Custom Australian sources should not bypass the domestic route.

Preferred pattern:

- shortlist custom sources alongside ABS
- keep one domestic search experience
- branch internally at retrieval time using source-specific adapters

Current example:

- DCCEEW Australian Energy Statistics Table O workbook
- custom flow type: `dcceew_aes_xlsx`
- retrieved live from source
- not stored locally as raw workbook data

For custom sources:

- use the same broad dataflow layout as domestic catalog entries
- include source-specific fields such as `flowType`, `sourceType`, `sourceUrl`, and `requiresMetadataBeforeRetrieval`
- prefer runtime merging over physically mixing the ABS and custom source files

## Retrieval guidance

For ABS-backed domestic datasets:

- metadata-first retrieval is still the default
- metadata determines the valid anchor and wildcard shape
- retrieval should follow the observed dataset structure, not guessed keys

For custom-backed domestic datasets:

- a custom flow may declare `requiresMetadataBeforeRetrieval: false`
- in that case retrieval may run directly from the selected dataset id
- the adapter should inspect, slice, and normalize the source internally

General rule:

- use metadata when the source needs it
- do not force artificial metadata steps where the source does not need them

## Adding new Australian sources

Preferred process:

1. identify a real public Australian source worth supporting
2. add one clean domestic catalog entry to `CUSTOM_AUS_DATAFLOWS.json`
3. define the correct `flowType` and source fields
4. build a retrieval adapter only as far as needed for that source
5. verify that shortlist, retrieval, and downstream analysis all work end to end

Do not add speculative integrations.
Do not add local raw data mirrors unless there is a strong operational reason.
Prefer live retrieval from the public source when practical.

## Shortlist architecture overview

At a high level, both domestic and macro retrieval use a local catalog plus AI-generated shortlist queries.

- Australian domestic shortlist:
  - runs SQLite FTS over the merged domestic catalog
  - primarily matches `dataset_id`, `name`, and `description`
  - includes both ABS and curated custom Australian entries

- Macro shortlist:
  - runs SQLite FTS over `MACRO_CATALOG_FULL.json`
  - matches fields including `provider_name`, `concept_label`, `indicator_label`, `description`, and `search_text`
  - uses heavier reranking to improve provider and indicator relevance

The practical pattern is:

1. the model writes a shortlist query
2. local catalog FTS produces candidates
3. reranking improves ordering before retrieval continues

## Reliability standard

The repo should optimize for reliability, not theoretical coverage.

That means:

- prefer one working source integration over a broad speculative abstraction
- verify live retrieval against the real public source
- verify the backend can actually execute the retrieval path you are adding
- keep source descriptions grounded in what is actually retrievable

## Product identity notes

There is a repo-level narrative identity file at:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/SOUL.md`

Use it as the source of truth for naming, mythology, tone, and personality.
