# AusData AI Harness Working Notes

## Purpose

This repo builds Nisaba: an agentic Australian data harness.

The core goal is not generic economic QA.
The goal is deep, grounded, detailed retrieval over Australian public data, with global macro sources available mainly for context and comparison.

## Current retrieval model

The harness now exposes one unified MCP.

Inside that MCP, retrieval still branches by source family:

- Australian domestic
  - includes both ABS API datasets and curated custom Australian sources
- global macro
  - includes sources such as OECD, World Bank, IMF, and UN Comtrade

Important:

- ABS is not ABS-only anymore
- it is the broader Australian domestic branch inside the unified MCP
- custom Australian sources should fit into the same shortlist and retrieval flow where possible

## Runtime shape

The repo is now Agent SDK based.

- main orchestration lives in:
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/backend/app/agents_service.py`
- the web backend lives in:
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/backend/app/main.py`
- the agent uses the OpenAI Agents SDK with a persisted SQLite session per conversation
- the app still keeps its own visible conversation state for UI/history/export purposes
- when runs fail, are cancelled, or are reset, the backend explicitly resyncs or clears the Agent SDK session so hidden SDK history does not drift from visible app history

Current MCP shape:

- unified MCP
  - Python stdio MCP server
  - entrypoint:
    - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/backend/app/unified_mcp_server.py`

There is now a repo-level MCP config at:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/.mcp.json`

The direction is increasingly MCP-first:

- reusable retrieval guidance should live in MCP server instructions and tool descriptions where practical
- app-only guidance such as progress updates, response style, and hosted UX behavior should stay in the Nisaba system prompt
- the web app should be treated as a layer on top of the MCP servers, not the only way the system can be used

## Unified catalog architecture

Retrieval now works like this:

1. the model calls unified `search_catalog`
2. SQLite FTS runs over the unified catalog
3. the model selects one dataset
4. unified `get_metadata` runs only when that source needs metadata
5. unified `retrieve` dispatches to the correct source adapter

Current unified build outputs:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/UNIFIED_CATALOG_FULL.json`
- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/UNIFIED_CATALOG_FTS.sqlite3`

Current manual source-definition file:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/MANUAL_SOURCE_DEFINITIONS.json`

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
- metadata is fetched live when needed and then transformed into a curated MCP-facing view
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
2. add one clean domestic catalog entry to `MANUAL_SOURCE_DEFINITIONS.json`
3. define the correct `flowType` and source fields
4. build a retrieval adapter only as far as needed for that source
5. verify that shortlist, retrieval, and downstream analysis all work end to end

Do not add speculative integrations.
Do not add local raw data mirrors unless there is a strong operational reason.
Prefer live retrieval from the public source when practical.

## Shortlist architecture overview

At a high level, retrieval uses one local catalog plus AI-generated shortlist queries.

- Unified shortlist:
  - runs SQLite FTS over `UNIFIED_CATALOG_FULL.json`
  - matches fields including `provider`, `dataset_id`, `title`, `description`, and `search_text`
  - returns candidates from both Australian domestic and macro sources
  - the catalog is a built snapshot, not a live provider catalog

Macro metadata behavior is provider-specific:

- World Bank
  - no separate metadata step after shortlist selection
- IMF
  - no separate metadata step after shortlist selection
- OECD
  - no separate metadata step after shortlist selection
- UN Comtrade
  - has a separate metadata step after shortlist selection
  - metadata comes from saved local file:
    - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/COMTRADE_METADATA.json`
  - that metadata can be rebuilt manually with:
    - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/scripts/build_comtrade_metadata.py`

Unified catalog refresh:

- the main unified catalog is a saved built file:
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/UNIFIED_CATALOG_FULL.json`
- it can be rebuilt manually with:
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/scripts/build_unified_catalog.py`

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

Operational note:

- if a change affects MCP usage, check both:
  - the app/harness path
  - the direct MCP path
- if a change affects frontend-only build behavior, make sure the frontend does not accidentally depend on the repo root package
- the frontend now includes a guard against reintroducing a local `file:..` dependency on the root MCP package
