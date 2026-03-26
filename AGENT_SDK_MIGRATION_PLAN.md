# Agent SDK Migration Plan

## Goal

Replace the bespoke conversation harness with the OpenAI Agents SDK while preserving Nisaba's core retrieval model:

- one Australian domestic route covering ABS plus curated Australian sources
- one macro route for global comparison sources
- source-specific retrieval adapters behind stable tool contracts
- the current frontend request and response contract
- progress updates during a run, followed by a final answer
- existing chart rendering and Excel export capability

The migration should remove custom orchestration code, not discard the working retrieval layer.

## What Stays

Keep these as the durable domain layer:

- domestic merged catalog and FTS search
- custom Australian catalog entries in `CUSTOM_AUS_DATAFLOWS.json`
- ABS retrieval and metadata logic
- custom source adapters such as DCCEEW AES and RBA tables
- macro catalog and macro retrieval logic
- FastAPI app and frontend chat product surface

Relevant current files:

- `src/services/abs/DataFlowService.ts`
- `src/services/abs/DatasetResolver.ts`
- `src/services/custom/DcceewAesService.ts`
- `src/services/custom/RbaTablesCsvService.ts`
- `backend/app/macro_data.py`
- `src/index.ts`

## What Gets Replaced

Replace the bespoke orchestration layer in the backend:

- custom loop prompting
- custom JSON parser and step grammar
- fake tool step ids such as `provider_route_tool`, `aus_metadata_tool`, `aus_raw_retrieve_tool`, `macro_data_tool`, `sandbox_tool`
- manual multi-turn orchestration over the Responses API

Relevant current files:

- `backend/app/openai_service.py`
- `backend/app/harness/parser.py`
- `backend/app/harness/prompt_builder.py`
- `backend/app/harness/state.py`

## Target Architecture

### Control plane

Use the OpenAI Agents SDK in the Python backend for:

- sessions and conversation memory
- tool orchestration
- run progress events that can be surfaced to the frontend
- approvals if needed
- tracing
- final response generation

The user-facing API should remain the same:

- frontend sends chat messages the same way it does now
- backend emits progress updates during the run
- backend returns the final assistant answer in the same product flow
- chart artifacts and Excel exports remain available

### Tool plane

Expose retrieval through real tool surfaces instead of private harness steps.

Preferred shape:

- `domestic.search_catalog`
- `domestic.get_metadata`
- `domestic.retrieve`
- `macro.search_catalog`
- `macro.get_metadata`
- `macro.retrieve`

Optional later:

- `analysis.run_python`
- `analysis.export_table`
- `analysis.export_workbook`

### MCP shape

The current Node MCP server should evolve from an ABS-only server into a domestic MCP server for all Australian domestic sources.

That means:

- keep one domestic search experience
- shortlist ABS and custom Australian flows together
- branch internally by `flowType`
- keep model-facing contracts stable across source types

The model should not need to reason directly about:

- `flowType`
- `sourceType`
- `requiresMetadataBeforeRetrieval`

Those remain implementation details inside the domestic tools.

## Recommended Runtime Split

Use the Python Agents SDK in the FastAPI backend as the main orchestrator.

Connect it to:

- the existing Node MCP server over stdio for domestic tools
- a dedicated macro MCP server for macro tools once that surface is extracted cleanly

Why this split:

- it preserves working TypeScript retrieval code
- it keeps orchestration in the existing Python product backend
- it lets us replace orchestration first
- it allows route-level MCP boundaries without forcing per-dataset MCPs

## Recommended Topology

This repo should target:

- one MCP server for the Australian domestic route
- one MCP server for the macro route
- one Agent SDK orchestrator in the Python backend that uses both

This is the recommended setup for this codebase.

### Why this is the best setup

#### Domestic should be MCP

Domestic retrieval already has the right MCP shape:

- one merged Australian catalog
- one shortlist experience
- multiple source adapters hidden behind stable retrieval contracts

That means the domestic route benefits directly from MCP:

- ABS and custom Australian sources can be exposed through the same tool surface
- the Agent SDK can call the domestic tools without knowing which adapter sits underneath
- custom Australian integrations become reusable tool infrastructure instead of harness-only branches

#### Macro should also become MCP

Macro is a distinct product route with its own shortlist and retrieval flow.

Making macro its own route-level MCP server gives:

- symmetry with the domestic route
- clean route boundaries in the Agent SDK tool layer
- reusable discovery and retrieval tools for macro outside the webapp
- less route-specific backend glue over time

The important constraint is still the same:

- one MCP per route
- not one MCP per dataset
- not one tool per dataset

### Alternatives and why they are weaker

#### One MCP server for all data

Possible, but not the best first target.

Why weaker:

- domestic and macro are different product routes
- they use different shortlist and retrieval logic
- combining everything into one MCP server too early blurs the architecture
- it increases the chance of leaking route-specific complexity into tool schemas

This can be revisited later if there is a strong operational reason to consolidate.

#### One MCP server per dataset

Do not do this.

Why wrong:

- the tool surface would become enormous
- catalog search becomes fragmented
- custom and ABS flows would stop feeling like one domestic route
- maintenance would become impractical

Datasets should be selected by tool input, not represented as separate MCP servers.

#### Zero macro MCP servers

Viable as a temporary implementation stage, but not the best final architecture.

Why weaker as an end state:

- it creates an asymmetry between your two top-level routes
- it leaves macro as backend-internal logic instead of reusable tool infrastructure
- it keeps more route-specific glue in the orchestrator than necessary

## Target Tool Surfaces

### Domestic MCP tools

The domestic MCP server should expose a small stable set of tools:

- `search_catalog`
- `get_metadata`
- `retrieve`

Optional later:

- `list_sources`
- `inspect_dataset_shape`

The server itself represents the domestic route. Tool names do not need to be prefixed with `domestic_` if the server identity is already domestic-focused.

### Macro MCP tools

The macro MCP server should expose a matching small stable set of tools:

- `search_catalog`
- `get_metadata`
- `retrieve`

As with domestic, the server identity carries the route meaning.

## Frontend and API Stability

The current frontend message flow should remain intact.

Implementation constraint:

- no frontend product rewrite as a prerequisite for the migration

Target behavior:

- the frontend sends a message the same way it does now
- the backend starts an Agent SDK run
- progress messages from the run are exposed through the existing conversation flow or a compatible extension
- the final answer lands in the same chat surface
- chart outputs still render in the frontend
- Excel export remains available when the run produces exportable artifacts

If transport changes are needed for better live progress, prefer extending the backend transport while keeping the frontend contract materially the same.

## Migration Phases

## Phase 0: Plan and freeze interfaces

Deliverables:

- this plan
- agreed target tool names and schemas
- explicit cutover rule: replace orchestration first, adapters second

Acceptance criteria:

- no new bespoke harness step types are added
- all new retrieval work targets stable tool contracts

## Phase 1: Normalize domestic MCP around real domestic tools

Changes:

- rename the server identity from ABS-only language to domestic Australian language
- ensure tool descriptions say domestic includes ABS plus curated Australian sources
- keep merged catalog search as the default shortlist entrypoint
- ensure metadata and retrieval work for both ABS and custom sources behind the same tool contract

Deliverables:

- updated MCP server naming and descriptions
- stable domestic tool schemas
- end-to-end verification for ABS, DCCEEW AES, and RBA examples

Acceptance criteria:

- an agent can use one domestic tool surface without caring whether the selected dataset is ABS or custom

## Phase 2: Add an Agents SDK orchestrator alongside the current harness

Changes:

- add a new backend module for Agents SDK orchestration
- wire session-backed runs in parallel with the current harness path
- register the domestic MCP server in the orchestrator
- keep the existing API route shape stable for the frontend
- map Agent SDK progress events into the current user-visible progress model

Deliverables:

- new orchestrator module
- feature flag or config switch between old harness and Agent SDK path
- backend progress event bridge for in-run updates

Acceptance criteria:

- the new path can answer domestic questions through MCP tools
- the frontend can use either path without interface changes
- the user sees progress updates during the run

## Phase 3: Extract the macro route into its own MCP server

Changes:

- create a dedicated macro MCP server around the existing macro shortlist and retrieval layer
- expose a stable route-level macro tool surface
- validate parity against current macro behavior

Deliverables:

- macro MCP server
- route-level macro tools for search, metadata, and retrieval

Acceptance criteria:

- macro discovery and retrieval work through the macro MCP server
- no per-dataset MCP surface exists
- the Agent SDK orchestrator can use both route MCPs

## Phase 4: Replace custom routing and loop grammar

Changes:

- remove custom step parsing and loop control from the active path
- replace `provider_route_tool` with natural tool choice between domestic and macro MCPs
- move any remaining shortlist precomputation behind tool calls

Deliverables:

- no active dependency on the harness parser for the new path
- reduced prompt complexity

Acceptance criteria:

- orchestration decisions come from the Agent SDK tool loop, not repo-specific JSON step contracts

## Phase 5: Preserve and rationalize analysis, charts, and exports

Changes:

- preserve chart generation and Excel export in the new path
- wrap current analysis and export capabilities in Agent SDK-compatible tools or post-run services
- decide whether the sandbox remains custom behind a tool boundary or is replaced later

Recommendation:

- keep the current sandbox initially
- keep chart and workbook generation working before removing the old harness

Reason:

- retrieval migration and orchestration migration are already large enough
- analysis code is a separate risk surface

Acceptance criteria:

- progress, charts, and Excel outputs are still available on the Agent SDK path

## Phase 6: Remove dead harness code

Changes:

- remove old harness prompt builder, parser, and loop machinery
- simplify backend logging and state handling

Acceptance criteria:

- only one orchestrator path remains
- run traces and tool execution are understandable without repo-specific recovery logic

## Tool Contract Principles

### Domestic tools

Domestic tools should support:

- shortlist search over merged Australian catalog
- metadata-first retrieval where required
- direct retrieval where `requiresMetadataBeforeRetrieval` is false
- consistent normalized outputs across ABS and custom sources

### Macro tools

Macro tools should support:

- shortlist search over macro catalog
- indicator metadata lookup
- retrieval from the chosen provider

### Output conventions

Tool outputs should be:

- structured JSON
- source-grounded
- explicit about selected dataset or candidate id
- explicit about source references and retrieval caveats

## Risks

### Biggest risk

Trying to rewrite orchestration and every adapter at the same time.

Avoid that.

### Other risks

- exposing too many low-level tools to the model
- leaking internal adapter details into prompts
- breaking current domestic shortlist behavior
- coupling macro and domestic contracts too early
- replacing the sandbox before the orchestration cutover is stable

## First Implementation Order

Implement in this order:

1. reframe the current MCP server as domestic, not ABS-only
2. make sure domestic tool descriptions reflect merged ABS plus custom Australian retrieval
3. add a new Agent SDK backend path that can call the domestic MCP server with the current frontend contract unchanged
4. surface Agent SDK progress through the existing conversation flow
5. extract macro into its own MCP server
6. keep the old harness behind a flag until parity is proven

## Definition of Done

This migration is complete when:

- the web app uses the Agent SDK path by default
- domestic retrieval works through one unified domestic tool surface
- custom Australian sources are available as normal domestic tools, not special harness branches
- macro retrieval works through its own route-level MCP tools
- users receive progress updates during runs
- charts and Excel exports still work
- the old harness loop is removed
