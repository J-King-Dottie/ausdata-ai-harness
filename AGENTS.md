# ABS MCP Working Notes

## Purpose

This repo now uses a deliberately narrow curated approach.

The goal is not to let the harness invent arbitrary ABS API calls.
The goal is to give it a short list of known working query templates.

## Current curated model

There are two source-of-truth files:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_CATALOG.txt`
- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_STRUCTURES.txt`

### Catalog file

The catalog file is intentionally simple.

Each entry contains:

- `dataset_id`
- `title`
- `description`

This is what the harness uses to decide whether a curated dataset is relevant.

### Structures file

The structures file is also intentionally simple.

Each dataset entry contains:

- `dataset_id`
- `title`
- `description`
- `query_templates`

Each `query_template` contains only:

- `template_id`
- `description`
- `api_call`

For measure-list templates such as the Labour Accounts wildcard template, each measure description is also part of the curated layer.

Those measure descriptions should say what is literally available in the returned data for that measure, not what the metadata or codelists imply might be available.

Example:

```json
{
  "template_id": "measure_wildcard",
  "description": "Choose a Labour Accounts measure and use the wildcard API call for that measure. Measure descriptions must say what the returned data literally includes.",
  "api_call": "/rest/data/LABOUR_ACCT_Q/{MEASURE}....?detail=full&dimensionAtObservation=TIME_PERIOD"
}
```

## Why this changed

The earlier broad approach exposed too much API surface:

- the model had to infer the right dimensions
- it had to infer the right code combinations
- it often built plausible-looking requests that failed

The curated template approach is more reliable because:

- every template is based on a real working ABS call
- the harness starts from known good patterns
- new capability is added by adding validated templates, not by widening model freedom

## How to add a new curated dataset or template

Use this workflow:

1. Pick a real user question.
2. Find a live ABS API call that actually works for that use case.
3. Test that call directly against ABS.
4. Add or update the catalog entry.
5. Add a matching structure entry with one or more `query_templates`.

Do not add speculative templates.
Only add templates that have been tested.
Do not add a template just because the API call returns data. Verify that the returned data actually contains the dimension/value combination the template claims to support.

If you are updating an existing wildcard-style template, also update the relevant descriptions so they reflect the verified returned data.

## How to test a live ABS API call

Use `curl`.

Example:

```bash
curl -sS "https://data.api.abs.gov.au/rest/data/LABOUR_ACCT_Q/M9...10+20+30.Q?detail=full&dimensionAtObservation=TIME_PERIOD"
```

If the call returns valid ABS data, it is a candidate template.
But it is only a valid curated template after you verify the returned content matches the intended use case.

If the call fails, do not add it to the curated file.

## Mandatory verification before writing a template

Always verify both:

1. The call succeeds.
2. The returned data actually contains the thing the template claims.

Examples:

- If a template says "jobs by state and industry", verify that the response contains multiple state codes, not just `AUS`.
- If a template says "manufacturing jobs", verify that the response actually contains the manufacturing code.
- If a template says "latest quarter by state", verify that state-level rows are present in the returned data.

If the response only partially matches the claim, rename or narrow the template before adding it.

## Mandatory verification for wildcard measure templates

For templates that fix a measure and leave the remaining dimensions open with wildcards, do not stop at confirming that the API call works.

You must inspect what the wildcard retrieval actually returns for each measure and record that in the description.

Required process:

1. Run the live wildcard retrieval for the measure.
2. Inspect the returned series keys or MCP availability output.
3. Identify which dimensions actually vary in published data and which are fixed.
4. Check whether important dimension combinations are constrained, even when the broad dimension appears available.
5. Update the measure description to describe observed availability only.

At minimum, check and describe:

- geography actually returned, for example `AUS` only vs state codes
- whether industry detail is `TOTAL` only, sections only, or includes detailed subdivision codes
- which adjustment types are actually present
- whether the data is quarterly, monthly, or another frequency
- whether the retrieval is original-only, seasonally adjusted/trend-only, or mixed
- whether broad availability hides narrower combination limits, for example state data existing only for totals or only for some sector variants

Do not describe dimensions as available just because they exist in metadata or codelists.
If metadata suggests broader coverage but the published series are narrower, say that explicitly in the dataset or measure description.
If a dimension is only available for some combinations, say that explicitly in the description instead of implying the dimension is broadly usable.

## Mandatory backend compatibility check for new curated schemas

When adding a new curated dataset or a new curated template shape, do not assume the backend can already execute it.

You must verify that the backend retrieval path supports the template schema you are introducing.

Examples:

- if the backend currently supports `measureId` substitution, and the new template uses `dataItemId`, add and verify `dataItemId` support before considering the curation complete
- if the new dataset uses a different placeholder pattern, dimension order, or retrieval metadata shape, confirm the backend materializes the API call correctly
- if the new schema needs new tool input fields, artifact fields, or parsing logic, implement those changes as part of the curation task

Minimum required check:

1. verify the curated `api_call` itself works directly against ABS
2. verify the backend can materialize and execute that curated template shape
3. only then treat the dataset as successfully curated

Do not stop after updating `CURATED_ABS_CATALOG.txt` and `CURATED_ABS_STRUCTURES.txt` if the backend cannot yet use the new schema.

Example of the required standard:

- bad: "jobs by geography and industry"
- good: "Wildcard retrieval returns quarterly Australia-only series. Original data includes TOTAL, sections A-S, and detailed subdivision codes. No state breakdowns are returned in the published series."

## Current first curated entry

Dataset:

- `LABOUR_ACCT_Q`

Template:

- `measure_wildcard`

Known working API call:

```text
/rest/data/LABOUR_ACCT_Q/{MEASURE}....?detail=full&dimensionAtObservation=TIME_PERIOD
```

Meaning:

- `{MEASURE}` = selected Labour Accounts measure
- wildcard positions return whatever published combinations actually exist for geography, industry, adjustment type and frequency
- `Q` = quarterly

Important:

- do not assume the wildcard geometry means state data exists
- for `LABOUR_ACCT_Q`, metadata may advertise geography capability beyond what the published series actually return
- always verify observed availability from the returned data before writing the curated description

## Rule for future additions

Keep the curated layer small.

Preferred process:

1. one dataset
2. one working template
3. test it
4. only then add the next template

This repo should optimize for reliability, not exhaustiveness.

## Condensed curation workflow

When curating a new ABS dataset, use this sequence:

1. Identify the candidate dataset with catalog, metadata, or approved raw ABS discovery.
2. Inspect the live dimension structure and decide whether the dataset is best anchored on:
   - `measure_id`, or
   - `data_item_id`
3. Choose the anchor that matches how a user naturally asks for the concept.
   - Use `measure_id` when the measure itself is the primary concept.
   - Use `data_item_id` when the user is really asking for an economic concept and `MEASURE` is mostly the representation form.
4. Record a simple dataset-level `data_shape`:
   - `time_series`
   - `panel`
   - `matrix`
5. Build a wildcard template that fixes the anchor and leaves the remaining key positions open.
6. Run the live wildcard retrieval for each anchored item.
7. Inspect what the published data literally returns.
8. Inspect whether important dimension combinations are narrower than the broad wildcard result.
9. Write the catalog and structure descriptions from observed availability, not from metadata alone.
10. Verify the backend can actually execute that template shape.
11. Only then treat the dataset as curated.

## How to choose the anchor

Use this rule:

- If the user would normally ask for the thing by name, prefer `data_item_id`.
- If the user would normally ask for the measure code or the measure is the natural concept, prefer `measure_id`.

Examples:

- `LABOUR_ACCT_Q`: measure-based
- `ANA_AGG`: data-item-based
- `ANA_SFD`: data-item-based
- `ANA_IND_GVA`: data-item-based
- `ANA_EXP`: data-item-based
- `ANA_INC`: data-item-based

## How to classify data shape

Use:

- `time_series`: mainly one concept over time
- `panel`: multiple categories over time, for example industries, states, or sectors over time
- `matrix`: cross-tab or matrix-style data where rows and columns are both analytical dimensions

This field should help the harness reason about:

- whether the result is naturally a single trend
- whether it is a grouped comparison over time
- whether it is a table or matrix rather than a chart-first dataset

## What descriptions must say

Descriptions should help the harness plan valid analysis before retrieval.

At minimum, descriptions should say what is literally available for the anchored item:

- geography actually returned
- frequency actually returned
- industry or other category level actually returned
- sector coverage actually returned
- adjustment types actually returned
- measure forms actually returned, for example current prices, chain volume, percentage changes, contributions
- important combination limits, when a dimension is available only for some variants

Descriptions should be written to support questions like:

- can this dataset be compared to another one?
- at what common level can they be aligned?
- is the item TOTAL only, divisions only, subdivisions, states, or something else?

Descriptions should stay compact.
Do not try to list every returned series key or every code combination.
Instead, state the broad observed availability first, then add one short caveat sentence when combinations are narrower than they first appear.

Preferred pattern:

- one sentence on the broad observed availability
- one sentence on the most important combination caveat
- one sentence on frequency or adjustment availability if needed

Example:

- "State-level data exists. Sector variants exist, but not all sector variants are available by state. Quarterly only."

## Planning rule for derived analysis

For ratios, rankings, decompositions, per-worker metrics, or other derived analysis:

1. work backwards from the target output
2. identify the exact numerator and denominator or components
3. inspect structure and item descriptions first
4. choose the lowest common compatible level across datasets
5. only then retrieve

Do not rely on sandbox to rescue a bad retrieval plan.
If compatible levels are still unclear after structure inspection, stop and ask the user one short clarification question.

## Recommended future autonomous curation pattern

The long-term goal can be:

- prefer the human-approved curated base first
- if the needed dataset is missing, ask the user for approval
- then let the agent perform raw ABS discovery and build a new wildcard-based curated entry
- then use that new entry to answer the question

Recommended safety structure:

- human-approved base:
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_CATALOG.txt`
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_STRUCTURES.txt`
- AI-created overlay:
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_CATALOG_AI.txt`
  - `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_STRUCTURES_AI.txt`

Recommended behavior:

1. do not let autonomous curation write directly into the human-approved base files
2. let autonomous curation write into the `_AI` overlay files
3. at runtime, merge base + overlay so the harness can use both
4. let a human later review and promote good overlay entries into the base files

This gives Seshat room to extend coverage without silently mutating the approved curated base.

## Rules for autonomous curation

If the harness is ever allowed to curate missing datasets itself:

- it must still ask for approval before raw ABS discovery
- it must still test the live API call
- it must still verify literal returned availability item by item
- it must still verify backend compatibility for the new template shape
- it must still write cautious descriptions based on observed data only
- it must prefer adding one good working template over inventing a broad schema

Do not let autonomous curation widen the API surface casually.
It should follow the same reliability-first rules as manual curation.

## Product identity notes

There is a repo-level narrative identity file at:

- `/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/SOUL.md`

Use it as the source of truth for the product's naming, mythology, tone, and personality.
