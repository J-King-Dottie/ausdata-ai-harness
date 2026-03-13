# ABS Curation Agent

Use this guide only when the curated ABS layer does not cover the user's request and raw ABS discovery has been approved.

## Goal

Extend the curated ABS layer carefully.

Do not guess a dataset and do not write to the human-approved curated files.
Use the `_AI` overlay files only:

- `CURATED_ABS_CATALOG_AI.txt`
- `CURATED_ABS_STRUCTURES_AI.txt`

## Approval flow

1. If the curated layer does not cover the request well enough, ask approval to inspect the broader ABS API structure.
2. After approval, inspect likely raw ABS datasets and metadata.
3. Come back with the likely dataset or table and ask the user to confirm that this is the one they want curated.
4. Only after that confirmation, curate it into the `_AI` overlay.
5. After writing the overlay entry, return to the user and say it is curated and ready, then ask whether to proceed with answering the original question.

Do not collapse these into one approval.

## Curation workflow

1. Identify the likely ABS dataset.
2. Inspect raw metadata and dimension order.
3. Decide whether the anchor should be `measure_id` or `data_item_id`.
Use whichever one matches the user-meaningful concept better.
4. Determine `data_shape`:
- `time_series`: mainly one concept over time
- `panel`: many groups over time
- `matrix`: table or matrix style data
5. Build the wildcard pattern:
- fix the chosen anchor
- leave the remaining key dimensions open
6. Retrieve the live wildcard result.
7. Inspect the returned data, not just the metadata.
8. Record what is literally available:
- geography actually returned
- frequency actually returned
- industry or category level actually returned
- sector coverage actually returned
- adjustment types actually returned
- measure forms actually returned
9. Write concise descriptions that say what the metric is first, then what the wildcard retrieval actually contains.

## Description rules

- Do not write descriptions from schema possibilities.
- Write descriptions from observed returned availability.
- If metadata advertises broader coverage than the live published series return, say that clearly.
- Prefer direct plain-English metric descriptions over generic labels.
- If an item is industry gross value added, say that directly.
- If an item is a ratio, index, percentage change, or contribution series, say that directly.

## Overlay writing rules

- Never edit the human-approved base curated files.
- Write new or revised entries only into the `_AI` overlay files.
- If the lesson applies to an existing curated dataset, write a revised overlay entry for that same `dataset_id` in `_AI` rather than editing the base file.
- Overlay revisions may update dataset descriptions, template instructions, template descriptions, and nested measure or data-item descriptions when a new lesson has been verified.
- Keep the overlay entries valid JSON.
- Reuse the same file shape as the base curated files.

## Runtime behavior

- Prefer the human-approved curated layer first.
- Use the `_AI` overlay only when the user has approved broader discovery and the dataset has been curated there.
- Once the overlay entry exists, treat it like a normal curated dataset for answering the original question.
