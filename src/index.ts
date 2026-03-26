#!/usr/bin/env node

import path from "node:path";
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { DataFlowService } from "./services/abs/DataFlowService.js";
import { DatasetResolver } from "./services/abs/DatasetResolver.js";
import {
  DataFormat,
  DataQueryOptions,
  DataStructureMetadata,
  CodeItem,
  DimensionMetadata,
  ConceptMetadata,
  CodeListMetadata,
} from "./types/abs.js";
import logger from "./utils/logger.js";

const dataflowCachePath = path.join(process.cwd(), "ABS_DATAFLOWS_FULL.json");
const dataFlowService = new DataFlowService(dataflowCachePath);
const datasetResolver = new DatasetResolver(dataFlowService);
const runtimeDir = process.env.NISABA_RUNTIME_DIR
  ? path.resolve(process.env.NISABA_RUNTIME_DIR)
  : path.join(process.cwd(), "runtime");
const conversationId = (process.env.NISABA_CONVERSATION_ID || "").trim();
const codeContainerId = (process.env.NISABA_CODE_CONTAINER_ID || "").trim();
const openAiApiKey = (process.env.OPENAI_API_KEY || "").trim();
const MAX_ANALYSIS_UPLOAD_BYTES = 50 * 1024 * 1024;

function summarizeArgs(args: Record<string, unknown> | undefined): Record<string, unknown> {
  const summary: Record<string, unknown> = {};
  if (!args) {
    return summary;
  }
  for (const key of [
    "searchQuery",
    "datasetId",
    "dataKey",
    "anchorType",
    "anchorCode",
    "startPeriod",
    "endPeriod",
    "detail",
    "limit",
  ]) {
    const value = args[key];
    if (value === undefined || value === null || value === "") {
      continue;
    }
    summary[key] = value;
  }
  return summary;
}

function summarizePayload(payload: unknown): Record<string, unknown> {
  if (!payload || typeof payload !== "object") {
    return { type: typeof payload };
  }
  const typed = payload as Record<string, unknown>;
  const summary: Record<string, unknown> = {
    keys: Object.keys(typed).slice(0, 12),
  };
  if (Array.isArray(typed.dataflows)) {
    summary.dataflows = typed.dataflows.length;
  }
  if (Array.isArray(typed.anchor_candidates)) {
    summary.anchor_candidates = typed.anchor_candidates.length;
  }
  if (Array.isArray(typed.series)) {
    summary.series = typed.series.length;
  }
  if (typed.dataset && typeof typed.dataset === "object") {
    const dataset = typed.dataset as Record<string, unknown>;
    if (typeof dataset.id === "string") {
      summary.datasetId = dataset.id;
    }
    if (typeof dataset.name === "string") {
      summary.datasetName = dataset.name;
    }
  }
  return summary;
}

function isCustomDomesticDataset(datasetId: string): boolean {
  return String(datasetId || "").trim().startsWith("CUSTOM_AUS,");
}

function normalizeAnchorType(dimensionId: string, conceptId: string): string {
  const text = [dimensionId, conceptId]
    .map((part) => String(part || "").trim().toUpperCase())
    .filter(Boolean)
    .join(" ");
  if (text.includes("MEASURE")) {
    return "MEASURE";
  }
  if (text.includes("DATA_ITEM") || text.endsWith("ITEM") || text.includes(" ITEM")) {
    return "DATA_ITEM";
  }
  if (
    ["CAT", "CATEGORY", "SUPG", "SUPC", "PRODUCT", "COMMODITY", "INDUSTRY", "SECTOR", "FLOW"].some(
      (token) => text.includes(token)
    )
  ) {
    return "CATEGORY";
  }
  return "";
}

function anchorPriority(anchorType: string): number {
  const priorityMap: Record<string, number> = {
    DATA_ITEM: 100,
    MEASURE: 90,
    CATEGORY: 80,
  };
  return priorityMap[String(anchorType || "").trim().toUpperCase()] ?? 0;
}

function rawMetadataPayload(datasetId: string, metadata: DataStructureMetadata): Record<string, unknown> {
  const dimensions = Array.isArray(metadata.dimensions) ? metadata.dimensions : [];
  const concepts = Array.isArray(metadata.concepts) ? metadata.concepts : [];
  const codelists = Array.isArray(metadata.codelists) ? metadata.codelists : [];

  const codelistById = new Map<string, CodeListMetadata>();
  for (const item of codelists) {
    const cleanId = String(item?.id || "").trim();
    if (cleanId) {
      codelistById.set(cleanId, item);
    }
  }

  const conceptById = new Map<string, ConceptMetadata>();
  for (const item of concepts) {
    const cleanId = String(item?.id || "").trim();
    if (cleanId) {
      conceptById.set(cleanId, item);
    }
  }

  const orderedDimensions = [...dimensions].sort(
    (left: DimensionMetadata, right: DimensionMetadata) =>
      Number(left?.position || 0) - Number(right?.position || 0)
  );
  const dimensionOrder: string[] = [];
  const anchorRows: Array<Record<string, unknown>> = [];

  for (const dimension of orderedDimensions) {
    const dimensionId = String(dimension?.id || "").trim();
    if (!dimensionId) {
      continue;
    }
    const conceptId = String(dimension?.conceptId || "").trim();
    const concept = conceptById.get(conceptId);
    const codelistId = String(dimension?.codelist?.id || "").trim();
    const codelist = codelistById.get(codelistId);
    const anchorType = normalizeAnchorType(dimensionId, conceptId);
    dimensionOrder.push(dimensionId);
    if (!anchorType) {
      continue;
    }
    const conceptName =
      String(concept?.name || concept?.description || conceptId || dimensionId).trim() || dimensionId;
    const anchorCodes = (Array.isArray(codelist?.codes) ? codelist?.codes : [])
      .map((code: CodeItem) => ({
        code: String(code?.id || "").trim(),
        label: String(code?.name || "").trim(),
        description: String(code?.description || "").trim(),
      }))
      .filter((item) => item.code);
    anchorRows.push({
      anchor_type: anchorType,
      dimension_id: dimensionId,
      anchor_description: conceptName,
      position: Number(dimension?.position || 0),
      anchor_codes: anchorCodes,
    });
  }

  const wildcardTemplateFor = (anchorDimensionId: string): string => {
    const parts = dimensionOrder.map((dimensionId) =>
      dimensionId === anchorDimensionId ? `{${dimensionId}}` : ""
    );
    return parts.length > 0 ? parts.join(".") : "all";
  };

  const anchorCandidatesByType = new Map<string, Record<string, unknown>>();
  for (const row of anchorRows) {
    const anchorType = String(row.anchor_type || "").trim();
    const dimensionId = String(row.dimension_id || "").trim();
    if (!anchorType || !dimensionId) {
      continue;
    }
    const candidate = {
      anchor_type: anchorType,
      anchor_description: String(row.anchor_description || anchorType).trim(),
      dimension_id: dimensionId,
      wildcard_data_key_template: wildcardTemplateFor(dimensionId),
      anchor_codes: Array.isArray(row.anchor_codes) ? row.anchor_codes : [],
    };
    const existing = anchorCandidatesByType.get(anchorType);
    if (!existing) {
      anchorCandidatesByType.set(anchorType, candidate);
      continue;
    }
    const existingDimension = String(existing.dimension_id || "").trim();
    const existingRank = dimensionOrder.indexOf(existingDimension);
    const currentRank = dimensionOrder.indexOf(dimensionId);
    if (existingRank === -1 || (currentRank !== -1 && currentRank < existingRank)) {
      anchorCandidatesByType.set(anchorType, candidate);
    }
  }

  const anchorCandidates = Array.from(anchorCandidatesByType.values()).sort(
    (left, right) =>
      anchorPriority(String(right.anchor_type || "")) - anchorPriority(String(left.anchor_type || ""))
  );

  return {
    kind: "raw_metadata",
    dataset_id: datasetId,
    anchor_candidates: anchorCandidates,
    metadata,
  };
}

function validateAnchorWildcardDataKey(datasetId: string, dataKey: string): void {
  const cleanDataKey = String(dataKey || "").trim();
  if (!cleanDataKey || cleanDataKey.toLowerCase() === "all") {
    throw new Error(
      `Invalid raw ABS dataKey. ABS retrieval must follow metadata-derived anchor selection; broad 'all' retrieval is not allowed. Received datasetId=${datasetId}, dataKey=${dataKey}.`
    );
  }
  const segments = cleanDataKey.split(".");
  const fixedSegments = segments
    .map((segment, index) => [index, String(segment || "").trim()] as const)
    .filter(([, token]) => token);

  if (fixedSegments.length !== 1) {
    throw new Error(
      `Invalid raw ABS dataKey. raw_retrieve must use exactly one anchored segment and wildcard every other segment. Received datasetId=${datasetId}, dataKey=${dataKey}.`
    );
  }

  const [, anchorToken] = fixedSegments[0];
  if (anchorToken.includes("+")) {
    throw new Error(
      `Invalid raw ABS dataKey. raw_retrieve must use exactly one anchor code, not multiple codes in one segment. Received datasetId=${datasetId}, dataKey=${dataKey}.`
    );
  }
}

function buildWildcardDataKey(
  metadataPayload: Record<string, unknown>,
  anchorType: string,
  anchorCode: string
): string {
  const candidates = Array.isArray(metadataPayload.anchor_candidates)
    ? metadataPayload.anchor_candidates
    : [];
  const target = candidates.find((item) => {
    if (!item || typeof item !== "object") {
      return false;
    }
    return String((item as Record<string, unknown>).anchor_type || "").trim().toUpperCase() === anchorType;
  }) as Record<string, unknown> | undefined;
  if (!target) {
    throw new Error(`No anchor candidate found for anchorType=${anchorType}. Inspect metadata again.`);
  }
  const allowedCodes = Array.isArray(target.anchor_codes) ? target.anchor_codes : [];
  const cleanAnchorCode = String(anchorCode || "").trim();
  const isAllowed = allowedCodes.some((item) => {
    if (!item || typeof item !== "object") {
      return false;
    }
    return String((item as Record<string, unknown>).code || "").trim() === cleanAnchorCode;
  });
  if (!isAllowed) {
    throw new Error(
      `Invalid ABS anchor code '${cleanAnchorCode}' for anchorType=${anchorType}. Choose a code from the metadata anchor_candidates list.`
    );
  }
  const template = String(target.wildcard_data_key_template || "").trim();
  if (!template) {
    throw new Error(`Anchor candidate for anchorType=${anchorType} does not include a wildcard template.`);
  }
  return template.replace(/\{[^}]+\}/g, cleanAnchorCode);
}

function artifactPathForId(artifactId: string) {
  return path.join(runtimeDir, "conversations", conversationId, "artifacts", `${artifactId}.json`);
}

async function storeDomesticArtifact(
  payload: Record<string, unknown>,
  label: string
): Promise<Record<string, unknown>> {
  if (!conversationId) {
    throw new Error("NISABA_CONVERSATION_ID is not set for domestic MCP retrieval.");
  }
  const artifactId = `raw-domestic-${randomUUID()}`;
  const artifactPath = artifactPathForId(artifactId);
  await mkdir(path.dirname(artifactPath), { recursive: true });
  await writeFile(artifactPath, JSON.stringify(payload, null, 2), "utf-8");
  const summary = summarizePayload(payload);
  return {
    artifact_id: artifactId,
    kind: "domestic_retrieve",
    label,
    summary: `Stored domestic retrieval artifact for ${label}. Inspect it before analysis.`,
    source_references: Array.isArray(payload.source_references) ? payload.source_references : [],
    manifest: summary,
  };
}

async function loadArtifactPayload(artifactId: string): Promise<Record<string, unknown>> {
  const artifactPath = artifactPathForId(artifactId);
  const raw = await readFile(artifactPath, "utf-8");
  const payload = JSON.parse(raw);
  if (!payload || typeof payload !== "object") {
    throw new Error(`Artifact ${artifactId} is not a JSON object artifact.`);
  }
  return payload as Record<string, unknown>;
}

async function latestDomesticArtifactId(): Promise<string | undefined> {
  if (!conversationId) {
    return undefined;
  }
  const dir = path.join(runtimeDir, "conversations", conversationId, "artifacts");
  let entries: string[] = [];
  try {
    entries = await readdir(dir);
  } catch {
    return undefined;
  }
  const candidates = entries.filter((name) =>
    name.endsWith(".json") &&
    (name.startsWith("raw-domestic-") || name.startsWith("narrowed-domestic-") || name.startsWith("artifact-"))
  );
  let latestName: string | undefined;
  let latestMtime = -1;
  for (const name of candidates) {
    try {
      const fileStat = await stat(path.join(dir, name));
      const mtime = fileStat.mtimeMs || 0;
      if (mtime > latestMtime) {
        latestMtime = mtime;
        latestName = name;
      }
    } catch {
      continue;
    }
  }
  return latestName ? latestName.replace(/\.json$/i, "") : undefined;
}

function flattenDomesticPayload(payload: Record<string, unknown>): { headers: string[]; rows: Array<Array<unknown>> } {
  const seriesItems = Array.isArray(payload.series) ? payload.series : [];
  const dimensionKeys: string[] = [];
  const attributeKeys: string[] = [];

  for (const series of seriesItems) {
    if (!series || typeof series !== "object") {
      continue;
    }
    const typedSeries = series as Record<string, unknown>;
    const seriesDims = typedSeries.dimensions && typeof typedSeries.dimensions === "object"
      ? (typedSeries.dimensions as Record<string, unknown>)
      : {};
    for (const key of Object.keys(seriesDims)) {
      if (!dimensionKeys.includes(key)) {
        dimensionKeys.push(key);
      }
    }
    const observations = Array.isArray(typedSeries.observations) ? typedSeries.observations : [];
    for (const observation of observations) {
      if (!observation || typeof observation !== "object") {
        continue;
      }
      const typedObservation = observation as Record<string, unknown>;
      const obsDims = typedObservation.dimensions && typeof typedObservation.dimensions === "object"
        ? (typedObservation.dimensions as Record<string, unknown>)
        : {};
      const obsAttrs = typedObservation.attributes && typeof typedObservation.attributes === "object"
        ? (typedObservation.attributes as Record<string, unknown>)
        : {};
      for (const key of Object.keys(obsDims)) {
        if (!dimensionKeys.includes(key)) {
          dimensionKeys.push(key);
        }
      }
      for (const key of Object.keys(obsAttrs)) {
        if (!attributeKeys.includes(key)) {
          attributeKeys.push(key);
        }
      }
    }
    const seriesAttrs = typedSeries.attributes && typeof typedSeries.attributes === "object"
      ? (typedSeries.attributes as Record<string, unknown>)
      : {};
    for (const key of Object.keys(seriesAttrs)) {
      if (!attributeKeys.includes(key)) {
        attributeKeys.push(key);
      }
    }
  }

  const headers = ["seriesKey", ...dimensionKeys, "observationKey", "value", ...attributeKeys];
  const rows: Array<Array<unknown>> = [];

  const labelOrValue = (value: unknown): unknown => {
    if (value && typeof value === "object") {
      const typed = value as Record<string, unknown>;
      if (typed.label !== undefined && typed.label !== null) {
        return typed.label;
      }
      if (typed.code !== undefined && typed.code !== null) {
        return typed.code;
      }
    }
    return value;
  };

  for (const series of seriesItems) {
    if (!series || typeof series !== "object") {
      continue;
    }
    const typedSeries = series as Record<string, unknown>;
    const seriesDims = typedSeries.dimensions && typeof typedSeries.dimensions === "object"
      ? (typedSeries.dimensions as Record<string, unknown>)
      : {};
    const seriesAttrs = typedSeries.attributes && typeof typedSeries.attributes === "object"
      ? (typedSeries.attributes as Record<string, unknown>)
      : {};
    const observations = Array.isArray(typedSeries.observations) ? typedSeries.observations : [];
    for (const observation of observations) {
      if (!observation || typeof observation !== "object") {
        continue;
      }
      const typedObservation = observation as Record<string, unknown>;
      const obsDims = typedObservation.dimensions && typeof typedObservation.dimensions === "object"
        ? (typedObservation.dimensions as Record<string, unknown>)
        : {};
      const obsAttrs = typedObservation.attributes && typeof typedObservation.attributes === "object"
        ? (typedObservation.attributes as Record<string, unknown>)
        : {};
      const row: Array<unknown> = [typedSeries.seriesKey];
      for (const key of dimensionKeys) {
        const value = obsDims[key] ?? seriesDims[key];
        row.push(labelOrValue(value));
      }
      row.push(typedObservation.observationKey);
      row.push(typedObservation.value);
      for (const key of attributeKeys) {
        const value = obsAttrs[key] ?? seriesAttrs[key];
        row.push(labelOrValue(value));
      }
      rows.push(row);
    }
  }

  return { headers, rows };
}

function domesticPreviewRows(payload: Record<string, unknown>, limit = 8): Array<Record<string, unknown>> {
  const { headers, rows } = flattenDomesticPayload(payload);
  return rows.slice(0, limit).map((row) => {
    const item: Record<string, unknown> = {};
    headers.forEach((header, index) => {
      item[header] = row[index];
    });
    return item;
  });
}

function domesticArtifactManifest(
  artifactId: string,
  kind: string,
  label: string,
  summaryText: string,
  payload: Record<string, unknown>,
  extra: Record<string, unknown> = {}
): Record<string, unknown> {
  const dataset = payload.dataset && typeof payload.dataset === "object"
    ? (payload.dataset as Record<string, unknown>)
    : {};
  const seriesItems = Array.isArray(payload.series) ? payload.series : [];
  let observationCount = 0;
  const dimensions: Record<string, string[]> = {};
  for (const series of seriesItems) {
    if (!series || typeof series !== "object") {
      continue;
    }
    const typedSeries = series as Record<string, unknown>;
    const observations = Array.isArray(typedSeries.observations) ? typedSeries.observations : [];
    observationCount += observations.length;
    const seriesDims = typedSeries.dimensions && typeof typedSeries.dimensions === "object"
      ? (typedSeries.dimensions as Record<string, unknown>)
      : {};
    for (const [key, value] of Object.entries(seriesDims)) {
      const labelValue = value && typeof value === "object"
        ? String(((value as Record<string, unknown>).label ?? (value as Record<string, unknown>).code ?? "") || "").trim()
        : String(value || "").trim();
      if (!labelValue) {
        continue;
      }
      dimensions[key] = dimensions[key] || [];
      if (!dimensions[key].includes(labelValue) && dimensions[key].length < 6) {
        dimensions[key].push(labelValue);
      }
    }
  }
  return {
    artifact_id: artifactId,
    kind,
    label,
    summary: summaryText,
    dataset_id: String(dataset.id || "").trim(),
    series_count: seriesItems.length,
    observation_count: observationCount,
    dimensions,
    preview_rows: domesticPreviewRows(payload),
    source_references: Array.isArray(payload.source_references) ? payload.source_references : [],
    ...extra,
  };
}

function isMatrixStyleDomesticPayload(payload: Record<string, unknown>): boolean {
  const dataset =
    payload.dataset && typeof payload.dataset === "object"
      ? (payload.dataset as Record<string, unknown>)
      : {};
  const datasetId = String(dataset.id || "").trim().toUpperCase();
  const label = String(dataset.name || payload.label || "").trim().toUpperCase();
  return (
    datasetId.startsWith("ABS_SU_TABLE_") ||
    label.includes("SUPPLY USE") ||
    label.includes("SUPPLY-USE") ||
    label.includes("INPUT OUTPUT") ||
    label.includes("INPUT-OUTPUT") ||
    label.includes("MATRIX")
  );
}

function normalizeDimensionFilters(value: unknown): Record<string, string[]> {
  if (Array.isArray(value)) {
    const normalized: Record<string, string[]> = {};
    for (const item of value) {
      if (!item || typeof item !== "object") {
        continue;
      }
      const typed = item as Record<string, unknown>;
      const cleanKey = String(typed.dimension || typed.key || "").trim();
      if (!cleanKey) {
        continue;
      }
      const rawValues = typed.values;
      const values = Array.isArray(rawValues)
        ? rawValues.map((entry) => String(entry || "").trim()).filter(Boolean)
        : [];
      if (values.length) {
        normalized[cleanKey] = values;
      }
    }
    return normalized;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const normalized: Record<string, string[]> = {};
  for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
    const cleanKey = String(key || "").trim();
    if (!cleanKey) {
      continue;
    }
    if (Array.isArray(raw)) {
      const items = raw.map((item) => String(item || "").trim()).filter(Boolean);
      if (items.length) {
        normalized[cleanKey] = items;
      }
      continue;
    }
    const single = String(raw || "").trim();
    if (single) {
      normalized[cleanKey] = [single];
    }
  }
  return normalized;
}

function matchesTimeRange(value: string, start: string, end: string): boolean {
  const clean = String(value || "").trim();
  if (!clean) {
    return false;
  }
  if (start && clean < start) {
    return false;
  }
  if (end && clean > end) {
    return false;
  }
  return true;
}

function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function writeCsvText(headers: string[], rows: Array<Array<unknown>>): string {
  const lines: string[] = [];
  if (headers.length) {
    lines.push(headers.map(csvEscape).join(","));
  }
  for (const row of rows) {
    lines.push(row.map(csvEscape).join(","));
  }
  return lines.join("\n");
}

function estimateAnalysisCsvBytes(headers: string[], rows: Array<Array<unknown>>): number {
  if (!headers.length) {
    return 0;
  }
  return Buffer.byteLength(writeCsvText(headers, rows), "utf8");
}

async function uploadAnalysisFile(
  artifactId: string,
  label: string,
  headers: string[],
  rows: Array<Array<unknown>>
): Promise<Record<string, unknown>> {
  if (!codeContainerId || !openAiApiKey || !headers.length) {
    return {};
  }
  const csvText = writeCsvText(headers, rows);
  const csvBytes = Buffer.byteLength(csvText, "utf8");
  if (csvBytes > MAX_ANALYSIS_UPLOAD_BYTES) {
    throw new Error(
      `Analysis file exceeds upload limit: ${(csvBytes / (1024 * 1024)).toFixed(2)}MB > 50MB.`
    );
  }
  const uploadName = `${artifactId}_${String(label || "analysis").replace(/[^A-Za-z0-9._-]+/g, "_").slice(0, 48) || "analysis"}.csv`;
  const form = new FormData();
  form.set("file", new globalThis.Blob([csvText], { type: "text/csv" }), uploadName);
  const response = await fetch(`https://api.openai.com/v1/containers/${codeContainerId}/files`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${openAiApiKey}`,
    },
    body: form,
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Failed to upload analysis file to container: ${response.status} ${body.slice(0, 400)}`);
  }
  const payload = (await response.json()) as Record<string, unknown>;
  return {
    analysis_container_id: codeContainerId,
    analysis_file_id: String(payload.id || "").trim(),
    analysis_filename: String(payload.filename || uploadName).trim(),
    analysis_file: {
      filename: String(payload.filename || uploadName).trim(),
      container_id: codeContainerId,
      artifact_id: artifactId,
    },
  };
}

async function artifactFileSizeBytes(artifactId: string): Promise<number> {
  const stats = await stat(artifactPathForId(artifactId));
  return Number(stats.size || 0);
}

const server = new Server(
  {
    name: "nisaba-domestic-mcp",
    version: "0.1.0",
    description: "Access Australian domestic public data, including ABS and curated custom Australian sources"
  },
  {
    capabilities: {
      tools: {
        list: true,
        call: true
      }
    }
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "search_catalog",
        description: "Search the merged Australian domestic catalog across ABS and curated custom Australian sources",
        inputSchema: {
          type: "object",
          properties: {
            forceRefresh: {
              type: "boolean",
              description: "If true, refreshes cached source metadata before searching"
            },
            searchQuery: {
              type: "string",
              description: "Optional local full-text search over the merged domestic catalog"
            },
            limit: {
              type: "number",
              description: "Optional shortlist size when using searchQuery (default 8)"
            }
          }
        }
      },
      {
        name: "list_dataflows",
        description: "List available domestic dataflows with identifiers and descriptions",
        inputSchema: {
          type: "object",
          properties: {
            forceRefresh: {
              type: "boolean",
              description: "If true, bypass cache and refresh latest domestic source metadata"
            },
            searchQuery: {
              type: "string",
              description: "Optional local full-text search over cached domestic dataflows to return only a ranked shortlist"
            },
            limit: {
              type: "number",
              description: "Optional shortlist size when using searchQuery (default 8)"
            }
          }
        }
      },
      {
        name: "get_metadata",
        description: "Fetch structural metadata for a domestic dataset, using source-specific adapters where needed. For ABS datasets, do this before retrieve so you can choose one anchor and the server can construct the wildcard retrieval correctly.",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "Domestic dataset identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0 or CUSTOM_AUS,RBA_A1,1.0)"
            },
            forceRefresh: {
              type: "boolean",
              description: "If true, refreshes cached structural metadata before returning"
            }
          }
        }
      },
      {
        name: "resolve_dataset",
        description:
          "Fetch domestic dataset observations and return normalized data using source-specific metadata and adapters",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "Domestic dataset identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0 or CUSTOM_AUS,RBA_A1,1.0)"
            },
            dataKey: {
              type: "string",
              description:
                "Optional SDMX data key for slicing the dataset (e.g., dots-separated dimension codes)"
            },
            startPeriod: {
              type: "string",
              description: "Optional start period in SDMX format (e.g., 2020, 2020-Q1, 2020-01)"
            },
            endPeriod: {
              type: "string",
              description: "Optional end period in SDMX format (e.g., 2023, 2023-Q4, 2023-12)"
            },
            format: {
              type: "string",
              enum: [
                "csvfilewithlabels",
                "csvfile",
                "jsondata",
                "genericdata",
                "structurespecificdata"
              ],
              description:
                "Optional ABS format (defaults to jsondata for lightweight queries; resolver uses metadata)"
            },
            dimensionAtObservation: {
              type: "string",
              description:
                "Optional ABS dimensionAtObservation parameter (default TIME_PERIOD for time-series)"
            },
            detail: {
              type: "string",
              enum: ["full", "dataonly", "serieskeysonly", "nodata"],
              description: "Optional ABS detail level (defaults to full)"
            },
            forceRefresh: {
              type: "boolean",
              description: "If true, refreshes cached dataflows metadata before resolving"
            }
          }
        }
      },
      {
        name: "retrieve",
        description: "Retrieve a domestic dataset, using the correct source adapter behind one domestic tool contract. For ABS datasets, always do this only after get_metadata and use one metadata-derived anchor so the server constructs the wildcard retrieval. ABS retrieve does not accept ad hoc dataKey input.",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "Domestic dataset identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0 or CUSTOM_AUS,AES_TABLE_O,1.0)"
            },
            dataKey: {
              type: "string",
              description:
                "Optional source-specific retrieval key. For ABS this is the SDMX data key; for custom sources it may be 'all' or a curated slice."
            },
            anchorType: {
              type: "string",
              description:
                "ABS only. One metadata-derived anchor type such as DATA_ITEM, MEASURE, or CATEGORY. Use with anchorCode so the server constructs the wildcard dataKey."
            },
            anchorCode: {
              type: "string",
              description:
                "ABS only. One metadata-derived anchor code chosen from get_metadata.anchor_candidates. The server will construct the wildcard dataKey."
            },
            startPeriod: {
              type: "string",
              description: "Optional start period (e.g., 2020, 2020-Q1, 2020-01)"
            },
            endPeriod: {
              type: "string",
              description: "Optional end period (e.g., 2023, 2023-Q4, 2023-12)"
            },
            detail: {
              type: "string",
              enum: ["full", "dataonly", "serieskeysonly", "nodata"],
              description: "Optional detail level (defaults to full)"
            },
            dimensionAtObservation: {
              type: "string",
              description: "Optional dimensionAtObservation parameter for ABS-backed series"
            },
            forceRefresh: {
              type: "boolean",
              description: "If true, refreshes cached metadata before resolving"
            }
          }
        }
      },
      {
        name: "inspect_artifact",
        description: "Inspect a stored domestic retrieval artifact and return a compact structural summary plus preview rows, including size information that helps decide whether to use it directly or narrow it first.",
        inputSchema: {
          type: "object",
          properties: {
            artifactId: {
              type: "string",
              description: "Optional domestic artifact id. If omitted, uses the latest domestic artifact for this conversation."
            }
          }
        }
      },
      {
        name: "narrow_artifact",
        description: "Create a narrowed domestic artifact by filtering the stored artifact down to the minimum slice needed. For supply-use, input-output, or other matrix-style tables, use this to isolate one correct full matrix or one metric/anchor, not to trim inside the matrix.",
        inputSchema: {
          type: "object",
          properties: {
            artifactId: {
              type: "string",
              description: "Optional domestic artifact id. If omitted, uses the latest domestic artifact for this conversation."
            },
            dimensionFilters: {
              type: "array",
              description: "Optional list of domestic dimension filters.",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["dimension", "values"],
                properties: {
                  dimension: {
                    type: "string",
                    description: "Domestic dimension name, such as SEX or AGE."
                  },
                  values: {
                    type: "array",
                    description: "Allowed labels or codes for that dimension.",
                    items: {
                      type: "string"
                    }
                  }
                }
              }
            },
            start: {
              type: "string",
              description: "Optional lower bound for time values."
            },
            end: {
              type: "string",
              description: "Optional upper bound for time values."
            },
            seriesKeyContains: {
              type: "string",
              description: "Optional substring that must appear in the series key."
            },
            maxSeries: {
              type: "number",
              description: "Optional cap on the number of series retained (default 12)."
            }
          }
        }
      },
      {
        name: "get_dataflow_metadata",
        description: "Fetch structural metadata (dimensions, codes, concepts) for a specific domestic dataflow",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "Domestic dataset identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0 or CUSTOM_AUS,RBA_A1,1.0)"
            },
            forceRefresh: {
              type: "boolean",
              description: "If true, refreshes cached structural metadata before returning"
            }
          }
        }
      },
      {
        name: "query_dataset",
        description: "Direct domestic dataset request with optional data key and time filters. Do not use this for ABS datasets; ABS retrieval must go through get_metadata and anchored retrieve.",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "Domestic dataset identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0 or CUSTOM_AUS,RBA_A1,1.0)"
            },
            dataKey: {
              type: "string",
              description: "Optional SDMX data key (dot separated) to slice the dataset"
            },
            startPeriod: {
              type: "string",
              description: "Optional start period (e.g., 2020, 2020-Q1, 2020-01)"
            },
            endPeriod: {
              type: "string",
              description: "Optional end period (e.g., 2023, 2023-Q4, 2023-12)"
            },
            detail: {
              type: "string",
              enum: ["full", "dataonly", "serieskeysonly", "nodata"],
              description: "Optional SDMX detail level (defaults to full)"
            },
            dimensionAtObservation: {
              type: "string",
              description: "Optional SDMX dimensionAtObservation parameter"
            }
          }
        }
      },
    ]
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  try {
    const { name, arguments: args } = request.params;
    const startedAt = Date.now();
    logger.info("Domestic MCP tool start", {
      conversationId,
      tool: name,
      args: summarizeArgs((args as Record<string, unknown> | undefined) ?? undefined),
    });

    switch (name) {
      case "search_catalog":
      case "list_dataflows": {
        const forceRefresh =
          typeof args?.forceRefresh === "boolean" ? args.forceRefresh : false;
        const searchQuery =
          typeof args?.searchQuery === "string" ? args.searchQuery : "";
        const limit =
          typeof args?.limit === "number" && Number.isFinite(args.limit)
            ? Math.max(1, Math.floor(args.limit))
            : 8;
        const flows = searchQuery
          ? await dataFlowService.searchDataFlows(searchQuery, limit, forceRefresh)
          : await dataFlowService.getDataFlows(forceRefresh);
        const payload = {
          total: flows.length,
          searchQuery,
          dataflows: flows,
        };
        logger.info("Domestic MCP tool success", {
          conversationId,
          tool: name,
          duration_ms: Date.now() - startedAt,
          summary: summarizePayload(payload),
        });
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(payload, null, 2),
            },
          ],
        };
      }
      case "get_metadata":
      case "get_dataflow_metadata": {
        if (!args?.datasetId || typeof args.datasetId !== "string") {
          throw new Error("datasetId is required and must be a string");
        }
        const metadata = await dataFlowService.getDataStructureForDataflow(
          args.datasetId,
          typeof args?.forceRefresh === "boolean" ? args.forceRefresh : false
        );
        const payload = isCustomDomesticDataset(args.datasetId)
          ? metadata
          : rawMetadataPayload(args.datasetId, metadata);
        logger.info("Domestic MCP tool success", {
          conversationId,
          tool: name,
          duration_ms: Date.now() - startedAt,
          summary: summarizePayload(payload),
        });
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(payload, null, 2),
            },
          ],
        };
      }
      case "query_dataset": {
        if (!args?.datasetId || typeof args.datasetId !== "string") {
          throw new Error("datasetId is required and must be a string");
        }
        if (!isCustomDomesticDataset(args.datasetId)) {
          throw new Error(
            "ABS query_dataset is not allowed. For ABS datasets, use get_metadata first and then retrieve with anchorType + anchorCode."
          );
        }
        const data = await dataFlowService.getFlowData(
          args.datasetId,
          typeof args?.dataKey === "string" ? args.dataKey : "all",
          {
            startPeriod:
              typeof args?.startPeriod === "string" ? args.startPeriod : undefined,
            endPeriod:
              typeof args?.endPeriod === "string" ? args.endPeriod : undefined,
            detail:
              typeof args?.detail === "string"
                ? (args.detail as DataQueryOptions["detail"])
                : undefined,
            dimensionAtObservation:
              typeof args?.dimensionAtObservation === "string"
                ? args.dimensionAtObservation
                : undefined,
          }
        );
        logger.info("Domestic MCP tool success", {
          conversationId,
          tool: name,
          duration_ms: Date.now() - startedAt,
          summary: summarizePayload(data),
        });
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(data, null, 2),
            },
          ],
        };
      }
      case "retrieve":
      case "resolve_dataset": {
        if (!args?.datasetId || typeof args.datasetId !== "string") {
          throw new Error("datasetId is required and must be a string");
        }
        let dataKey =
          typeof args.dataKey === "string" && args.dataKey.trim()
            ? args.dataKey.trim()
            : undefined;
        if (!isCustomDomesticDataset(args.datasetId)) {
          if (dataKey) {
            throw new Error(
              "ABS retrieve does not accept dataKey input. Use get_metadata first, choose one anchor from anchor_candidates, and call retrieve with anchorType + anchorCode."
            );
          }
          const anchorType = String(args.anchorType || "").trim().toUpperCase();
          const anchorCode = String(args.anchorCode || "").trim();
          if (!anchorType || !anchorCode) {
            throw new Error(
              "ABS retrieve requires anchorType + anchorCode from get_metadata. ABS always uses metadata-derived anchor + wildcard retrieval."
            );
          }
          const metadata = await dataFlowService.getDataStructureForDataflow(
            args.datasetId,
            typeof args?.forceRefresh === "boolean" ? args.forceRefresh : false
          );
          const metadataPayload = rawMetadataPayload(args.datasetId, metadata);
          dataKey = buildWildcardDataKey(metadataPayload, anchorType, anchorCode);
          validateAnchorWildcardDataKey(args.datasetId, dataKey);
        }
        const result = await datasetResolver.resolve({
          datasetId: args.datasetId,
          dataKey,
          startPeriod: typeof args.startPeriod === "string" ? args.startPeriod : undefined,
          endPeriod: typeof args.endPeriod === "string" ? args.endPeriod : undefined,
          detail:
            typeof args.detail === "string"
              ? (args.detail as DataQueryOptions["detail"])
              : undefined,
          dimensionAtObservation:
            typeof args.dimensionAtObservation === "string"
              ? args.dimensionAtObservation
              : undefined,
          format:
            typeof args.format === "string"
              ? (args.format as DataFormat)
              : undefined,
          forceRefresh: typeof args.forceRefresh === "boolean" ? args.forceRefresh : undefined,
        });
        const dataset =
          result.dataset && typeof result.dataset === "object"
            ? (result.dataset as Record<string, unknown>)
            : {};
        const label =
          (typeof dataset.name === "string" && dataset.name.trim()) ||
          (typeof dataset.id === "string" && dataset.id.trim()) ||
          args.datasetId;
        const manifest = await storeDomesticArtifact(
          result as unknown as Record<string, unknown>,
          String(label)
        );
        logger.info("Domestic MCP tool success", {
          conversationId,
          tool: name,
          duration_ms: Date.now() - startedAt,
          summary: summarizePayload(manifest),
        });

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(manifest, null, 2),
            },
          ],
        };
      }
      case "inspect_artifact": {
        const requestedArtifactId =
          typeof args?.artifactId === "string" && args.artifactId.trim()
            ? args.artifactId.trim()
            : await latestDomesticArtifactId();
        if (!requestedArtifactId) {
          throw new Error("No domestic artifact is available to inspect yet.");
        }
        const payload = await loadArtifactPayload(requestedArtifactId);
        const label = String(
          (
            payload.dataset && typeof payload.dataset === "object"
              ? (payload.dataset as Record<string, unknown>).name
              : ""
          ) || requestedArtifactId
        ).trim() || requestedArtifactId;
        const kind = String((payload.kind as string) || (requestedArtifactId.startsWith("narrowed-domestic-") ? "domestic_narrowed" : "domestic_retrieve")).trim();
        const extra: Record<string, unknown> = {};
        const rows = flattenDomesticPayload(payload);
        const estimatedBytes = estimateAnalysisCsvBytes(rows.headers, rows.rows);
        extra.artifact_size_bytes = await artifactFileSizeBytes(requestedArtifactId);
        extra.analysis_estimated_bytes = estimatedBytes;
        extra.analysis_estimated_mb = Number((estimatedBytes / (1024 * 1024)).toFixed(2));
        if (isMatrixStyleDomesticPayload(payload) && estimatedBytes <= MAX_ANALYSIS_UPLOAD_BYTES) {
          Object.assign(extra, await uploadAnalysisFile(requestedArtifactId, label, rows.headers, rows.rows));
        }
        else if (isMatrixStyleDomesticPayload(payload) && estimatedBytes > MAX_ANALYSIS_UPLOAD_BYTES) {
          extra.analysis_too_large_for_direct_python = true;
          extra.analysis_limit_bytes = MAX_ANALYSIS_UPLOAD_BYTES;
          extra.analysis_guidance =
            "This matrix-style artifact is too large to send directly to python. Narrow to one correct full matrix or one metric/anchor first, then analyze that full matrix.";
        }
        const manifest = domesticArtifactManifest(
          requestedArtifactId,
          kind,
          label,
          `Inspected domestic artifact '${label}'.`,
          payload,
          extra
        );
        logger.info("Domestic MCP tool success", {
          conversationId,
          tool: name,
          duration_ms: Date.now() - startedAt,
          summary: summarizePayload(manifest),
        });
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(manifest, null, 2),
            },
          ],
        };
      }
      case "narrow_artifact": {
        const requestedArtifactId =
          typeof args?.artifactId === "string" && args.artifactId.trim()
            ? args.artifactId.trim()
            : await latestDomesticArtifactId();
        if (!requestedArtifactId) {
          throw new Error("No domestic artifact is available to narrow yet.");
        }
        const payload = await loadArtifactPayload(requestedArtifactId);
        const kind = String((payload.kind as string) || (requestedArtifactId.startsWith("narrowed-domestic-") ? "domestic_narrowed" : "domestic_retrieve")).trim();
        const label = String(
          (
            payload.dataset && typeof payload.dataset === "object"
              ? (payload.dataset as Record<string, unknown>).name
              : ""
          ) || requestedArtifactId
        ).trim() || requestedArtifactId;
        const dimensionFilters = normalizeDimensionFilters(args?.dimensionFilters);
        const cleanSeriesKeyContains = String(args?.seriesKeyContains || "").trim().toLowerCase();
        const cleanStart = String(args?.start || "").trim();
        const cleanEnd = String(args?.end || "").trim();
        const maxSeries =
          typeof args?.maxSeries === "number" && Number.isFinite(args.maxSeries)
            ? Math.max(1, Math.min(Math.floor(args.maxSeries), 40))
            : 12;
        const noExplicitFilters =
          Object.keys(dimensionFilters).length === 0 &&
          !cleanSeriesKeyContains &&
          !cleanStart &&
          !cleanEnd;
        const matrixStylePayload = isMatrixStyleDomesticPayload(payload);
        if (matrixStylePayload && noExplicitFilters) {
          throw new Error(
            "For supply-use, input-output, or matrix-style domestic tables, narrow_artifact requires a specific metric/anchor filter so it can isolate one correct full matrix before python analysis."
          );
        }

        const sourceSeries = Array.isArray(payload.series) ? payload.series : [];
        if (noExplicitFilters && kind === "domestic_narrowed" && sourceSeries.length <= maxSeries) {
          const rows = flattenDomesticPayload(payload);
          const analysis = await uploadAnalysisFile(requestedArtifactId, label, rows.headers, rows.rows);
          const manifest = domesticArtifactManifest(
            requestedArtifactId,
            kind,
            label,
            `Narrowed domestic artifact '${label}'.`,
            payload,
            analysis
          );
          logger.info("Domestic MCP tool success", {
            conversationId,
            tool: name,
            duration_ms: Date.now() - startedAt,
            summary: summarizePayload(manifest),
          });
          return {
            content: [
              {
                type: "text",
                text: JSON.stringify(manifest, null, 2),
              },
            ],
          };
        }

        const narrowedSeries: Array<Record<string, unknown>> = [];
        for (const series of sourceSeries) {
          if (!series || typeof series !== "object") {
            continue;
          }
          const typedSeries = series as Record<string, unknown>;
          const seriesKey = String(typedSeries.seriesKey || "").trim().toLowerCase();
          if (cleanSeriesKeyContains && !seriesKey.includes(cleanSeriesKeyContains)) {
            continue;
          }
          const seriesDims = typedSeries.dimensions && typeof typedSeries.dimensions === "object"
            ? (typedSeries.dimensions as Record<string, unknown>)
            : {};
          let skipSeries = false;
          for (const [key, allowed] of Object.entries(dimensionFilters)) {
            const value = seriesDims[key];
            const labelValue = value && typeof value === "object"
              ? String(((value as Record<string, unknown>).label ?? (value as Record<string, unknown>).code ?? "") || "").trim()
              : String(value || "").trim();
            if (labelValue && !allowed.includes(labelValue)) {
              skipSeries = true;
              break;
            }
          }
          if (skipSeries) {
            continue;
          }
          const observations = Array.isArray(typedSeries.observations) ? typedSeries.observations : [];
          if (matrixStylePayload) {
            narrowedSeries.push({
              ...typedSeries,
              observations: observations,
            });
            if (narrowedSeries.length >= maxSeries) {
              break;
            }
            continue;
          }
          const narrowedObservations: Array<Record<string, unknown>> = [];
          for (const observation of observations) {
            if (!observation || typeof observation !== "object") {
              continue;
            }
            const typedObservation = observation as Record<string, unknown>;
            const obsDims = typedObservation.dimensions && typeof typedObservation.dimensions === "object"
              ? (typedObservation.dimensions as Record<string, unknown>)
              : {};
            let matchesDims = true;
            for (const [key, allowed] of Object.entries(dimensionFilters)) {
              const value = obsDims[key] ?? seriesDims[key];
              const labelValue = value && typeof value === "object"
                ? String(((value as Record<string, unknown>).label ?? (value as Record<string, unknown>).code ?? "") || "").trim()
                : String(value || "").trim();
              if (labelValue && !allowed.includes(labelValue)) {
                matchesDims = false;
                break;
              }
            }
            if (!matchesDims) {
              continue;
            }
            const timeValue = String(
              typedObservation.observationKey ||
              ((obsDims.TIME_PERIOD as Record<string, unknown> | undefined)?.label) ||
              ((obsDims.TIME_PERIOD as Record<string, unknown> | undefined)?.code) ||
              ""
            ).trim();
            if ((cleanStart || cleanEnd) && !matchesTimeRange(timeValue, cleanStart, cleanEnd)) {
              continue;
            }
            narrowedObservations.push(typedObservation);
          }
          if (!narrowedObservations.length) {
            continue;
          }
          narrowedSeries.push({
            ...typedSeries,
            observations: narrowedObservations,
          });
          if (narrowedSeries.length >= maxSeries) {
            break;
          }
        }

        const narrowedPayload: Record<string, unknown> = {
          ...payload,
          kind: "domestic_narrowed",
          series: narrowedSeries,
          source_references: Array.isArray(payload.source_references) ? payload.source_references : [],
        };
        const artifactId = `narrowed-domestic-${randomUUID()}`;
        const artifactPath = artifactPathForId(artifactId);
        await mkdir(path.dirname(artifactPath), { recursive: true });
        await writeFile(artifactPath, JSON.stringify(narrowedPayload, null, 2), "utf-8");
        const rows = flattenDomesticPayload(narrowedPayload);
        const estimatedBytes = estimateAnalysisCsvBytes(rows.headers, rows.rows);
        if (estimatedBytes > MAX_ANALYSIS_UPLOAD_BYTES) {
          throw new Error(
            `Narrowed artifact is still too large for python handoff (${(estimatedBytes / (1024 * 1024)).toFixed(2)}MB > 50MB). Narrow further to one correct full matrix or one metric/anchor.`
          );
        }
        const analysis = await uploadAnalysisFile(artifactId, `${label} narrowed`, rows.headers, rows.rows);
        const manifest = domesticArtifactManifest(
          artifactId,
          "domestic_narrowed",
          `${label} (narrowed)`,
          `Narrowed domestic artifact '${label}'.`,
          narrowedPayload,
          {
            parent_artifact_id: requestedArtifactId,
            analysis_estimated_bytes: estimatedBytes,
            analysis_estimated_mb: Number((estimatedBytes / (1024 * 1024)).toFixed(2)),
            ...analysis,
          }
        );
        logger.info("Domestic MCP tool success", {
          conversationId,
          tool: name,
          duration_ms: Date.now() - startedAt,
          summary: summarizePayload(manifest),
        });
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(manifest, null, 2),
            },
          ],
        };
      }
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error("Domestic MCP tool failure", {
      conversationId,
      error: errorMessage,
      tool: request.params.name,
    });
    throw new Error(`Tool execution failed: ${errorMessage}`);
  }
});

async function main() {
  try {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error("Server started successfully");
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error("Server failed to start:", errorMessage);
    process.exit(1);
  }
}

main();
