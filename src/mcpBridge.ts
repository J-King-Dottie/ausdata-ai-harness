#!/usr/bin/env node

import path from "node:path";
import { fileURLToPath } from "node:url";
import { mkdir } from "node:fs/promises";

import { DataFlowService } from "./services/abs/DataFlowService.js";
import { DatasetResolver } from "./services/abs/DatasetResolver.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const projectRoot = path.resolve(__dirname, "..");
const cachePath = path.join(projectRoot, "ABS_DATAFLOWS_FULL.json");

async function ensureDataflowCacheParent() {
  await mkdir(path.dirname(cachePath), { recursive: true });
}

const COMPACT_JSON = process.env.MCP_BRIDGE_COMPACT === "1";

function emit(result: unknown) {
  const payload = COMPACT_JSON
    ? JSON.stringify(result)
    : JSON.stringify(result, null, 2);
  process.stdout.write(payload);
}

async function main() {
  await ensureDataflowCacheParent();

  const [command, rawPayload] = process.argv.slice(2);
  if (!command) {
    throw new Error("A command is required (list-dataflows | resolve-dataset).");
  }

  let payload: Record<string, unknown> = {};
  if (rawPayload) {
    try {
      payload = JSON.parse(rawPayload);
    } catch (error) {
      throw new Error(`Payload must be valid JSON. Received: ${rawPayload}`);
    }
  }

  const dataFlowService = new DataFlowService(cachePath);
  const resolver = new DatasetResolver(dataFlowService);

  if (command === "list-dataflows") {
    const forceRefresh = Boolean(payload.forceRefresh);
    const searchQuery =
      typeof payload.searchQuery === "string" ? payload.searchQuery : "";
    const limit =
      typeof payload.limit === "number" && Number.isFinite(payload.limit)
        ? Math.max(1, Math.floor(payload.limit))
        : 8;
    const flows = searchQuery
      ? await dataFlowService.searchDataFlows(searchQuery, limit, forceRefresh)
      : await dataFlowService.getDataFlows(forceRefresh);
    const response = {
      total: flows.length,
      searchQuery,
      dataflows: flows,
    };
    emit(response);
    return;
  }

  if (command === "get-dataflow-metadata") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("get-dataflow-metadata requires a datasetId string.");
    }
    const metadata = await dataFlowService.getDataStructureForDataflow(
      datasetId,
      Boolean(payload.forceRefresh)
    );
    emit(metadata);
    return;
  }

  if (command === "query-dataset") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("query-dataset requires a datasetId string.");
    }
    const allowedDetails = new Set([
      "full",
      "dataonly",
      "serieskeysonly",
      "nodata",
    ]);
    const detailValue =
      typeof payload.detail === "string" && allowedDetails.has(payload.detail)
        ? (payload.detail as "full" | "dataonly" | "serieskeysonly" | "nodata")
        : undefined;
    const data = await dataFlowService.getFlowData(
      datasetId,
      typeof payload.dataKey === "string" ? payload.dataKey : "all",
      {
        startPeriod:
          typeof payload.startPeriod === "string" ? payload.startPeriod : undefined,
        endPeriod:
          typeof payload.endPeriod === "string" ? payload.endPeriod : undefined,
        detail: detailValue,
        dimensionAtObservation:
          typeof payload.dimensionAtObservation === "string"
            ? payload.dimensionAtObservation
            : undefined,
      }
    );
    emit(data);
    return;
  }

  if (command === "resolve-dataset") {
    const datasetId = payload.datasetId;
    if (typeof datasetId !== "string" || datasetId.length === 0) {
      throw new Error("resolve-dataset requires a datasetId string.");
    }

    const allowedDetails = new Set([
      "full",
      "dataonly",
      "serieskeysonly",
      "nodata",
    ]);

    const detailValue =
      typeof payload.detail === "string" && allowedDetails.has(payload.detail)
        ? (payload.detail as "full" | "dataonly" | "serieskeysonly" | "nodata")
        : undefined;

    const result = await resolver.resolve({
      datasetId,
      dataKey: typeof payload.dataKey === "string" ? payload.dataKey : undefined,
      startPeriod:
        typeof payload.startPeriod === "string" ? payload.startPeriod : undefined,
      endPeriod:
        typeof payload.endPeriod === "string" ? payload.endPeriod : undefined,
      detail: detailValue,
      dimensionAtObservation:
        typeof payload.dimensionAtObservation === "string"
          ? payload.dimensionAtObservation
          : undefined,
      forceRefresh: Boolean(payload.forceRefresh),
    });

    emit(result);
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

main().catch((error) => {
  const message =
    error instanceof Error ? error.message : `Unexpected error: ${String(error)}`;
  console.error(message);
  process.exit(1);
});
