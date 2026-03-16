#!/usr/bin/env node

import path from "node:path";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { DataFlowService } from "./services/abs/DataFlowService.js";
import { DatasetResolver } from "./services/abs/DatasetResolver.js";
import { DataFormat, DataQueryOptions } from "./types/abs.js";

const dataflowCachePath = path.join(process.cwd(), "ABS_DATAFLOWS_FULL.json");
const dataFlowService = new DataFlowService(dataflowCachePath);
const datasetResolver = new DatasetResolver(dataFlowService);

const server = new Server(
  {
    name: "abs-mcp-server",
    version: "0.1.0",
    description: "Access Australian Bureau of Statistics (ABS) data"
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
        name: "list_dataflows",
        description: "List available ABS dataflows with identifiers and descriptions",
        inputSchema: {
          type: "object",
          properties: {
            forceRefresh: {
              type: "boolean",
              description: "If true, bypass cache and fetch latest dataflows from ABS"
            },
            searchQuery: {
              type: "string",
              description: "Optional local full-text search over cached ABS dataflows to return only a ranked shortlist"
            },
            limit: {
              type: "number",
              description: "Optional shortlist size when using searchQuery (default 8)"
            }
          }
        }
      },
      {
        name: "resolve_dataset",
        description:
          "Fetch ABS dataset observations and return human-readable data using metadata lookups",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "ABS dataflow identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0)"
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
        name: "get_dataflow_metadata",
        description: "Fetch structural metadata (dimensions, codes, concepts) for a specific ABS dataflow",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "ABS dataflow identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0)"
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
        description: "Direct ABS dataset request with optional data key and time filters",
        inputSchema: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: {
              type: "string",
              description:
                "ABS dataflow identifier in {agencyId},{dataflowId},{version} format (e.g., ABS,CPI,1.1.0)"
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

    switch (name) {
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
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(payload, null, 2),
            },
          ],
        };
      }
      case "get_dataflow_metadata": {
        if (!args?.datasetId || typeof args.datasetId !== "string") {
          throw new Error("datasetId is required and must be a string");
        }
        const metadata = await dataFlowService.getDataStructureForDataflow(
          args.datasetId,
          typeof args?.forceRefresh === "boolean" ? args.forceRefresh : false
        );
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(metadata, null, 2),
            },
          ],
        };
      }
      case "query_dataset": {
        if (!args?.datasetId || typeof args.datasetId !== "string") {
          throw new Error("datasetId is required and must be a string");
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
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(data, null, 2),
            },
          ],
        };
      }
      case "resolve_dataset": {
        if (!args?.datasetId || typeof args.datasetId !== "string") {
          throw new Error("datasetId is required and must be a string");
        }
        const result = await datasetResolver.resolve({
          datasetId: args.datasetId,
          dataKey: typeof args.dataKey === "string" ? args.dataKey : undefined,
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

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      }
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
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
