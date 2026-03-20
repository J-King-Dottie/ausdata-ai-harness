import { Fragment, type Dispatch, type FormEvent, type KeyboardEvent, type ReactNode, type SetStateAction } from "react";
import { useEffect, useRef, useState } from "react";
import type { Session } from "@supabase/supabase-js";
import ReactECharts from "echarts-for-react";
import "./index.css";
import { supabase } from "./supabaseClient";

type Sender = "user" | "assistant" | "progress";

interface RunCost {
  model?: string;
  input_tokens?: number;
  output_tokens?: number;
  ai_cost_usd?: number;
  surcharge_usd?: number;
  final_cost_usd?: number;
}

interface ChatMessage {
  id: string;
  sender: Sender;
  content: string;
  runCost?: RunCost;
}

interface PendingMessage {
  id: string;
  userId: string;
}

interface ConversationSnapshotResponse {
  conversation_id?: unknown;
  messages?: unknown;
  run_status?: unknown;
  latest_progress?: unknown;
  latest_error?: unknown;
  pending_user_message?: unknown;
  pending_user_mode?: unknown;
  latest_export_url?: unknown;
  latest_export_status?: unknown;
}

interface ChatAcceptedResponse {
  conversation_id?: unknown;
  run_status?: unknown;
  latest_progress?: unknown;
}

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const STORAGE_KEY = "abs-analyst-session";
const MAX_POLL_FAILURES = 20;
const USD_TO_AUD_RATE = 1.398;
const EXAMPLE_PROMPTS = [
  "What data do you have access to?",
];

function createConversationId() {
  if (crypto && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

interface ChartPoint {
  x: string;
  y: number;
}

type ChartType = "line" | "bar" | "area" | "scatter" | "stacked_bar" | "stacked_area";

interface ChartSeries {
  name: string;
  color?: string;
  points: ChartPoint[];
}

interface ChartSpec {
  type?: ChartType;
  title?: string;
  xLabel?: string;
  yLabel?: string;
  series: ChartSeries[];
}

type ContentBlock =
  | { type: "heading"; level: 1 | 2 | 3 | 4 | 5 | 6; text: string }
  | { type: "paragraph"; lines: string[] }
  | { type: "list"; items: string[] }
  | { type: "ordered-list"; items: string[] }
  | { type: "table"; headers: string[]; rows: string[][] }
  | { type: "code"; code: string; language: string }
  | { type: "chart"; spec: ChartSpec };

function renderInlineMarkdown(value: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  const pattern = /(\[[^\]]+\]\([^)]+\)|`[^`]+`|\*\*[^*]+\*\*)/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null = null;
  let key = 0;

  while ((match = pattern.exec(value)) !== null) {
    if (match.index > lastIndex) {
      nodes.push(value.slice(lastIndex, match.index));
    }
    const token = match[0];
    if (token.startsWith("[")) {
      const linkMatch = token.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
      if (linkMatch) {
        nodes.push(
          <a key={`link-${key++}`} href={linkMatch[2]} target="_blank" rel="noreferrer">
            {linkMatch[1]}
          </a>
        );
      } else {
        nodes.push(token);
      }
    } else if (token.startsWith("`")) {
      nodes.push(<code key={`code-${key++}`}>{token.slice(1, -1)}</code>);
    } else if (token.startsWith("**")) {
      nodes.push(<strong key={`strong-${key++}`}>{token.slice(2, -2)}</strong>);
    }
    lastIndex = pattern.lastIndex;
  }

  if (lastIndex < value.length) {
    nodes.push(value.slice(lastIndex));
  }

  return nodes;
}

function isTableLine(value: string) {
  const trimmed = value.trim();
  return trimmed.includes("|") && trimmed.replaceAll("|", "").trim().length > 0;
}

function isTableSeparator(line: string) {
  const cells = line
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((cell) => cell.trim());
  return cells.length > 0 && cells.every((cell) => /^:?-{3,}:?$/.test(cell));
}

function parseTableCells(line: string) {
  return line
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((cell) => cell.trim());
}

function parseChartBlock(raw: string): ChartSpec | null {
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.series)) {
      return null;
    }
    const series = parsed.series
      .map((entry: unknown) => {
        if (!entry || typeof entry !== "object" || !Array.isArray((entry as { points?: unknown[] }).points)) {
          return null;
        }
        const typed = entry as { name?: unknown; color?: unknown; points: Array<{ x?: unknown; y?: unknown }> };
        const points = typed.points
          .map((point) => {
            const y = Number(point?.y);
            const x = String(point?.x ?? "");
            if (!x || !Number.isFinite(y)) {
              return null;
            }
            return { x, y };
          })
          .filter((point): point is ChartPoint => point !== null);
        if (!points.length) {
          return null;
        }
        return {
          name: String(typed.name ?? "Series"),
          color: typeof typed.color === "string" ? typed.color : undefined,
          points,
        };
      })
      .filter((entry: ChartSeries | null): entry is ChartSeries => entry !== null);

    if (!series.length) {
      return null;
    }

    const supportedTypes = new Set<ChartType>([
      "line",
      "bar",
      "area",
      "scatter",
      "stacked_bar",
      "stacked_area",
    ]);
    const chartType: ChartType =
      typeof parsed.type === "string" && supportedTypes.has(parsed.type as ChartType)
        ? (parsed.type as ChartType)
        : "line";

    return {
      type: chartType,
      title: typeof parsed.title === "string" ? parsed.title : undefined,
      xLabel: typeof parsed.xLabel === "string" ? parsed.xLabel : undefined,
      yLabel: typeof parsed.yLabel === "string" ? parsed.yLabel : undefined,
      series,
    };
  } catch {
    return null;
  }
}

function maybeChartLanguage(language: string) {
  return language === "chart" || language === "json" || language === "";
}

function parseHeadingLine(value: string) {
  const match = value.trim().match(/^(#{1,6})\s+(.+)$/);
  if (!match) {
    return null;
  }
  return {
    level: match[1].length as 1 | 2 | 3 | 4 | 5 | 6,
    text: match[2].trim(),
  };
}

function parseContentBlocks(value: string): ContentBlock[] {
  const normalized = value.replace(/\r\n/g, "\n").trim();
  if (!normalized) {
    return [];
  }

  const lines = normalized.split("\n");
  const blocks: ContentBlock[] = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      index += 1;
      continue;
    }

    const heading = parseHeadingLine(trimmed);
    if (heading) {
      blocks.push({ type: "heading", level: heading.level, text: heading.text });
      index += 1;
      continue;
    }

    if (trimmed.startsWith("```")) {
      const language = trimmed.slice(3).trim().toLowerCase();
      index += 1;
      const codeLines: string[] = [];
      while (index < lines.length && !lines[index].trim().startsWith("```")) {
        codeLines.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) {
        index += 1;
      }
      const raw = codeLines.join("\n");
      const spec = maybeChartLanguage(language) ? parseChartBlock(raw) : null;
      if (spec) {
        blocks.push({ type: "chart", spec });
      } else {
        blocks.push({ type: "code", code: raw, language });
      }
      continue;
    }

    if (
      index + 1 < lines.length &&
      isTableLine(line) &&
      isTableSeparator(lines[index + 1])
    ) {
      const headers = parseTableCells(line);
      index += 2;
      const rows: string[][] = [];
      while (index < lines.length && isTableLine(lines[index]) && !lines[index].trim().startsWith("```")) {
        rows.push(parseTableCells(lines[index]));
        index += 1;
      }
      blocks.push({ type: "table", headers, rows });
      continue;
    }

    if (trimmed.startsWith("- ")) {
      const items: string[] = [];
      while (index < lines.length && lines[index].trim().startsWith("- ")) {
        items.push(lines[index].trim().slice(2));
        index += 1;
      }
      blocks.push({ type: "list", items });
      continue;
    }

    if (/^\d+\.\s+/.test(trimmed)) {
      const items: string[] = [];
      while (index < lines.length && /^\d+\.\s+/.test(lines[index].trim())) {
        items.push(lines[index].trim().replace(/^\d+\.\s+/, ""));
        index += 1;
      }
      blocks.push({ type: "ordered-list", items });
      continue;
    }

    const paragraphLines: string[] = [];
    while (index < lines.length) {
      const candidate = lines[index];
      const candidateTrimmed = candidate.trim();
      if (!candidateTrimmed) {
        break;
      }
      if (
        candidateTrimmed.startsWith("```") ||
        candidateTrimmed.startsWith("- ") ||
        /^\d+\.\s+/.test(candidateTrimmed) ||
        parseHeadingLine(candidateTrimmed)
      ) {
        break;
      }
      if (
        index + 1 < lines.length &&
        isTableLine(candidate) &&
        isTableSeparator(lines[index + 1])
      ) {
        break;
      }
      paragraphLines.push(candidateTrimmed);
      index += 1;
    }
    if (paragraphLines.length === 1) {
      const spec = parseChartBlock(paragraphLines[0]);
      if (spec) {
        blocks.push({ type: "chart", spec });
        continue;
      }
    }
    blocks.push({ type: "paragraph", lines: paragraphLines });
  }

  return blocks;
}

function formatTick(value: number) {
  if (Math.abs(value) >= 1000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }
  if (Math.abs(value) >= 10) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 1 });
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function ChartBlock({ spec }: { spec: ChartSpec }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [containerWidth, setContainerWidth] = useState(0);
  const colors = ["#234233", "#8f6a3a", "#54745f", "#b45f3a"];
  const chartType = spec.type || "line";
  const isBarLike = chartType === "bar" || chartType === "stacked_bar";
  const isLineLike = chartType === "line" || chartType === "area" || chartType === "stacked_area";
  const isAreaLike = chartType === "area" || chartType === "stacked_area";
  const isStacked = chartType === "stacked_bar" || chartType === "stacked_area";
  const isScatter = chartType === "scatter";
  const allPoints = spec.series.flatMap((series) => series.points);
  const rawXValues = Array.from(new Set(allPoints.map((point) => point.x)));
  const scatterNumericX = isScatter && allPoints.every((point) => Number.isFinite(Number(point.x)));
  const xValues = scatterNumericX
    ? [...rawXValues].sort((a, b) => Number(a) - Number(b))
    : rawXValues;
  const longestXAxisLabelLength = xValues.reduce((max, value) => Math.max(max, value.length), 0);
  const longestSeries = Math.max(...spec.series.map((series) => series.points.length), 0);
  const isNarrow = containerWidth > 0 && containerWidth < 640;
  const useHorizontalBars =
    isBarLike && (isNarrow || xValues.length > 10 || longestXAxisLabelLength > 16);
  const rotateVerticalLabels =
    isBarLike && !useHorizontalBars && (xValues.length > 7 || longestXAxisLabelLength > 12);
  const chartHeight =
    isBarLike && useHorizontalBars
      ? Math.max(360, xValues.length * 28 + 120)
      : 360;

  useEffect(() => {
    const element = containerRef.current;
    if (!element || typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) {
        return;
      }
      setContainerWidth(entry.contentRect.width);
    });
    observer.observe(element);
    setContainerWidth(element.getBoundingClientRect().width);
    return () => observer.disconnect();
  }, []);

  const option = {
    animationDuration: 320,
    color: spec.series.map((series, index) => series.color || colors[index % colors.length]),
    textStyle: {
      color: "rgba(30, 43, 33, 0.78)",
      fontFamily: "IBM Plex Sans, Segoe UI, sans-serif",
    },
    grid: useHorizontalBars
      ? {
          top: spec.series.length > 1 ? (spec.title ? 54 : 42) : spec.title ? 20 : 8,
          right: 20,
          bottom: spec.xLabel ? 52 : 22,
          left: Math.min(220, Math.max(110, longestXAxisLabelLength * 7)),
          containLabel: false,
        }
      : {
          top: spec.series.length > 1 ? (spec.title ? 54 : 42) : spec.title ? 20 : 8,
          right: 20,
          bottom: rotateVerticalLabels ? 92 : spec.xLabel ? 54 : 30,
          left: spec.yLabel ? 72 : 58,
          containLabel: false,
        },
    tooltip: {
      trigger: isScatter && scatterNumericX ? "item" : "axis",
      confine: true,
      backgroundColor: "rgba(245, 240, 227, 0.96)",
      borderColor: "rgba(30, 43, 33, 0.12)",
      borderWidth: 1,
      textStyle: {
        color: "#1e2b21",
      },
      axisPointer: {
        type: isBarLike ? "shadow" : "line",
        lineStyle: {
          color: "rgba(30, 43, 33, 0.22)",
        },
        shadowStyle: {
          color: "rgba(30, 43, 33, 0.06)",
        },
      },
    },
    legend:
      spec.series.length > 1
        ? {
            top: spec.title ? 18 : 6,
            left: "center",
            icon: "circle",
            itemWidth: 10,
            itemHeight: 10,
            textStyle: {
              color: "rgba(30, 43, 33, 0.72)",
              fontSize: 12,
            },
          }
        : undefined,
    xAxis: useHorizontalBars
      ? {
          type: "value",
          name: spec.xLabel,
          nameLocation: "middle",
          nameGap: spec.xLabel ? 36 : 0,
          axisLabel: {
            color: "rgba(30, 43, 33, 0.58)",
            fontSize: 11,
            formatter: (value: number) => formatTick(Number(value)),
          },
          splitLine: {
            lineStyle: {
              color: "rgba(30, 43, 33, 0.09)",
              type: [2, 5],
            },
          },
          axisLine: {
            lineStyle: {
              color: "rgba(30, 43, 33, 0.18)",
            },
          },
          axisTick: {
            show: false,
          },
        }
      : {
          type: scatterNumericX ? "value" : "category",
          data: scatterNumericX ? undefined : xValues,
          name: spec.xLabel,
          nameLocation: "middle",
          nameGap: rotateVerticalLabels ? 78 : spec.xLabel ? 34 : 0,
          axisLabel: {
            color: "rgba(30, 43, 33, 0.58)",
            fontSize: 11,
            interval: isBarLike ? 0 : "auto",
            hideOverlap: true,
            rotate: rotateVerticalLabels ? -40 : 0,
            width: rotateVerticalLabels ? 96 : 88,
            overflow: "truncate",
            formatter: scatterNumericX ? (value: number) => formatTick(Number(value)) : undefined,
          },
          axisLine: {
            lineStyle: {
              color: "rgba(30, 43, 33, 0.18)",
            },
          },
          axisTick: {
            show: false,
          },
        },
    yAxis: useHorizontalBars
      ? {
          type: "category",
          data: xValues,
          name: spec.yLabel,
          nameLocation: "middle",
          nameGap: spec.yLabel ? 92 : 0,
          axisLabel: {
            color: "rgba(30, 43, 33, 0.62)",
            fontSize: 11,
            width: Math.min(200, Math.max(120, longestXAxisLabelLength * 7)),
            overflow: "truncate",
          },
          axisTick: {
            show: false,
          },
          axisLine: {
            show: false,
          },
        }
      : {
          type: "value",
          name: spec.yLabel,
          nameLocation: "middle",
          nameGap: spec.yLabel ? 52 : 0,
          axisLabel: {
            color: "rgba(30, 43, 33, 0.58)",
            fontSize: 11,
            formatter: (value: number) => formatTick(Number(value)),
          },
          splitLine: {
            lineStyle: {
              color: "rgba(30, 43, 33, 0.09)",
              type: [2, 5],
            },
          },
          axisLine: {
            lineStyle: {
              color: "rgba(30, 43, 33, 0.18)",
            },
          },
          axisTick: {
            show: false,
          },
          min: (value: { min: number; max: number }) =>
            value.min === value.max ? value.min - 1 : value.min - (value.max - value.min) * 0.08,
          max: (value: { min: number; max: number }) =>
            value.min === value.max ? value.max + 1 : value.max + (value.max - value.min) * 0.08,
        },
    series: spec.series.map((series) => {
      const data = isScatter && scatterNumericX
        ? series.points
            .map((point) => [Number(point.x), point.y])
            .filter((point) => Number.isFinite(point[0]) && Number.isFinite(point[1]))
        : xValues.map((xValue) => {
            const match = series.points.find((point) => point.x === xValue);
            return match ? match.y : null;
          });
      return {
        name: series.name,
        type: isBarLike ? "bar" : isScatter ? "scatter" : "line",
        data,
        stack: isStacked ? "total" : undefined,
        barMaxWidth: 28,
        barCategoryGap: spec.series.length > 1 ? "34%" : "42%",
        smooth: isLineLike && !isAreaLike && spec.series.length === 1 ? 0.15 : 0,
        showSymbol: (isLineLike || isScatter) && longestSeries <= 16 && spec.series.length <= 2,
        symbolSize: isScatter ? 9 : 6,
        lineStyle: {
          width: isScatter ? 0 : spec.series.length > 1 ? 2.4 : 2.8,
        },
        areaStyle: isAreaLike ? { opacity: isStacked ? 0.82 : 0.18 } : undefined,
        itemStyle: {
          borderRadius: isBarLike ? [4, 4, 0, 0] : 0,
        },
        emphasis: {
          focus: "series",
        },
      };
    }),
  };

  return (
    <section className="chart-block">
      {spec.title && <h3>{spec.title}</h3>}
      <div ref={containerRef} className="chart-frame">
        <ReactECharts
          option={option}
          notMerge
          lazyUpdate
          style={{ width: "100%", height: `${chartHeight}px` }}
          className="chart-echart"
        />
      </div>
    </section>
  );
}

function NisabaLoader() {
  return (
    <svg
      className="nisaba-loader"
      viewBox="0 0 80 60"
      role="img"
      aria-hidden="true"
    >
      <g className="nisaba-loader-group">
        <rect
          className="nisaba-stroke nisaba-border nisaba-stroke-1"
          x="8"
          y="7"
          width="64"
          height="46"
          rx="10"
          pathLength={1}
        />

        <g className="nisaba-wedge nisaba-stroke-2">
          <line className="nisaba-wedge-line" x1="26" y1="24" x2="39" y2="19" pathLength={1} />
          <path className="nisaba-wedge-press" d="M19 25 L26 19 L26 29 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-3">
          <line className="nisaba-wedge-line" x1="45" y1="24" x2="58" y2="19" pathLength={1} />
          <path className="nisaba-wedge-press" d="M38 25 L45 19 L45 29 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-4">
          <line className="nisaba-wedge-line" x1="33" y1="40" x2="46" y2="35" pathLength={1} />
          <path className="nisaba-wedge-press" d="M26 41 L33 35 L33 45 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-5">
          <line className="nisaba-wedge-line" x1="52" y1="40" x2="65" y2="35" pathLength={1} />
          <path className="nisaba-wedge-press" d="M45 41 L52 35 L52 45 Z" />
        </g>
      </g>
    </svg>
  );
}

function renderContentBlocks(value: string) {
  const blocks = parseContentBlocks(value);
  return blocks.map((block, index) => {
    if (block.type === "heading") {
      const HeadingTag = `h${block.level}` as const;
      return <HeadingTag key={`heading-${index}`}>{renderInlineMarkdown(block.text)}</HeadingTag>;
    }

    if (block.type === "paragraph") {
      return (
        <p key={`p-${index}`}>
          {block.lines.map((line, lineIndex) => (
            <Fragment key={`line-${lineIndex}`}>
              {lineIndex > 0 ? <br /> : null}
              {renderInlineMarkdown(line)}
            </Fragment>
          ))}
        </p>
      );
    }

    if (block.type === "list") {
      return (
        <ul key={`list-${index}`}>
          {block.items.map((item, itemIndex) => (
            <li key={`item-${itemIndex}`}>{renderInlineMarkdown(item)}</li>
          ))}
        </ul>
      );
    }

    if (block.type === "ordered-list") {
      return (
        <ol key={`olist-${index}`}>
          {block.items.map((item, itemIndex) => (
            <li key={`oitem-${itemIndex}`}>{renderInlineMarkdown(item)}</li>
          ))}
        </ol>
      );
    }

    if (block.type === "table") {
      return (
        <div key={`table-${index}`} className="table-scroll">
          <table className="message-table">
            <thead>
              <tr>
                {block.headers.map((header, headerIndex) => (
                  <th key={`header-${headerIndex}`}>{renderInlineMarkdown(header)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {block.rows.map((row, rowIndex) => (
                <tr key={`row-${rowIndex}`}>
                  {block.headers.map((_, cellIndex) => (
                    <td key={`cell-${rowIndex}-${cellIndex}`}>
                      {renderInlineMarkdown(row[cellIndex] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    }

    if (block.type === "code") {
      return (
        <pre key={`code-${index}`} className="message-code-block">
          <code>{block.code}</code>
        </pre>
      );
    }

    return <ChartBlock key={`chart-${index}`} spec={block.spec} />;
  });
}

function simplifyStatusMessage(value: string) {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }

  const toolResultMatch = normalized.match(/^(.*?)(?:\s+result_json(?:_preview)?:|\s+created artifacts:)/i);
  if (toolResultMatch) {
    const summary = toolResultMatch[1]
      .replace(/artifact-\d+/gi, "the data")
      .replace(/\s+/g, " ")
      .trim();
    if (/^Creating a .*chart /i.test(summary) || /^Building a .*chart /i.test(summary)) {
      return "Preparing the chart.";
    }
    if (/^Retrieving .*chart content/i.test(summary) || /^Reading .*chart file/i.test(summary)) {
      return "Preparing the chart for display.";
    }
    if (/^Composing final answer/i.test(summary)) {
      return "Writing the final answer.";
    }
    return summary || "Working through the ABS results.";
  }

  if (/^Loop \d+: reasoning about the next step\.?$/i.test(normalized)) {
    return "";
  }
  if (/^Plan approved\./i.test(normalized)) {
    return "Continuing with the approved approach.";
  }
  if (/^Retrieved and resolved /i.test(normalized)) {
    return "Fetched the ABS data. Reviewing the results.";
  }
  if (/^Resolved ABS dataset /i.test(normalized)) {
    return "Working through the ABS results.";
  }
  if (/^Tool execution failed\./i.test(normalized)) {
    return "Adjusting the approach after a failed step.";
  }
  if (normalized.startsWith("{") || normalized.startsWith("[")) {
    return "Fetched structured ABS output. Summarising it.";
  }
  if (normalized.length > 180) {
    return "Working through the ABS results.";
  }
  return normalized;
}

function loadSavedSession() {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.sessionStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as {
      conversationId?: unknown;
      messages?: unknown;
      latestExportUrl?: unknown;
      latestExportStatus?: unknown;
    };
    const conversationId =
      typeof parsed.conversationId === "string" && parsed.conversationId.trim()
        ? parsed.conversationId
        : createConversationId();
    const messages = Array.isArray(parsed.messages)
      ? parsed.messages.filter(
          (message): message is ChatMessage =>
            !!message &&
            typeof message === "object" &&
            typeof (message as ChatMessage).id === "string" &&
            ((message as ChatMessage).sender === "user" ||
              (message as ChatMessage).sender === "assistant" ||
              (message as ChatMessage).sender === "progress") &&
            typeof (message as ChatMessage).content === "string"
        )
      : [];
    const latestExportUrl =
      typeof parsed.latestExportUrl === "string" ? parsed.latestExportUrl.trim() : "";
    const latestExportStatus =
      typeof parsed.latestExportStatus === "string" ? parsed.latestExportStatus.trim().toLowerCase() : "";
    return { conversationId, messages, latestExportUrl, latestExportStatus };
  } catch {
    return null;
  }
}

function mapBackendMessages(rawMessages: unknown): ChatMessage[] {
  if (!Array.isArray(rawMessages)) {
    return [];
  }
  return rawMessages.flatMap((message) => {
    if (!message || typeof message !== "object") {
      return [];
    }
    const typed = message as { role?: unknown; content?: unknown; run_cost?: unknown };
    const role = typeof typed.role === "string" ? typed.role.trim().toLowerCase() : "";
    const content = typeof typed.content === "string" ? typed.content : "";
    if (!content.trim()) {
      return [];
    }
    if (role !== "user" && role !== "assistant" && role !== "progress") {
      return [];
    }
    const rawRunCost = typed.run_cost;
    const runCost =
      rawRunCost && typeof rawRunCost === "object"
        ? {
            model: typeof (rawRunCost as RunCost).model === "string" ? (rawRunCost as RunCost).model : undefined,
            input_tokens: Number((rawRunCost as RunCost).input_tokens),
            output_tokens: Number((rawRunCost as RunCost).output_tokens),
            ai_cost_usd: Number((rawRunCost as RunCost).ai_cost_usd),
            surcharge_usd: Number((rawRunCost as RunCost).surcharge_usd),
            final_cost_usd: Number((rawRunCost as RunCost).final_cost_usd),
          }
        : undefined;
    return [
      {
        id: createConversationId(),
        sender: role as Sender,
        content,
        runCost,
      } satisfies ChatMessage,
    ];
  });
}

function formatUsd(value: number | undefined) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "A$0.00";
  }
  return `A$${numeric.toFixed(2)}`;
}

function renderRunCost(runCost?: RunCost) {
  if (!runCost) {
    return null;
  }
  const finalCost = Number(runCost.final_cost_usd);
  const aiCost = Number(runCost.ai_cost_usd);
  const surcharge = Number(runCost.surcharge_usd);
  const displayCost = Number.isFinite(finalCost) ? finalCost : aiCost;
  if (!Number.isFinite(displayCost)) {
    return null;
  }
  const hoverParts = [
    Number.isFinite(aiCost) ? `Raw: ${formatUsd(aiCost * USD_TO_AUD_RATE)}` : "",
    Number.isFinite(surcharge) ? `10%: ${formatUsd(surcharge * USD_TO_AUD_RATE)}` : "",
    `Total: ${formatUsd(displayCost * USD_TO_AUD_RATE)}`,
  ].filter(Boolean);
  return (
    <span className="assistant-run-cost-wrap">
      <span className="assistant-run-cost-trigger" tabIndex={0}>
        <span className="assistant-run-cost">
          {formatUsd(displayCost * USD_TO_AUD_RATE)}
        </span>
        <div className="assistant-run-cost-tooltip" role="tooltip">
          {hoverParts.map((part) => (
            <p key={part}>{part}</p>
          ))}
        </div>
      </span>
    </span>
  );
}

function keepCompletedTurns(messages: ChatMessage[]) {
  const completed: ChatMessage[] = [];
  let index = 0;
  while (index < messages.length) {
    const userMessage = messages[index];
    if (userMessage?.sender !== "user" || !userMessage.content.trim()) {
      index += 1;
      continue;
    }

    let cursor = index + 1;
    while (cursor < messages.length && messages[cursor]?.sender === "progress") {
      cursor += 1;
    }

    const assistantMessage = messages[cursor];
    if (assistantMessage?.sender !== "assistant" || !assistantMessage.content.trim()) {
      break;
    }

    completed.push(...messages.slice(index, cursor + 1));
    index = cursor + 1;
  }
  return completed;
}

function applyConversationSnapshot(
  payload: ConversationSnapshotResponse,
  setMessages: Dispatch<SetStateAction<ChatMessage[]>>,
  assistantMessageId?: string,
  forceReplace = false
) {
  const runStatus = String(payload.run_status ?? "").trim().toLowerCase();
  const mappedMessages = mapBackendMessages(payload.messages);
  const restoredMessages = runStatus === "completed" ? mappedMessages : keepCompletedTurns(mappedMessages);
  if (restoredMessages.length > 0 || forceReplace) {
    setMessages(restoredMessages);
    return;
  }
  if (!assistantMessageId) {
    return;
  }
  setMessages((prev) =>
    prev.map((message) =>
      message.id === assistantMessageId
        ? { ...message, content: "There was an error generating a response." }
        : message
    )
  );
}

function appendProgressMessage(
  setMessages: Dispatch<SetStateAction<ChatMessage[]>>,
  assistantMessageId: string,
  content: string
) {
  setMessages((prev) => {
    const next = [...prev];
    const assistantIndex = next.findIndex((msg) => msg.id === assistantMessageId);
    const insertionIndex = assistantIndex === -1 ? next.length : assistantIndex;
    next.splice(insertionIndex, 0, {
      id: createConversationId(),
      sender: "progress",
      content,
    });
    return next;
  });
}

function isProgressSubtask(content: string) {
  return content === "Code generated." || content === "Code run.";
}

function ProductTitle() {
  return (
    <div className="product-title">
      <div className="product-title-text">
        <div className="product-title-row product-title-row-main">
          <h1>Nisaba</h1>
        </div>
        <div className="product-title-row product-title-row-subtitle">
          <div className="product-subtitle-group">
          <div className="info-action">
            <span className="subtitle-info-trigger">
              <span>an AI economic analyst</span>
              <span className="subtitle-info-icon" aria-hidden="true" tabIndex={0}>
                <svg viewBox="0 0 12 12" focusable="false">
                  <circle cx="6" cy="6" r="5.25" fill="none" stroke="currentColor" strokeWidth="1" />
                  <circle cx="6" cy="3.45" r="0.7" fill="currentColor" />
                  <rect x="5.4" y="4.95" width="1.2" height="3.75" rx="0.6" fill="currentColor" />
                </svg>
                <div className="header-tooltip info-tooltip" role="tooltip">
                  <p>
                    In Sumerian mythology, Nisaba was the goddess of writing,
                    accounting, and the keeping of records.
                  </p>
                  <p>
                    Writing emerged in Mesopotamian bureaucracies to count what
                    mattered. Grain, livestock, labour, taxes.
                  </p>
                  <p>
                    This system does the same. Designed for Australian analysts,
                    it combines detailed domestic data with global macro
                    sources including UN Comtrade trade data.
                  </p>
                  <p>
                    Ask anything and Nisaba will find the numbers and explain
                    what they mean.
                  </p>
                  <p>
                    Produced by{" "}
                    <a href="https://dottieaistudio.com.au/" target="_blank" rel="noreferrer">
                      Dottie AI Studio
                    </a>
                    {" · "}
                    open source{" "}
                    <a
                      href="https://github.com/J-King-Dottie/ausdata-ai-harness"
                      target="_blank"
                      rel="noreferrer"
                    >
                      ausdata-ai-harness
                    </a>
                  </p>
                  <div className="info-tooltip-summary">
                    <div className="table-scroll">
                      <table className="info-tooltip-table">
                        <thead>
                          <tr>
                            <th>Route</th>
                            <th>Provider</th>
                            <th>Datasets</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td>Domestic</td>
                            <td>ABS</td>
                            <td>1,221</td>
                          </tr>
                          <tr>
                            <td>Domestic</td>
                            <td>DCCEEW</td>
                            <td>1</td>
                          </tr>
                          <tr>
                            <td>Domestic</td>
                            <td>RBA</td>
                            <td>71</td>
                          </tr>
                          <tr>
                            <td>Macro</td>
                            <td>OECD</td>
                            <td>1,464</td>
                          </tr>
                          <tr>
                            <td>Macro</td>
                            <td>World Bank</td>
                            <td>28,377</td>
                          </tr>
                          <tr>
                            <td>Macro</td>
                            <td>IMF</td>
                            <td>132</td>
                          </tr>
                          <tr>
                            <td>Macro</td>
                            <td>UN Comtrade</td>
                            <td>1</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              </span>
            </span>
          </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function App() {
  const savedSession = loadSavedSession();
  const [messages, setMessages] = useState<ChatMessage[]>(() => savedSession?.messages ?? []);
  const [conversationId, setConversationId] = useState<string>(
    () => savedSession?.conversationId ?? createConversationId()
  );
  const [authReady, setAuthReady] = useState(false);
  const [session, setSession] = useState<Session | null>(null);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [authError, setAuthError] = useState<string | null>(null);
  const [authBusy, setAuthBusy] = useState(false);
  const [input, setInput] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [queuedMessage, setQueuedMessage] = useState("");
  const [queuedMode, setQueuedMode] = useState<"" | "queued" | "steer">("");
  const [latestExportUrl, setLatestExportUrl] = useState(savedSession?.latestExportUrl ?? "");
  const [latestExportStatus, setLatestExportStatus] = useState(savedSession?.latestExportStatus ?? "");
  const scrollRef = useRef<HTMLElement | null>(null);
  const pendingRef = useRef<PendingMessage | null>(null);
  const lastProgressRef = useRef("");
  const pollFailureCountRef = useRef(0);
  const composerRef = useRef<HTMLTextAreaElement | null>(null);
  const hydratedConversationRef = useRef("");
  const queuedSubmitRef = useRef(false);
  const displayName =
    String(
      session?.user?.user_metadata?.display_name ||
        session?.user?.user_metadata?.full_name ||
        session?.user?.email ||
        ""
    ).trim() || "Signed in";

  const syncComposerHeight = () => {
    const element = composerRef.current;
    if (!element) {
      return;
    }
    element.style.height = "0px";
    const maxHeight = Number.parseFloat(window.getComputedStyle(element).maxHeight || "0");
    const nextHeight = element.scrollHeight;
    element.style.height = `${nextHeight}px`;
    element.style.overflowY = maxHeight > 0 && nextHeight > maxHeight ? "auto" : "hidden";
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isStreaming]);

  useEffect(() => {
    syncComposerHeight();
  }, [input]);

  const syncPendingState = (payload: ConversationSnapshotResponse) => {
    const nextMessage = String(payload.pending_user_message ?? "").trim();
    const rawMode = String(payload.pending_user_mode ?? "").trim().toLowerCase();
    const nextMode = rawMode === "queued" || rawMode === "steer" ? rawMode : "";
    setQueuedMessage(nextMessage);
    setQueuedMode(nextMode);
    setLatestExportUrl(String(payload.latest_export_url ?? "").trim());
    setLatestExportStatus(String(payload.latest_export_status ?? "").trim().toLowerCase());
  };

  const storePendingMessage = async (message: string, mode: "queued" | "steer") => {
    const response = await fetch(`${API_BASE}/api/pending-message`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        conversation_id: conversationId,
        message,
        mode,
      }),
    });
    if (!response.ok) {
      throw new Error(`Failed to store pending message: ${response.status}`);
    }
    const payload = (await response.json()) as ConversationSnapshotResponse;
    syncPendingState(payload);
    return payload;
  };

  const consumePendingMessage = async () => {
    const response = await fetch(`${API_BASE}/api/pending-message/consume`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ conversation_id: conversationId }),
    });
    if (!response.ok) {
      throw new Error(`Failed to clear pending message: ${response.status}`);
    }
    const payload = (await response.json()) as ConversationSnapshotResponse;
    syncPendingState(payload);
    return payload;
  };

  useEffect(() => {
    let active = true;

    supabase.auth.getSession().then(({ data, error: sessionError }) => {
      if (!active) {
        return;
      }
      if (sessionError) {
        setAuthError(sessionError.message);
      }
      setSession(data.session ?? null);
      setAuthReady(true);
    });

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
      setAuthReady(true);
      setAuthError(null);
    });

    return () => {
      active = false;
      subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const completedMessages = keepCompletedTurns(messages);
    window.sessionStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        conversationId,
        messages: completedMessages,
        latestExportUrl,
        latestExportStatus,
      })
    );
  }, [conversationId, messages, latestExportUrl, latestExportStatus]);

  useEffect(() => {
    if (typeof window === "undefined" || !conversationId) {
      return;
    }

    const cancelActiveRun = () => {
      if (!pendingRef.current) {
        return;
      }
      const payload = JSON.stringify({ conversation_id: conversationId });
      const url = `${API_BASE}/api/cancel`;
      try {
        if (navigator.sendBeacon) {
          const blob = new Blob([payload], { type: "application/json" });
          navigator.sendBeacon(url, blob);
          return;
        }
      } catch (cancelError) {
        console.error("Failed to send cancellation beacon", cancelError);
      }

      void fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payload,
        keepalive: true,
      }).catch((cancelError) => {
        console.error("Failed to cancel active run", cancelError);
      });
    };

    const handlePageHide = () => {
      cancelActiveRun();
    };

    window.addEventListener("pagehide", handlePageHide);
    return () => {
      window.removeEventListener("pagehide", handlePageHide);
    };
  }, [conversationId]);

  useEffect(() => {
    if (!authReady || !session || !conversationId) {
      return;
    }
    if (hydratedConversationRef.current === conversationId) {
      return;
    }

    let active = true;
    hydratedConversationRef.current = conversationId;
    setIsStreaming(false);
    pendingRef.current = null;
    lastProgressRef.current = "";

    void fetch(`${API_BASE}/api/conversation/${encodeURIComponent(conversationId)}`)
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`Failed to load conversation: ${response.status}`);
        }
        return (await response.json()) as ConversationSnapshotResponse;
      })
      .then((payload) => {
        if (!active) {
          return;
        }
        applyConversationSnapshot(payload, setMessages, undefined, true);
        syncPendingState(payload);
        const runStatus = String(payload.run_status ?? "");
        if (runStatus === "processing") {
          pendingRef.current = null;
          lastProgressRef.current = "";
          setIsStreaming(false);
          void fetch(`${API_BASE}/api/cancel`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ conversation_id: conversationId }),
            keepalive: true,
          })
            .then(() =>
              fetch(`${API_BASE}/api/conversation/${encodeURIComponent(conversationId)}`)
            )
            .then(async (response) => {
              if (!response.ok) {
                throw new Error(`Failed to reload conversation: ${response.status}`);
              }
              return (await response.json()) as ConversationSnapshotResponse;
            })
            .then((cancelledPayload) => {
              if (!active) {
                return;
              }
              applyConversationSnapshot(cancelledPayload, setMessages, undefined, true);
              syncPendingState(cancelledPayload);
            })
            .catch((cancelError) => {
              console.error("Failed to cancel stale background run", cancelError);
            });
        }
        const latestError = String(payload.latest_error ?? "").trim();
        if (latestError) {
          setError(latestError);
        }
      })
      .catch((loadError) => {
        console.error("Failed to load saved conversation", loadError);
      });

    return () => {
      active = false;
    };
  }, [authReady, conversationId, session]);

  const resetConversation = async () => {
    try {
      await fetch(`${API_BASE}/api/reset`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ conversation_id: conversationId }),
        keepalive: true,
      });
    } catch (resetError) {
      console.error("Failed to reset conversation", resetError);
    }

    setMessages([]);
    setInput("");
    setError(null);
    setIsStreaming(false);
    setQueuedMessage("");
    setQueuedMode("");
    setLatestExportUrl("");
    setLatestExportStatus("");
    pendingRef.current = null;
    lastProgressRef.current = "";
    queuedSubmitRef.current = false;
    hydratedConversationRef.current = "";
    setConversationId(createConversationId());
    if (typeof window !== "undefined") {
      window.sessionStorage.removeItem(STORAGE_KEY);
    }
  };

  const handleLogin = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!email.trim() || !password) {
      setAuthError("Enter your email and password.");
      return;
    }
    setAuthBusy(true);
    setAuthError(null);
    const { error: signInError } = await supabase.auth.signInWithPassword({
      email: email.trim(),
      password,
    });
    if (signInError) {
      setAuthError(signInError.message);
    } else {
      setPassword("");
    }
    setAuthBusy(false);
  };

  const handleSignOut = async () => {
    setAuthBusy(true);
    await resetConversation();
    const { error: signOutError } = await supabase.auth.signOut();
    if (signOutError) {
      setAuthError(signOutError.message);
    }
    setAuthBusy(false);
  };

  const startPromptRun = async (trimmedPrompt: string) => {
    const userMessage: ChatMessage = {
      id: createConversationId(),
      sender: "user",
      content: trimmedPrompt,
    };

    const assistantMessage: ChatMessage = {
      id: createConversationId(),
      sender: "assistant",
      content: "",
    };

    setMessages((prev) => [...prev, userMessage, assistantMessage]);
    setInput("");
    setIsStreaming(true);
    setError(null);
    lastProgressRef.current = "";
    pollFailureCountRef.current = 0;

    const pendingState: PendingMessage = {
      id: assistantMessage.id,
      userId: userMessage.id,
    };
    pendingRef.current = pendingState;

    try {
      const response = await fetch(`${API_BASE}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          conversation_id: conversationId,
          message: trimmedPrompt,
        }),
      });

      if (!response.ok) {
        throw new Error(
          `Request failed with status ${response.status} ${response.statusText}`
        );
      }
      const payload = (await response.json()) as ChatAcceptedResponse;
      const rawInitialProgress = String(payload.latest_progress ?? "").trim();
      const initialProgress = simplifyStatusMessage(rawInitialProgress) || rawInitialProgress;
      if (initialProgress && initialProgress !== lastProgressRef.current) {
        lastProgressRef.current = initialProgress;
        appendProgressMessage(setMessages, assistantMessage.id, initialProgress);
      }
    } catch (err) {
      console.error(err);
      const message = err instanceof Error ? err.message : "Failed to reach the server.";
      setError(message);
      setMessages((prev) =>
        prev.map((message) =>
          message.id === assistantMessage.id
            ? { ...message, content: "There was an error generating a response." }
            : message
        )
      );
      setIsStreaming(false);
      pendingRef.current = null;
    }
  };

  const submitPrompt = async (prompt: string) => {
    const trimmedPrompt = prompt.trim();
    if (!trimmedPrompt) return;

    if (isStreaming) {
      try {
        await storePendingMessage(trimmedPrompt, "queued");
        setInput("");
        setError(null);
      } catch (err) {
        console.error(err);
        const message =
          err instanceof Error ? err.message : "Failed to queue the message.";
        setError(message);
      }
      return;
    }

    await startPromptRun(trimmedPrompt);
  };

  const handleSteer = async () => {
    const message = queuedMessage.trim();
    if (!message || queuedMode !== "queued") {
      return;
    }
    try {
      await storePendingMessage(message, "steer");
      setError(null);
    } catch (err) {
      console.error(err);
      const nextError =
        err instanceof Error ? err.message : "Failed to steer the active run.";
      setError(nextError);
    }
  };

  const handleClearQueued = async () => {
    if (!queuedMessage.trim()) {
      return;
    }
    try {
      await consumePendingMessage();
      setError(null);
    } catch (err) {
      console.error(err);
      const nextError =
        err instanceof Error ? err.message : "Failed to clear the queued message.";
      setError(nextError);
    }
  };

  useEffect(() => {
    if (!conversationId || !isStreaming || !pendingRef.current) {
      return;
    }

    let cancelled = false;
    const assistantMessageId = pendingRef.current.id;

    const pollOnce = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/conversation/${encodeURIComponent(conversationId)}`);
        if (!response.ok) {
          throw new Error(`Failed to poll conversation: ${response.status}`);
        }
        const payload = (await response.json()) as ConversationSnapshotResponse;
        if (cancelled) {
          return;
        }

        syncPendingState(payload);

        pollFailureCountRef.current = 0;
        setError((prev) => (prev === "Connection interrupted. Retrying..." ? null : prev));

        const runStatus = String(payload.run_status ?? "").trim().toLowerCase();
        const rawLatestProgress = String(payload.latest_progress ?? "").trim();
        const latestProgress = simplifyStatusMessage(rawLatestProgress) || rawLatestProgress;
        const latestError = String(payload.latest_error ?? "").trim();

        if (latestProgress && latestProgress !== lastProgressRef.current) {
          lastProgressRef.current = latestProgress;
          appendProgressMessage(setMessages, assistantMessageId, latestProgress);
        }

        if (runStatus === "completed") {
          applyConversationSnapshot(payload, setMessages, assistantMessageId);
          setIsStreaming(false);
          pendingRef.current = null;
          const queuedAfterRun = String(payload.pending_user_message ?? "").trim();
          const queuedModeAfterRun = String(payload.pending_user_mode ?? "").trim().toLowerCase();
          if (queuedAfterRun && queuedModeAfterRun === "queued" && !queuedSubmitRef.current) {
            queuedSubmitRef.current = true;
            try {
              await consumePendingMessage();
              await startPromptRun(queuedAfterRun);
            } catch (queuedError) {
              console.error(queuedError);
              const nextError =
                queuedError instanceof Error
                  ? queuedError.message
                  : "Failed to start the queued message.";
              setError(nextError);
            } finally {
              queuedSubmitRef.current = false;
            }
          }
          return;
        }

        if (runStatus === "failed" || runStatus === "cancelled") {
          setError(latestError || "There was an error generating a response.");
          applyConversationSnapshot(payload, setMessages, assistantMessageId);
          setIsStreaming(false);
          pendingRef.current = null;
        }
      } catch (err) {
        if (cancelled) {
          return;
        }
        console.error(err);
        pollFailureCountRef.current += 1;
        if (pollFailureCountRef.current < MAX_POLL_FAILURES) {
          setError("Connection interrupted. Retrying...");
          return;
        }
        const message =
          err instanceof Error ? err.message : "Failed to reach the server.";
        setError(message);
        setMessages((prev) =>
          prev.map((messageItem) =>
            messageItem.id === assistantMessageId
              ? { ...messageItem, content: "There was an error generating a response." }
              : messageItem
          )
        );
        setIsStreaming(false);
        pendingRef.current = null;
      }
    };

    void pollOnce();
    const timer = window.setInterval(() => {
      void pollOnce();
    }, 1500);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [conversationId, isStreaming]);

  useEffect(() => {
    if (!conversationId || isStreaming || latestExportStatus !== "processing") {
      return;
    }

    let cancelled = false;

    const pollExport = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/conversation/${encodeURIComponent(conversationId)}`);
        if (!response.ok) {
          throw new Error(`Failed to poll export status: ${response.status}`);
        }
        const payload = (await response.json()) as ConversationSnapshotResponse;
        if (cancelled) {
          return;
        }
        syncPendingState(payload);
      } catch (err) {
        if (!cancelled) {
          console.error(err);
        }
      }
    };

    void pollExport();
    const timer = window.setInterval(() => {
      void pollExport();
    }, 1500);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [conversationId, isStreaming, latestExportStatus]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    await submitPrompt(input);
  };

  const handleComposerKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      const form = event.currentTarget.form;
      form?.requestSubmit();
    }
  };

  const handleComposerBeforeInput = (event: FormEvent<HTMLTextAreaElement>) => {
    const nativeEvent = event.nativeEvent as InputEvent | undefined;
    if (nativeEvent?.isComposing) {
      return;
    }
    if (nativeEvent?.inputType === "insertLineBreak") {
      event.preventDefault();
      const form = event.currentTarget.form;
      form?.requestSubmit();
    }
  };

  const lastCompletedAssistantIndex = messages.reduce((lastIndex, message, index) => {
    if (message.sender === "assistant" && message.content.trim()) {
      return index;
    }
    return lastIndex;
  }, -1);

  if (!authReady) {
    return (
      <div className="auth-shell">
        <div className="auth-card">
          <ProductTitle />
          <p>Checking your session.</p>
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <div className="auth-shell">
        <form className="auth-card" onSubmit={handleLogin}>
          <ProductTitle />
          <label className="auth-field">
            <input
              type="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              autoComplete="email"
              placeholder="Email"
              disabled={authBusy}
            />
          </label>
          <label className="auth-field">
            <input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              autoComplete="current-password"
              placeholder="Password"
              disabled={authBusy}
            />
          </label>
          {authError && <div className="auth-error">{authError}</div>}
          <button type="submit" className="auth-submit" disabled={authBusy}>
            {authBusy ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="title-group">
          <ProductTitle />
        </div>
        <div className="header-controls">
          <span className="header-user-label" title={displayName}>
            {displayName}
          </span>
          <div className="header-action">
            <button
              type="button"
              onClick={resetConversation}
              className="header-icon-button"
              aria-label="Start a new conversation"
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9">
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 5v14M5 12h14" />
              </svg>
            </button>
            <span className="header-tooltip" role="tooltip">
              Start a new conversation
            </span>
          </div>
          <div className="header-action">
            <button
              type="button"
              className="header-icon-button sign-out-icon-button"
              onClick={handleSignOut}
              disabled={authBusy}
              aria-label="Sign out"
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                <path strokeLinecap="round" strokeLinejoin="round" d="M10 6H7.75A1.75 1.75 0 0 0 6 7.75v8.5C6 17.216 6.784 18 7.75 18H10" />
                <path strokeLinecap="round" strokeLinejoin="round" d="M13 8l5 4-5 4" />
                <path strokeLinecap="round" strokeLinejoin="round" d="M18 12H10" />
              </svg>
            </button>
            <span className="header-tooltip" role="tooltip">
              Sign out
            </span>
          </div>
        </div>
      </header>

      <main ref={scrollRef} className="app-main">
        <section className="chat-panel">
          {messages.length === 0 && (
            <div className="empty-state">
              <div className="empty-state-note">
                <div className="empty-state-note-group">
                  <p>Designed for detailed Australian analysis and global macro comparison.</p>
                  <p aria-hidden="true">&nbsp;</p>
                  <p>Performs best on targeted questions. Break complex queries down.</p>
                </div>
              </div>
              <div className="empty-state-prompts">
                {EXAMPLE_PROMPTS.map((prompt) => (
                  <button
                    key={prompt}
                    type="button"
                    className="empty-state-prompt"
                    onClick={() => void submitPrompt(prompt)}
                  >
                    {prompt}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((message, index) =>
            message.sender === "progress" ? (
              <article key={message.id} className="bubble-row progress">
                <div className={`progress-step${isProgressSubtask(message.content) ? " progress-step-subtask" : ""}`}>
                  <span className="progress-rail" aria-hidden="true">
                    <span className="progress-marker" />
                  </span>
                  <span className="progress-line">{message.content}</span>
                </div>
              </article>
            ) : message.sender === "assistant" ? (
              <article key={message.id} className="bubble-row assistant-text">
                {message.content ? (
                  <div className="assistant-text-block">
                    {renderContentBlocks(message.content)}
                    <div className="assistant-meta-row">
                      {renderRunCost(message.runCost)}
                      {!isStreaming && latestExportUrl && index === lastCompletedAssistantIndex ? (
                        <div className="assistant-export-link">
                          <a href={`${API_BASE}${latestExportUrl}`} target="_blank" rel="noreferrer" aria-label="Download Excel export">
                            <svg viewBox="0 0 24 24" aria-hidden="true">
                              <path fill="currentColor" d="M14 2H7a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7Zm0 1.5L17.5 7H14ZM9.2 10.6h1.6l1.3 2.2 1.3-2.2H15l-2.1 3.2 2.2 3.6h-1.7l-1.4-2.4-1.4 2.4H8.9l2.2-3.6Z"/>
                            </svg>
                          </a>
                        </div>
                      ) : !isStreaming &&
                        latestExportStatus === "processing" &&
                        index === lastCompletedAssistantIndex ? (
                        <div className="assistant-export-pending" aria-live="polite" aria-label="Preparing Excel export">
                          <span className="assistant-export-spinner" aria-hidden="true" />
                        </div>
                      ) : null}
                    </div>
                  </div>
                ) : (
                  <div className="thinking-line" aria-live="polite" aria-label="Thinking">
                    <NisabaLoader />
                  </div>
                )}
              </article>
            ) : (
              <article
                key={message.id}
                className="bubble-row user"
              >
                <div className="bubble">
                  <div className="rich-content">{renderContentBlocks(message.content)}</div>
                </div>
              </article>
            )
          )}

          {error && <div className="error-banner">{error}</div>}
        </section>
      </main>

      <footer className="app-footer">
        <form onSubmit={handleSubmit} className="composer">
          {queuedMessage && (
            <div className="queued-message-banner">
              <div className="queued-message-copy">
                <span className="queued-message-label">
                  {queuedMode === "steer" ? "Steering next" : "Queued"}
                </span>
                <span className="queued-message-text">{queuedMessage}</span>
              </div>
              <div className="queued-message-actions">
                {isStreaming && queuedMode === "queued" ? (
                  <button
                    type="button"
                    className="queued-message-pill"
                    onClick={() => void handleSteer()}
                  >
                    Steer
                  </button>
                ) : queuedMode === "steer" ? (
                  <span className="queued-message-pill queued-message-pill-passive">
                    Steering
                  </span>
                ) : null}
                <button
                  type="button"
                  className="queued-message-clear"
                  onClick={() => void handleClearQueued()}
                  aria-label="Delete queued message"
                >
                  ×
                </button>
              </div>
            </div>
          )}
          <div className="composer-input-shell">
            <textarea
              ref={composerRef}
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={handleComposerKeyDown}
              onBeforeInput={handleComposerBeforeInput}
              placeholder="Ask Nisaba your economic questions..."
              enterKeyHint="send"
              rows={1}
            />
            <button
              type="submit"
              disabled={!input.trim()}
              className="icon-send-button"
              aria-label={isStreaming ? "Queue message" : "Send message"}
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path
                  d="M3.4 20.6 21 12 3.4 3.4l2.8 6.8 8 1.8-8 1.8-2.8 6.8Z"
                  fill="currentColor"
                />
              </svg>
            </button>
          </div>
        </form>
      </footer>
    </div>
  );
}

export default App;
