import { Fragment, type FormEvent, type KeyboardEvent, type ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import type { Session } from "@supabase/supabase-js";
import ReactECharts from "echarts-for-react";
import "./index.css";
import { supabase } from "./supabaseClient";

type Sender = "user" | "assistant" | "progress";

interface ChatMessage {
  id: string;
  sender: Sender;
  content: string;
}

interface PendingMessage {
  id: string;
  abortController: AbortController;
}

interface ConversationSnapshotResponse {
  conversation_id?: unknown;
  messages?: unknown;
}

const API_BASE = "";
const STORAGE_KEY = "abs-analyst-session";
const EXAMPLE_PROMPTS = [
  "What data do you have access to?",
  "What are the drivers of a decline in Manufacturing in Australia?",
  "How many jobs are there in Australia's gas intensive sectors?",
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

interface ChartSeries {
  name: string;
  color?: string;
  points: ChartPoint[];
}

interface ChartSpec {
  type?: "line" | "bar";
  title?: string;
  subtitle?: string;
  xLabel?: string;
  yLabel?: string;
  series: ChartSeries[];
}

type ContentBlock =
  | { type: "paragraph"; lines: string[] }
  | { type: "list"; items: string[] }
  | { type: "table"; headers: string[]; rows: string[][] }
  | { type: "code"; code: string; language: string }
  | { type: "chart"; spec: ChartSpec };

function renderInlineMarkdown(value: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  const pattern = /(`[^`]+`|\*\*[^*]+\*\*)/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null = null;
  let key = 0;

  while ((match = pattern.exec(value)) !== null) {
    if (match.index > lastIndex) {
      nodes.push(value.slice(lastIndex, match.index));
    }
    const token = match[0];
    if (token.startsWith("`")) {
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
      .filter((entry): entry is ChartSeries => entry !== null);

    if (!series.length) {
      return null;
    }

    return {
      type: parsed.type === "bar" ? "bar" : "line",
      title: typeof parsed.title === "string" ? parsed.title : undefined,
      subtitle: typeof parsed.subtitle === "string" ? parsed.subtitle : undefined,
      xLabel: typeof parsed.xLabel === "string" ? parsed.xLabel : undefined,
      yLabel: typeof parsed.yLabel === "string" ? parsed.yLabel : undefined,
      series,
    };
  } catch {
    return null;
  }
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
      if (language === "chart") {
        const spec = parseChartBlock(raw);
        if (spec) {
          blocks.push({ type: "chart", spec });
        } else {
          blocks.push({ type: "code", code: raw, language });
        }
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

    const paragraphLines: string[] = [];
    while (index < lines.length) {
      const candidate = lines[index];
      const candidateTrimmed = candidate.trim();
      if (!candidateTrimmed) {
        break;
      }
      if (candidateTrimmed.startsWith("```") || candidateTrimmed.startsWith("- ")) {
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

function buildNiceTicks(minValue: number, maxValue: number, count = 4) {
  if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return [0];
  }
  if (minValue === maxValue) {
    return [minValue];
  }

  const range = maxValue - minValue;
  const roughStep = range / Math.max(count - 1, 1);
  const magnitude = 10 ** Math.floor(Math.log10(Math.abs(roughStep) || 1));
  const normalized = roughStep / magnitude;

  let niceStep = magnitude;
  if (normalized <= 1) {
    niceStep = magnitude;
  } else if (normalized <= 2) {
    niceStep = 2 * magnitude;
  } else if (normalized <= 5) {
    niceStep = 5 * magnitude;
  } else {
    niceStep = 10 * magnitude;
  }

  const niceMin = Math.floor(minValue / niceStep) * niceStep;
  const niceMax = Math.ceil(maxValue / niceStep) * niceStep;
  const ticks: number[] = [];

  for (let tick = niceMin; tick <= niceMax + niceStep * 0.5; tick += niceStep) {
    ticks.push(Number(tick.toFixed(10)));
  }

  return ticks.reverse();
}

function ChartBlock({ spec }: { spec: ChartSpec }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [containerWidth, setContainerWidth] = useState(0);
  const colors = ["#234233", "#8f6a3a", "#54745f", "#b45f3a"];
  const allPoints = spec.series.flatMap((series) => series.points);
  const xValues = Array.from(new Set(allPoints.map((point) => point.x)));
  const longestXAxisLabelLength = xValues.reduce((max, value) => Math.max(max, value.length), 0);
  const yValues = allPoints.map((point) => point.y);
  const longestSeries = Math.max(...spec.series.map((series) => series.points.length), 0);
  const isNarrow = containerWidth > 0 && containerWidth < 640;
  const useHorizontalBars =
    spec.type === "bar" && (isNarrow || xValues.length > 10 || longestXAxisLabelLength > 16);
  const rotateVerticalLabels =
    spec.type === "bar" && !useHorizontalBars && (xValues.length > 7 || longestXAxisLabelLength > 12);
  const chartHeight =
    spec.type === "bar" && useHorizontalBars
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
          top: spec.title ? 20 : 8,
          right: 20,
          bottom: spec.xLabel ? 52 : 22,
          left: Math.min(220, Math.max(110, longestXAxisLabelLength * 7)),
          containLabel: false,
        }
      : {
          top: spec.title ? 20 : 8,
          right: 20,
          bottom: rotateVerticalLabels ? 92 : spec.xLabel ? 54 : 30,
          left: spec.yLabel ? 72 : 58,
          containLabel: false,
        },
    tooltip: {
      trigger: "axis",
      confine: true,
      backgroundColor: "rgba(245, 240, 227, 0.96)",
      borderColor: "rgba(30, 43, 33, 0.12)",
      borderWidth: 1,
      textStyle: {
        color: "#1e2b21",
      },
      axisPointer: {
        type: spec.type === "bar" ? "shadow" : "line",
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
            bottom: 0,
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
          type: "category",
          data: xValues,
          name: spec.xLabel,
          nameLocation: "middle",
          nameGap: rotateVerticalLabels ? 78 : spec.xLabel ? 34 : 0,
          axisLabel: {
            color: "rgba(30, 43, 33, 0.58)",
            fontSize: 11,
            interval: spec.type === "bar" ? 0 : "auto",
            hideOverlap: true,
            rotate: rotateVerticalLabels ? -40 : 0,
            width: rotateVerticalLabels ? 96 : 88,
            overflow: "truncate",
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
    series: spec.series.map((series, index) => {
      const data = xValues.map((xValue) => {
        const match = series.points.find((point) => point.x === xValue);
        return match ? match.y : null;
      });
      return {
        name: series.name,
        type: spec.type === "bar" ? "bar" : "line",
        data,
        barMaxWidth: 28,
        barCategoryGap: spec.series.length > 1 ? "34%" : "42%",
        smooth: spec.type === "line" && spec.series.length === 1 ? 0.15 : 0,
        showSymbol: spec.type === "line" && longestSeries <= 16 && spec.series.length <= 2,
        symbolSize: 6,
        lineStyle: {
          width: spec.series.length > 1 ? 2.4 : 2.8,
        },
        itemStyle: {
          borderRadius: spec.type === "bar" ? [4, 4, 0, 0] : 0,
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
      {spec.subtitle && <p className="chart-subtitle">{spec.subtitle}</p>}
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
          className="nisaba-stroke nisaba-stroke-1"
          x="10"
          y="8"
          width="60"
          height="44"
          rx="12"
          pathLength={1}
        />

        <g className="nisaba-wedge nisaba-stroke-2">
          <line className="nisaba-wedge-line" x1="22" y1="22" x2="33" y2="18" pathLength={1} />
          <path className="nisaba-wedge-press" d="M18 23 L22 20 L22 26 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-3">
          <line className="nisaba-wedge-line" x1="38" y1="22" x2="50" y2="18" pathLength={1} />
          <path className="nisaba-wedge-press" d="M34 23 L38 20 L38 26 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-4">
          <line className="nisaba-wedge-line" x1="54" y1="22" x2="64" y2="18" pathLength={1} />
          <path className="nisaba-wedge-press" d="M50 23 L54 20 L54 26 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-5">
          <line className="nisaba-wedge-line" x1="24" y1="32" x2="36" y2="28" pathLength={1} />
          <path className="nisaba-wedge-press" d="M20 33 L24 30 L24 36 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-6">
          <line className="nisaba-wedge-line" x1="40" y1="32" x2="52" y2="28" pathLength={1} />
          <path className="nisaba-wedge-press" d="M36 33 L40 30 L40 36 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-7">
          <line className="nisaba-wedge-line" x1="56" y1="32" x2="66" y2="28" pathLength={1} />
          <path className="nisaba-wedge-press" d="M52 33 L56 30 L56 36 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-8">
          <line className="nisaba-wedge-line" x1="30" y1="42" x2="42" y2="38" pathLength={1} />
          <path className="nisaba-wedge-press" d="M26 43 L30 40 L30 46 Z" />
        </g>
        <g className="nisaba-wedge nisaba-stroke-9">
          <line className="nisaba-wedge-line" x1="46" y1="42" x2="58" y2="38" pathLength={1} />
          <path className="nisaba-wedge-press" d="M42 43 L46 40 L46 46 Z" />
        </g>
      </g>
    </svg>
  );
}

function renderContentBlocks(value: string) {
  const blocks = parseContentBlocks(value);
  return blocks.map((block, index) => {
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
  if (/^Loading the curated ABS dataset catalog\.?$/i.test(normalized)) {
    return "Checking the curated ABS datasets.";
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
  if (/^Adding `[^`]+` to the curated ABS files/i.test(normalized)) {
    return "Updating the curated ABS definitions.";
  }
  if (/^Tool execution failed\./i.test(normalized)) {
    return "Adjusting the approach after a failed step.";
  }
  if (/^This curated template must be used as-is\./i.test(normalized)) {
    return "Using the curated ABS template directly.";
  }
  if (/^ABS returned no data for that curated template call\.?$/i.test(normalized)) {
    return "That ABS retrieval returned no data. Trying a different path.";
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
    return { conversationId, messages };
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
    const typed = message as { role?: unknown; content?: unknown };
    const role = typeof typed.role === "string" ? typed.role.trim().toLowerCase() : "";
    const content = typeof typed.content === "string" ? typed.content : "";
    if (!content.trim()) {
      return [];
    }
    if (role !== "user" && role !== "assistant") {
      return [];
    }
    return [
      {
        id: createConversationId(),
        sender: role as Sender,
        content,
      } satisfies ChatMessage,
    ];
  });
}

function ProductTitle() {
  return (
    <div className="product-title">
      <div className="product-title-text">
        <div className="product-title-main">
          <h1>Ni-SA-ba</h1>
        </div>
        <div className="product-subtitle-group">
          <span>an AI economic analyst</span>
          <div className="header-action info-action">
            <button
              type="button"
              className="header-icon-button"
              aria-label="About this tool"
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <circle cx="12" cy="12" r="10" fill="currentColor" />
                <rect x="11" y="10" width="2" height="7" rx="1" fill="#f7f3ea" />
                <circle cx="12" cy="7.25" r="1.2" fill="#f7f3ea" />
              </svg>
            </button>
            <div className="header-tooltip info-tooltip" role="tooltip">
              <p>
                In Sumerian mythology, Nisaba was the goddess of writing,
                accounting, and administrative record-keeping. Scribe of the gods,
                keeper of the ledger. One of the oldest named deities in human history.
              </p>
              <p>
                She tracked what came in, what went out, and what was owed. Writing
                itself emerged in Mesopotamian bureaucracies to count what mattered -
                grain rations, livestock, labour allocations, tax obligations.
              </p>
              <p>
                Your Nisaba does the same. She has command of the full ABS economic dataset.
                Ask her anything about the Australian economy and she'll pull the numbers,
                run the calculations, and show you exactly what they say.
              </p>
              <p>
                Produced by{" "}
                <a href="https://dottieaistudio.com.au/" target="_blank" rel="noreferrer">
                  Dottie AI Studio
                </a>
                {" "}· Powered by{" "}
                <a
                  href="https://github.com/seansoreilly/mcp-server-abs"
                  target="_blank"
                  rel="noreferrer"
                >
                  mcp-server-abs
                </a>
                .
              </p>
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
  const [pending, setPending] = useState<PendingMessage | null>(null);
  const scrollRef = useRef<HTMLElement | null>(null);
  const pendingRef = useRef<PendingMessage | null>(null);
  const lastProgressRef = useRef("");
  const composerRef = useRef<HTMLTextAreaElement | null>(null);
  const hydratedConversationRef = useRef("");

  const syncComposerHeight = () => {
    const element = composerRef.current;
    if (!element) {
      return;
    }
    element.style.height = "0px";
    element.style.height = `${element.scrollHeight}px`;
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isStreaming]);

  useEffect(() => {
    syncComposerHeight();
  }, [input]);

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
    window.sessionStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        conversationId,
        messages,
      })
    );
  }, [conversationId, messages]);

  useEffect(() => {
    if (!authReady || !session || !conversationId) {
      return;
    }
    if (hydratedConversationRef.current === conversationId) {
      return;
    }

    let active = true;
    hydratedConversationRef.current = conversationId;

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
        const restoredMessages = mapBackendMessages(payload.messages);
        if (restoredMessages.length > 0) {
          setMessages(restoredMessages);
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
    if (pending) {
      pending.abortController.abort();
    }

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
    setPending(null);
    pendingRef.current = null;
    lastProgressRef.current = "";
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

  const submitPrompt = async (prompt: string) => {
    const trimmedPrompt = prompt.trim();
    if (!trimmedPrompt || isStreaming) return;

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

    const abortController = new AbortController();
    const pendingState: PendingMessage = {
      id: assistantMessage.id,
      abortController,
    };
    setPending(pendingState);
    pendingRef.current = pendingState;

    try {
      const response = await fetch(`${API_BASE}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          conversation_id: conversationId,
          message: trimmedPrompt,
        }),
        signal: abortController.signal,
      });

      if (!response.ok || !response.body) {
        throw new Error(
          `Request failed with status ${response.status} ${response.statusText}`
        );
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let assistantContent = "";
      let streamComplete = false;

      while (!streamComplete) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        let newlineIndex = buffer.indexOf("\n");
        while (newlineIndex !== -1) {
          const line = buffer.slice(0, newlineIndex).trim();
          buffer = buffer.slice(newlineIndex + 1);

          if (line) {
            let payload: { type?: string; message?: string; chunk?: string } | null = null;
            try {
              payload = JSON.parse(line);
            } catch {
              payload = null;
            }

            if (payload?.type === "status" && typeof payload.message === "string") {
              const progressContent = simplifyStatusMessage(payload.message);
              if (progressContent && progressContent !== lastProgressRef.current) {
                lastProgressRef.current = progressContent;
                const progressMessage: ChatMessage = {
                  id: createConversationId(),
                  sender: "progress",
                  content: progressContent,
                };
                setMessages((prev) => {
                  const next = [...prev];
                  const assistantIndex = next.findIndex((msg) => msg.id === assistantMessage.id);
                  const insertionIndex = assistantIndex === -1 ? next.length : assistantIndex;
                  next.splice(insertionIndex, 0, progressMessage);
                  return next;
                });
              }
            } else if (payload?.type === "final" && typeof payload.chunk === "string") {
              assistantContent += payload.chunk;
              const snapshot = assistantContent;
              setMessages((prev) =>
                prev.map((message) =>
                  message.id === assistantMessage.id
                    ? { ...message, content: snapshot }
                    : message
                )
              );
            } else if (payload?.type === "error") {
              const message =
                typeof payload.message === "string"
                  ? payload.message
                  : "The assistant could not finish generating a response.";
              setError(message);
              setMessages((prev) =>
                prev.map((message) =>
                  message.id === assistantMessage.id
                    ? { ...message, content: "There was an error generating a response." }
                    : message
                )
              );
              streamComplete = true;
              break;
            } else if (payload?.type === "done") {
              streamComplete = true;
              break;
            }
          }

          newlineIndex = buffer.indexOf("\n");
        }
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
    } finally {
      setIsStreaming(false);
      setPending(null);
      pendingRef.current = null;
    }
  };

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
          <div className="user-chip">{session.user.email}</div>
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
          )}

          {messages.map((message) =>
            message.sender === "progress" ? (
              <article key={message.id} className="bubble-row progress">
                <div className="progress-step">
                  <span className="progress-rail" aria-hidden="true">
                    <span className="progress-marker" />
                  </span>
                  <span className="progress-line">{message.content}</span>
                </div>
              </article>
            ) : message.sender === "assistant" ? (
              <article key={message.id} className="bubble-row assistant-text">
                {message.content ? (
                  <div className="assistant-text-block">{renderContentBlocks(message.content)}</div>
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
          <div className="composer-input-shell">
            <textarea
              ref={composerRef}
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={handleComposerKeyDown}
              placeholder="Ask an ABS economic question..."
              rows={1}
              disabled={isStreaming}
            />
            <button
              type="submit"
              disabled={isStreaming || !input.trim()}
              className="icon-send-button"
              aria-label={isStreaming ? "Sending message" : "Send message"}
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
