# Nisaba

Nisaba is an AI economic analyst. Ask it a question about the Australian economy or global macro data and it will find the right data, run the numbers, and explain what it found.

MCP servers and Claude/Codex agentic frameworks are genuinely powerful — but they are still aimed at developer types. Most people are not going to configure API keys, set up MCP servers, and run agentic pipelines from a terminal.

Nisaba exists to make these tools accessible to everyone else.

The backend is open source — run it locally if you want. We are also hosting a public version on AWS where anyone can log in and use it on a pay-per-use basis.

Pricing is API cost plus a small margin to cover hosting. That's it.

The goal is accessibility. Nothing more.

## What Nisaba Does

- answers economic questions in plain English
- identifies the most appropriate dataset or indicator for the question
- retrieves, inspects, and narrows the data before drawing conclusions
- runs calculations and comparisons in a Python sandbox
- produces grounded answers with charts, tables, and source references

## Underlying Tooling

This repo builds a public-facing harness on top of underlying MCP and retrieval work. Credit is due to:

- [`mcp-server-abs`](https://github.com/seansoreilly/mcp-server-abs)
- [`openecon-data`](https://github.com/hanlulong/openecon-data)

Nisaba uses ABS retrieval built on top of `mcp-server-abs`, and its broader macro flow is inspired by the retrieval, catalog, and routing ideas pioneered in `openecon-data`.

## Retrieval Model

Nisaba currently has two high-level retrieval paths:

- `abs`
  - Australian Bureau of Statistics data
  - shortlist, inspect metadata, retrieve, analyze
- `macro`
  - international macro data across sources like the OECD, World Bank, and IMF
  - shortlist indicators, retrieve structured series, analyze in sandbox

## Stack

- ABS retrieval path
- macro retrieval path for OECD / World Bank / IMF
- web-search support for broader context when needed
- Python sandbox for inspect, narrow, calculate, compare, and chart-prep
- React frontend
- FastAPI backend

Produced by [Dottie AI Studio](https://dottieaistudio.com.au/).

## Requirements
- Python
- Node.js + npm
- `OPENAI_API_KEY` in `.env`

Example `.env`:
```env
OPENAI_API_KEY=your_key_here
```

## Local Dev

- frontend dev server with HMR on `http://127.0.0.1:3000`
- backend auto-reload on `http://127.0.0.1:8000`

First run:
```powershell
cd path\to\abs-mcp; .\start-dev.ps1
```

Later runs:
```powershell
cd path\to\abs-mcp; .\start-dev.ps1 -SkipInstall
```

Built out of genuine curiosity. If you find it useful or have ideas, open an issue or get in touch.
