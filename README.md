# Nisaba

Nisaba is an AI economic analyst. Ask it a question about the Australian economy or global macro data and it will find the right data, run the numbers, and explain what it found.

MCP servers plugged into agentic frameworks like Claude Code or Codex are genuinely powerful, probably the most capable tools available right now. But they still require a level of technical knowhow that puts them out of reach for most people.

Most people don't have an AI API key. Most don't know which MCP servers to use, or what an MCP server even is. Most can't write a sophisticated AI prompt. That's not a criticism — it's just the reality.

The misison for building Nisaba is to make the power of these tools more accessible to the average person. Log-in, ask a simple question, thats it.

It's designed for detailed Australian analysis with global macro context layered in where useful. I am Aussie and have worked as an economic analyst for federal gov for the last 10 years, hense the foucus.

Under the hood, Nisaba answers economic questions by identifying the right dataset for the job, retrieving and inspecting the data, running calculations in a Python sandbox, and producing grounded answers with charts, tables, and source references.

The backend is open source — run it locally if you want. 

The main output is a hosted version on AWS, available on a pay-per-use basis, priced at the raw API costs plus a small margin to cover hosting. That's it.

The goal is accessibility. Nothing more.

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
- backend on `http://127.0.0.1:5000`

Run from the repo root in PowerShell:

First run:
```powershell
.\start-dev.ps1
```

Later runs:
```powershell
.\start-dev.ps1 -SkipInstall
```

If you want backend auto-reload:
```powershell
.\start-dev.ps1 -SkipInstall -Reload
```

If the frontend fails with `'vite' is not recognized`, rebuild the frontend install and start dev again:
```powershell
Remove-Item -Recurse -Force .\frontend\node_modules; npm install --prefix .\frontend; .\start-dev.ps1
```

Built out of genuine curiosity. If you find it useful or have ideas, open an issue or get in touch.
