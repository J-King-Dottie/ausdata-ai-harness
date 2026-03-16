# Nisaba

Nisaba is an agentic economic analysis harness with ABS-first retrieval, a Python analysis sandbox, web-search support, and a native macro retrieval layer for World Bank, IMF, and OECD data.

Rather than exposing raw APIs and hoping the model invents valid calls, Nisaba wraps those substrates with a planning loop, controlled retrieval tools, runtime inspection, narrowing, calculation, and chart/table output so it behaves more like a real economic analyst.

## Data Engines

Nisaba currently packages two retrieval paths inside this repo:

- ABS access built on top of [`mcp-server-abs`](https://github.com/seansoreilly/mcp-server-abs)
- a native macro provider layer for World Bank, IMF, and OECD

This repo is Nisaba's own application. The ABS path builds on top of the upstream MCP, while the macro path is implemented directly in this codebase.

## Runtime Model

- Nisaba remains the primary app and the only public-facing backend surface.
- Nisaba routes:
  - ABS questions to the ABS retrieval path
  - non-ABS macro questions to the native macro provider path
  - mixed questions through both, then reconciles the outputs in sandbox

In final answers, sources should reference the upstream provider data itself, not the middleware layer. For example:

- ABS dataset id and title
- World Bank indicator code and URL
- IMF series code and URL
- OECD dataflow and URL

## ABS Guidance

The ABS curated layer is grounded in:

- [CURATED_ABS_CATALOG.txt](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_CATALOG.txt)
- [CURATED_ABS_STRUCTURES.txt](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_STRUCTURES.txt)

These files encode tested dataset descriptions, known-working query templates, and guidance about what is literally available in the returned ABS data.

## Stack

- ABS retrieval path
- native World Bank / IMF / OECD retrieval path
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
OPENAI_MODEL=gpt-5.4
OPENAI_REASONING_EFFORT=low
MAX_LOOPS=15
```

## Local Dev

This is the normal local startup flow.

- frontend dev server with HMR on `http://127.0.0.1:3000`
- Nisaba backend auto-reload on `http://127.0.0.1:8000`
- one-line command:

First run:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-dev.ps1
```

Later runs:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-dev.ps1 -SkipInstall
```

## Files

- [start-dev.ps1](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/start-dev.ps1): local dev with hot reload
- [start-demo.ps1](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/start-demo.ps1): deprecated compatibility wrapper that now starts local dev
- [run.py](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/run.py): combined entrypoint for build and local start
- [backend/app/macro_data.py](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/backend/app/macro_data.py): native World Bank / IMF / OECD retrieval layer

## Notes

- Dev mode uses Vite on port `3000` and proxies API calls to Nisaba backend port `8000`.
- `start-dev.ps1` is the default local command.
- `start-demo.ps1` now forwards to `start-dev.ps1` so the local entrypoint stays consistent.
- Docker uses one image and one public backend port.
- If PowerShell blocks scripts, run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```
