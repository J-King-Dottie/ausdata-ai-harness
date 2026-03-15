# Nisaba

Nisaba is an agentic economic analysis harness for the ABS API.

Rather than exposing ABS through MCP and hoping the model invents valid queries, Nisaba wraps that substrate with a planning loop, curated retrieval layer, Python analysis sandbox, and frontend so it behaves more like a real economic analyst.

The curated layer is the key reliability move. It keeps the runtime surface small, verified, and semantically clear, so the model can spend its effort on economic reasoning and analysis instead of guessing ABS dimensions, code combinations, and fragile API calls.

It is grounded in:
- [CURATED_ABS_CATALOG.txt]
- [CURATED_ABS_STRUCTURES.txt]

These files encode tested dataset descriptions, known-working query templates, and guidance about what is literally available in the returned ABS data.

In practice, Nisaba can retrieve curated ABS data, compare datasets, calculate derived metrics, handle matrix-style tables, and generate charts and tables when they help explain the answer.

Nisaba is built on top of the ABS MCP server provided by [`mcp-server-abs`](https://github.com/seansoreilly/mcp-server-abs). That server is an important foundation: it provides the underlying ABS access layer this harness depends on. Nisaba is the agentic product built around that substrate, not a replacement for it.

The stack includes:
- curated ABS retrieval logic
- web-search support for broader context when needed
- one Python sandbox tool
- React frontend
- FastAPI backend

Produced by [Dottie AI Studio](https://dottieaistudio.com.au/) · Built on top of [mcp-server-abs](https://github.com/seansoreilly/mcp-server-abs).

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
- backend auto-reload on `http://127.0.0.1:8000`
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
- [run.py](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/run.py): combined single-server entrypoint, not the normal local workflow

## Notes

- Dev mode uses Vite on port `3000` and proxies API calls to backend port `8000`.
- `start-dev.ps1` is the default local command.
- `start-demo.ps1` now forwards to `start-dev.ps1` so the local entrypoint stays consistent.
- If PowerShell blocks scripts, run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```
