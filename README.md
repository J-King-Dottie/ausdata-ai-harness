# Nisaba

Nisaba is an agentic economic analysis harness for the ABS API.

It is built around a simple production constraint: the agent does not roam the full ABS API blindly at runtime. Instead, it works from a curated, verified layer of datasets and query templates so retrieval stays reliable and analysis stays grounded.

Rather than exposing ABS through MCP and hoping the model invents valid queries, Nisaba wraps that server with a planning loop, curated retrieval layer, Python analysis sandbox, and frontend so it behaves more like a real economic analyst.

In practice, Nisaba can retrieve curated ABS data, compare datasets, calculate derived metrics, handle matrix-style tables, and generate charts and tables when they help explain the answer.

The raw MCP-style ABS server is still here, but it is the substrate rather than the product.

The stack includes:
- curated ABS retrieval logic
- web-search support for broader context when needed
- one Python sandbox tool
- React frontend
- FastAPI backend

Produced by [Dottie AI Studio](https://dottieaistudio.com.au/) · Powered by [mcp-server-abs](https://github.com/seansoreilly/mcp-server-abs).

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

Frontend hot reload on `http://localhost:3000`  
Backend auto-reload on `http://127.0.0.1:8000`

First run:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-dev.ps1
```

Later runs:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-dev.ps1 -SkipInstall
```

## Local Demo

Single built app served from one backend on `http://localhost:3000`

First run:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-demo.ps1
```

Later runs without reinstall/rebuild:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-demo.ps1 -SkipInstall -SkipBuild
```

## Files

- [start-dev.ps1](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/start-dev.ps1): local dev with hot reload
- [start-demo.ps1](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/start-demo.ps1): built local demo
- [run.py](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/run.py): Python entrypoint used by the demo flow

## Core idea

The harness prefers the curated layer first:

- [CURATED_ABS_CATALOG.txt](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_CATALOG.txt)
- [CURATED_ABS_STRUCTURES.txt](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_STRUCTURES.txt)

These files encode tested dataset descriptions, query templates, and guidance about what is literally available in the returned ABS data.

That curated layer is what makes the system dependable. In agentic production systems, this is a known grounding pattern: keep the runtime surface small, verified, and semantically clear. Instead of asking the model to infer arbitrary ABS dimensions and codes, Nisaba starts from validated patterns and then uses sandbox analysis to narrow, combine, and interpret the results.

The alternative vision, where the agent ranges freely across the full ABS API, is possible in principle but much less reliable in practice. It needs broader discovery, more structure inspection, more recovery loops, and still has a lower reliability ceiling. Nisaba takes the pragmatic path: do the hard discovery work offline, encode it in the curated layer, and let the runtime agent stay fast, cautious, and dependable.

## Notes

- Dev mode uses Vite on port `3000` and proxies API calls to backend port `8000`.
- Demo mode builds the frontend and serves it directly from FastAPI on port `3000`.
- If PowerShell blocks scripts, run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```
