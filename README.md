# ABS Economic Analyst

Minimal local ABS analyst app with:

- GPT-5.4-driven harness loop
- one ABS data tool
- one Python sandbox tool
- React frontend
- FastAPI backend

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

## Notes

- Dev mode uses Vite on port `3000` and proxies API calls to backend port `8000`.
- Demo mode builds the frontend and serves it directly from FastAPI on port `3000`.
- If PowerShell blocks scripts, run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```
