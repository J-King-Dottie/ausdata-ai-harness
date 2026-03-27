## Nisaba

Nisaba is an open source data harness built around one unified MCP server.

It builds on existing open source projects including [mcp-server-abs](https://github.com/seansoreilly/mcp-server-abs) and [openecon-data](https://github.com/hanlulong/openecon-data). We continue to expand it. 

The goal is to make discovery, retrieval, and analysis across a wide range of public data sources as easy as possible through one catalog and one MCP surface, with source-specific retrieval adapters behind it.

If you are technical, clone the repo, add your API key, and run it locally. If you are not, we have built a simple hosted web app on top of the MCP server. Log in, ask a question, get a grounded answer. That version is not free; we pass through the raw AI cost and add 10% to cover hosting. The repo is fully open source either way.

Here is what is currently plugged into the unified catalog:

| Provider | Datasets |
| --- | ---: |
| ABS | 1,221 |
| DCCEEW | 1 |
| RBA | 71 |
| OECD | 1,464 |
| World Bank | 28,377 |
| IMF | 132 |
| UN Comtrade | 1 |

This product is heavily vibecoded and tested by outcomes rather than code review. It does not work perfectly every time, but it works well most of the time.

We want people to suggest additional integrations so the system can grow into the strongest open source for AI-driven and Australian focused data analysis in the world.

Produced by [Dottie AI Studio](https://dottieaistudio.com.au/).

## Requirements

- Python
- Node.js and npm
- `OPENAI_API_KEY` set in `.env`

Example `.env`:

```env
OPENAI_API_KEY=your_key_here
```

## Direct MCP Use

The repo can be used directly as MCP, not just through the hosted app.

- Unified MCP server: `python -m backend.app.unified_mcp_server`

If your local MCP client supports a project-scoped `.mcp.json`, the repo includes one at the root with the unified server already defined.

Some catalog and metadata assets are built snapshots and need to be refreshed manually when needed:

- Refresh the unified catalog and FTS index:

```bash
python3 scripts/build_unified_catalog.py
```

- Refresh the local UN Comtrade metadata bundle:

```bash
python3 scripts/build_comtrade_metadata.py
```

## Local Dev

- Frontend dev server with HMR: `http://127.0.0.1:3000`
- Backend: `http://127.0.0.1:5000`

Run from the repo root in PowerShell.

First run:

```powershell
.\start-dev.ps1
```

Later runs:

```powershell
.\start-dev.ps1 -SkipInstall
```

If you see `'vite' is not recognized as an internal or external command`, your `frontend/node_modules` was likely installed from WSL/Linux rather than from Windows. Run:

```powershell
.\start-dev.ps1
```

That reinstalls the frontend dependencies with the Windows `vite.cmd` shim that `npm run dev` expects.

If you want backend auto-reload:

```powershell
.\start-dev.ps1 -SkipInstall -Reload
```
