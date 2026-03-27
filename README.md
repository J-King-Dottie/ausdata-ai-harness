## Nisaba

Nisaba is an open source data harness built around two MCP servers: one for domestic Australian data and one for global macro data.

It builds on existing open source projects including [mcp-server-abs](https://github.com/seansoreilly/mcp-server-abs) and [openecon-data](https://github.com/hanlulong/openecon-data). We continue to expand it. 

The goal is to make discovery, retrieval, and analysis across a wide range of public data sources as easy as possible. Over time we want to consolidate that into a single integrated MCP with broad, deep coverage.

If you are technical, clone the repo, add your API key, and run it locally. If you are not, we have built a simple hosted web app on top of the MCP servers — log in, ask a question, get a grounded answer. That version is not free; we pass through the raw AI cost and add 10% to cover hosting. The repo is fully open source either way.

Here is what is currently plugged in:

| Route | Provider | Datasets |
| --- | --- | ---: |
| Domestic | ABS | 1,221 |
| Domestic | DCCEEW | 1 |
| Domestic | RBA | 71 |
| Macro | OECD | 1,464 |
| Macro | World Bank | 28,377 |
| Macro | IMF | 132 |
| Macro | UN Comtrade | 1 |

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

Some catalog and metadata assets are built snapshots and need to be refreshed manually when needed:

- Refresh the macro catalog:

```bash
python3 scripts/build_macro_catalog.py
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

If you want backend auto-reload:

```powershell
.\start-dev.ps1 -SkipInstall -Reload
```
