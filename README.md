# Nisaba

Nisaba is an AI data harness built to become the best expert system for Australian public data.

Its purpose is to help people ask detailed questions about Australia, retrieve the right public data, run the numbers, and explain what the data actually says.

Nisaba started with ABS at the core, and ABS remains central. But the project is no longer ABS-only. The direction now is broader: combine ABS with other useful public Australian data sources over time, so the harness becomes more nuanced, more detailed, and more useful than any single-source Australian data assistant.

Global macro sources such as the OECD, World Bank, and IMF are included for comparison and context. They matter, but they are not the point. The point is depth on Australia.

## Why This Exists

MCP servers plugged into agentic frameworks like Claude Code or Codex are genuinely powerful. If you are technical, willing to wire tools together, comfortable with API keys, and happy to prompt an agent directly, you can do very sophisticated work that way.

Most people are not interested in doing that. Most people do not want to choose models, manage API keys, understand MCP, or hand-build a retrieval workflow just to answer an economic question.

Nisaba is meant to fill that gap.

Log in. Ask a question. Get a grounded answer.

That is the product goal.

We will host a version of Nisaba for people who want that simplicity. That hosted version is not free: you pay per use. The pricing model is straightforward. We pass through the underlying AI cost and add a 10% margin to cover hosting and maintenance. This is just the practical cost of running a hosted webapp.

If you do not want to pay, download the repo and run it locally with your own API.

## Product Direction

Nisaba is being built as an expert Australian data harness with:

- deep ABS coverage
- growing support for custom Australian public sources
- global macro context where comparison is useful
- transparent retrieval, analysis, and sourcing

Over time, the project should expand to cover a broader and broader range of detailed Australian public data sources.

The long-run ambition is simple:

- make Nisaba the strongest integrated AI harness for Australian data in the world

That means careful source integration, not vague claims of coverage.
Each new source should be useful, grounded, and actually retrievable.

## What It Does

Under the hood, Nisaba:

1. routes a question to the right retrieval path
2. shortlists candidate datasets or indicators
3. retrieves structured data from the selected source
4. inspects and narrows the returned data
5. runs calculations in a Python sandbox where needed
6. returns grounded answers with charts, tables, and source references

Nisaba currently has two high-level retrieval routes:

- `aus`
  - Australian domestic retrieval
  - includes ABS API data and curated custom Australian public sources
- `macro`
  - global macro retrieval
  - includes sources such as OECD, World Bank, and IMF

## Open Source

The backend is open source and can be run locally.

This project is also deliberately open to contribution. If there is another Australian public dataset or source you want available in Nisaba, open an issue or submit a pull request. Useful, working source integrations are welcome.

The standard is straightforward:

- the integration should be real
- the source should be public
- the retrieval path should work


## Underlying Tooling

This repo builds on top of strong open-source retrieval work. Credit is due to:

- [`mcp-server-abs`](https://github.com/seansoreilly/mcp-server-abs)
- [`openecon-data`](https://github.com/hanlulong/openecon-data)

Nisaba uses ABS retrieval built on top of `mcp-server-abs`, and its broader macro flow is informed by the retrieval, catalog, and routing ideas developed in `openecon-data`.

## Stack

- Australian domestic retrieval across ABS and custom Australian public sources
- global macro retrieval for OECD / World Bank / IMF
- web-search support for broader context when needed
- Python sandbox for inspect, narrow, calculate, compare, and chart preparation
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

If you find it useful, want a new Australian source integrated, or want to improve the harness, open an issue or submit a pull request.
