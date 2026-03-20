# Nisaba

Nisaba is an AI data harness built to become the best expert system for Australian public data.

Its purpose is to help people ask detailed questions about Australia, retrieve the right public data, run the numbers, and explain what the data actually says.

Nisaba started with ABS at the core, and ABS remains central. But the project is no longer ABS-only. The direction now is broader: combine ABS with other useful public Australian data sources over time, so the harness becomes more nuanced, more detailed, and more useful than any single-source Australian data assistant.

Global macro sources such as the OECD, World Bank, IMF, and UN Comtrade are included for comparison and context. They matter, but they are not the point. The point is depth on Australia.

Current source summary:

| Route | Provider | Datasets |
| --- | --- | ---: |
| Domestic | ABS | 1,221 |
| Domestic | DCCEEW | 1 |
| Domestic | RBA | 71 |
| Macro | OECD | 1,464 |
| Macro | World Bank | 28,377 |
| Macro | IMF | 132 |
| Macro | UN Comtrade | 1 |

## Why This Exists

MCP servers plugged into agentic frameworks like Claude Code or Codex are genuinely powerful. If you are technical, willing to wire tools together, comfortable with API keys, and happy to prompt an agent directly, you can do very sophisticated work that way.

Most people are not interested in doing that. Most people do not want to choose models, manage API keys, understand MCP, or hand-build a retrieval workflow just to answer an economic question.

Nisaba is meant to fill that gap.

Log in. Ask a question. Get a grounded answer.

That is the product goal.

We will host a version of Nisaba for people who want that simplicity. That hosted version is not free: you pay per use. The pricing model is straightforward. We pass through the underlying AI cost and add a 10% margin to cover hosting and maintenance. This is just the practical cost of running a hosted webapp.

If you do not want to pay, download the repo and run it locally with your own API.

## Product Direction

Nisaba is being built as an expert Australian data harness with deep coverage and growing support for Australian publically available data sources.

We want Nisaba to become the strongest integrated AI harness for Australian data in the world. We believe only open source can achieve this.

If you find Nisaba useful, want a new source integrated, or want to improve the harness, submit a pull request.

## What It Does

Under the hood, Nisaba:

1. routes a question to the right retrieval path
2. shortlists candidate datasets or indicators
3. retrieves structured data from the selected source
4. inspects and narrows the returned data
5. runs calculations in a Python sandbox where needed
6. returns grounded answers with charts, tables, and source references

Produced by [Dottie AI Studio](https://dottieaistudio.com.au/).

## Requirements

- Python
- Node.js + npm
- `OPENAI_API_KEY` in `.env`

Example `.env`:

```env
OPENAI_API_KEY=your_key_here
COMTRADE_API_KEY=optional_key_here
```

Refresh the local UN Comtrade metadata bundle when needed:

```bash
python3 scripts/build_comtrade_metadata.py
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
