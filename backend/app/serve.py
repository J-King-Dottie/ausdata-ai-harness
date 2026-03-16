from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the ABS analyst backend.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    reload_kwargs = {}
    if args.reload:
        reload_kwargs = {
            "reload_dirs": [str(Path(__file__).resolve().parents[2] / "backend"), str(Path(__file__).resolve().parents[2] / "frontend")],
            "reload_excludes": ["vendor/*", "vendor/**", "runtime/*", "runtime/**", "*.sqlite3"],
        }

    uvicorn.run(
        "backend.app.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_config=None,
        **reload_kwargs,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
