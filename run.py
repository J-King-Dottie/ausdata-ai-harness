from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def resolve_executable(name: str) -> str:
    candidates = [name]
    if os.name == "nt":
        stem = Path(name).name.lower()
        if "." not in stem:
            candidates = [f"{name}.cmd", f"{name}.exe", name]

        common_windows_paths = {
            "npm": [
                r"C:\Program Files\nodejs\npm.cmd",
                r"C:\Program Files (x86)\nodejs\npm.cmd",
            ],
            "node": [
                r"C:\Program Files\nodejs\node.exe",
                r"C:\Program Files (x86)\nodejs\node.exe",
            ],
        }
        for key, paths in common_windows_paths.items():
            if stem == key or stem == f"{key}.cmd" or stem == f"{key}.exe":
                candidates.extend(paths)

    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
        path_candidate = Path(candidate)
        if path_candidate.exists():
            return str(path_candidate)

    raise FileNotFoundError(
        f"Could not find executable '{name}'. "
        f"Make sure it is installed and available on PATH."
    )


def run_command(args: list[str], *, cwd: Path) -> None:
    resolved_args = list(args)
    resolved_args[0] = resolve_executable(args[0])
    completed = subprocess.run(resolved_args, cwd=str(cwd))
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build and run the ABS analyst harness as a single deploy entrypoint."
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Start the app without rebuilding the MCP server and frontend.",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip installing Node and Python dependencies before build.",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("HOST", "127.0.0.1"),
        help="Host for the FastAPI server.",
    )
    parser.add_argument(
        "--port",
        default=int(os.getenv("PORT", "3000")),
        type=int,
        help="Port for the FastAPI server.",
    )
    args = parser.parse_args()

    if not args.skip_install:
        run_command(["npm", "install"], cwd=ROOT)
        run_command(["npm", "install"], cwd=ROOT / "frontend")
        run_command(
            [sys.executable, "-m", "pip", "install", "-r", "backend/requirements.txt"],
            cwd=ROOT,
        )

    if not args.skip_build:
        run_command(["npm", "run", "build"], cwd=ROOT)
        run_command(["npm", "run", "build"], cwd=ROOT / "frontend")

    run_command(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "backend.app.main:app",
            "--host",
            args.host,
            "--port",
            str(args.port),
        ],
        cwd=ROOT,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
