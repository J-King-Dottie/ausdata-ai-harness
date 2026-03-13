from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel


BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)


class Settings(BaseModel):
    openai_api_key: str
    openai_model: str = "gpt-5.4"
    openai_reasoning_effort: str = "low"
    node_binary: str = "node"
    python_binary: str = "python3"
    mcp_bridge_path: Path = BASE_DIR / "build" / "mcpBridge.js"
    runtime_dir: Path = BASE_DIR / "runtime"
    max_loops: int = 15
    openai_timeout_seconds: int = 90
    openai_max_output_tokens: int = 2400


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Create a .env file with OPENAI_API_KEY=..."
        )

    overrides = {}

    openai_model = os.getenv("OPENAI_MODEL")
    if openai_model:
        overrides["openai_model"] = openai_model

    openai_reasoning_effort = os.getenv("OPENAI_REASONING_EFFORT")
    if openai_reasoning_effort:
        overrides["openai_reasoning_effort"] = openai_reasoning_effort

    node_binary = os.getenv("NODE_BINARY")
    if node_binary:
        overrides["node_binary"] = node_binary

    python_binary = os.getenv("PYTHON_BINARY")
    if python_binary:
        overrides["python_binary"] = python_binary

    max_loops = os.getenv("MAX_LOOPS")
    if max_loops:
        overrides["max_loops"] = int(max_loops)

    timeout = os.getenv("OPENAI_TIMEOUT_SECONDS")
    if timeout:
        overrides["openai_timeout_seconds"] = int(timeout)

    max_output_tokens = os.getenv("OPENAI_MAX_OUTPUT_TOKENS")
    if max_output_tokens:
        overrides["openai_max_output_tokens"] = int(max_output_tokens)

    return Settings(openai_api_key=api_key, **overrides)
