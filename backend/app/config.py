from __future__ import annotations

import os
import sys
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
    abs_api_base: str = "https://data.api.abs.gov.au"
    worldbank_base_url: str = "https://api.worldbank.org/v2"
    imf_base_url: str = "https://www.imf.org/external/datamapper/api/v1"
    oecd_base_url: str = "https://sdmx.oecd.org/public/rest"
    comtrade_base_url: str = "https://comtradeapi.un.org/data/v1/get"
    comtrade_api_key: str | None = None
    macro_timeout_seconds: int = 120
    node_binary: str = "node"
    python_binary: str = sys.executable
    runtime_dir: Path = BASE_DIR / "runtime"
    max_loops: int = 15
    openai_timeout_seconds: int = 180


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

    worldbank_base_url = os.getenv("WORLDBANK_BASE_URL")
    if worldbank_base_url:
        overrides["worldbank_base_url"] = worldbank_base_url

    abs_api_base = os.getenv("ABS_API_BASE")
    if abs_api_base:
        overrides["abs_api_base"] = abs_api_base

    imf_base_url = os.getenv("IMF_BASE_URL")
    if imf_base_url:
        overrides["imf_base_url"] = imf_base_url

    oecd_base_url = os.getenv("OECD_BASE_URL")
    if oecd_base_url:
        overrides["oecd_base_url"] = oecd_base_url

    comtrade_base_url = os.getenv("COMTRADE_BASE_URL")
    if comtrade_base_url:
        overrides["comtrade_base_url"] = comtrade_base_url

    comtrade_api_key = os.getenv("COMTRADE_API_KEY")
    if comtrade_api_key:
        overrides["comtrade_api_key"] = comtrade_api_key

    macro_timeout = os.getenv("MACRO_TIMEOUT_SECONDS") or os.getenv("MACRO_TIMEOUT")
    if macro_timeout:
        overrides["macro_timeout_seconds"] = int(macro_timeout)

    python_binary = os.getenv("PYTHON_BINARY")
    if python_binary:
        overrides["python_binary"] = python_binary

    timeout = os.getenv("OPENAI_TIMEOUT_SECONDS")
    if timeout:
        overrides["openai_timeout_seconds"] = int(timeout)

    return Settings(openai_api_key=api_key, **overrides)
