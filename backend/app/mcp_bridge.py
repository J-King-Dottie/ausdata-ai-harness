from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Any, Dict, Optional, Tuple

from .config import get_settings
import httpx
from typing import List


ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
SAFE_ENV_KEYS = {
    "APPDATA",
    "COMSPEC",
    "HOME",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "WINDIR",
}


class MCPBridgeError(RuntimeError):
    """Raised when the MCP bridge script fails."""

    def __init__(
        self,
        message: str,
        *,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr


def _run_bridge(command: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    settings = get_settings()
    bridge_path = settings.mcp_bridge_path
    if not bridge_path.exists():
        raise MCPBridgeError(
            f"MCP bridge executable not found at {bridge_path}. Run `npm run build` first."
        )

    args = [settings.node_binary, str(bridge_path), command]
    if payload is not None:
        args.append(json.dumps(payload))

    env = {
        key: value
        for key, value in os.environ.items()
        if key in SAFE_ENV_KEYS
    }
    env["MCP_BRIDGE_COMPACT"] = "1"

    try:
        completed = subprocess.run(
            args,
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - subprocess failure
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""

        raise MCPBridgeError(
            f"MCP bridge failed for command '{command}'.\nSTDOUT (first 1000 chars): {stdout[:1000]}\nSTDERR (first 1000 chars): {stderr[:1000]}",
            stdout=stdout,
            stderr=stderr,
        ) from exc

    output = completed.stdout.strip()
    if not output:
        return None

    def _strip_prefix(raw: str) -> str:
        for marker in ('{"total"', '{"dataflows"', '{"flows"'):
            idx = raw.find(marker)
            if idx != -1:
                return raw[idx:]

        match = re.search(r'(?m)^[ \t]*[{[]', raw)
        if match:
            return raw[match.start():]
        return raw

    cleaned_output = _strip_prefix(ANSI_ESCAPE_RE.sub("", output)).strip()
    # First attempt a straight JSON parse.
    try:
        return json.loads(cleaned_output)
    except json.JSONDecodeError:
        # If parsing fails, attempt to extract the first balanced JSON object
        # or array from the cleaned output. This is defensive because some
        # bridge implementations may emit logging or timing information
        # alongside the JSON payload.
        def _extract_balanced(s: str) -> Optional[str]:
            # Find the first open bracket ('{' or '[') and then scan forward
            # to find the matching closing bracket taking nesting into account.
            start_idx = None
            open_char = None
            for i, ch in enumerate(s):
                if ch == '{' or ch == '[':
                    start_idx = i
                    open_char = ch
                    break
            if start_idx is None:
                return None

            pairs = {'{': '}', '[': ']'}
            close_char = pairs[open_char]
            # If we're attempting to list dataflows, fall back to calling the
            # ABS REST endpoint directly from Python. This avoids the MCP bridge
            # crashing the process and lets the backend attempt a best-effort
            # retrieval and normalization.
            try:
                if command == "list-dataflows":
                    settings = get_settings()
                    abs_base = getattr(settings, "abs_api_base", "https://data.api.abs.gov.au")
                    url = f"{abs_base.rstrip('/')}/rest/dataflow/ABS"
                    # Request SDMX structure JSON if available
                    headers = {"Accept": "application/vnd.sdmx.structure+json, application/json, application/*+json"}
                    resp = httpx.get(url, headers=headers, timeout=10.0)
                    if resp.status_code == 200:
                        parsed = resp.json()

                        def _extract_text(value: object) -> str:
                            if value is None:
                                return ""
                            if isinstance(value, str):
                                return value
                            if isinstance(value, list) and value:
                                # prefer english labelled entry
                                for entry in value:
                                    if isinstance(entry, dict) and entry.get("lang") == "en":
                                        return _extract_text(entry.get("_text") or entry.get("text") or entry.get("value"))
                                return _extract_text(value[0])
                            if isinstance(value, dict):
                                return _extract_text(value.get("_text") or value.get("text") or value.get("value"))
                            return str(value)

                        dataflows_node = (parsed.get("Structure") or {}).get("Structures") if isinstance(parsed, dict) else None
                        candidates = []
                        if dataflows_node:
                            df = dataflows_node.get("Dataflows") or {}
                            flows = df.get("Dataflow")
                            if flows:
                                flows_arr = flows if isinstance(flows, list) else [flows]
                                for flow in flows_arr:
                                    fid = flow.get("id")
                                    name = _extract_text(flow.get("Name"))
                                    desc = _extract_text(flow.get("Description"))
                                    candidates.append({"id": fid, "name": name, "description": desc})
                        # If we parsed something sensible, return a compact structure
                        if candidates:
                            return {"total": len(candidates), "dataflows": candidates}
            except Exception:
                # swallow and continue to raising the original MCPBridgeError below
                pass
            depth = 0
            for j in range(start_idx, len(s)):
                if s[j] == open_char:
                    depth += 1
                elif s[j] == close_char:
                    depth -= 1
                    if depth == 0:
                        return s[start_idx : j + 1]
            return None

        fragment = _extract_balanced(cleaned_output)
        if fragment is not None:
            try:
                return json.loads(fragment)
            except json.JSONDecodeError:
                # Fall through to raising an informative error below.
                pass
        raise MCPBridgeError(
            f"Bridge command '{command}' returned invalid JSON or non-JSON output. "
            f"Raw output (first 1000 chars): {output[:1000]}\n"
            f"Cleaned fragment (first 400 chars): {cleaned_output[:400]!r}"
        )


def list_dataflows(force_refresh: bool = False) -> Any:
    payload = {"forceRefresh": force_refresh}
    return _run_bridge("list-dataflows", payload)


def get_dataflow_metadata(dataset_id: str, force_refresh: bool = False) -> Any:
    payload = {"datasetId": dataset_id, "forceRefresh": force_refresh}
    return _run_bridge("get-dataflow-metadata", payload)


def query_dataset(
    dataset_id: str,
    data_key: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    detail: Optional[str] = None,
    dimension_at_observation: Optional[str] = None,
) -> Any:
    payload = {
        "datasetId": dataset_id,
        "dataKey": data_key,
        "startPeriod": start_period,
        "endPeriod": end_period,
        "detail": detail,
        "dimensionAtObservation": dimension_at_observation,
    }
    payload = {key: value for key, value in payload.items() if value is not None}
    return _run_bridge("query-dataset", payload)


def resolve_dataset(
    dataset_id: str,
    data_key: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    detail: Optional[str] = None,
    dimension_at_observation: Optional[str] = None,
) -> Any:
    payload = {
        "datasetId": dataset_id,
        "dataKey": data_key,
        "startPeriod": start_period,
        "endPeriod": end_period,
        "detail": detail,
        "dimensionAtObservation": dimension_at_observation,
    }
    # Remove None values for cleaner payloads
    payload = {key: value for key, value in payload.items() if value is not None}
    return _run_bridge("resolve-dataset", payload)


def describe_dataset_availability(
    dataset_id: str,
    force_refresh: bool = False,
) -> Any:
    payload = {"datasetId": dataset_id, "forceRefresh": force_refresh}
    return _run_bridge("describe-availability", payload)
