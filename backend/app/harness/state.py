from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


ALLOWED_STEP_IDS = {
    "use_abs_data_tool",
    "use_web_search_tool",
    "use_sandbox_tool",
    "propose_plan",
    "compose_final",
}


def _compact_structured_value(value: Any, *, limit_chars: int = 12000) -> Any:
    if not isinstance(value, (dict, list)):
        return value
    try:
        rendered = json.dumps(value, ensure_ascii=False)
    except Exception:
        return None
    if len(rendered) <= limit_chars:
        return value
    if isinstance(value, dict):
        trimmed: Dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= 12:
                break
            if isinstance(item, list):
                trimmed[key] = item[:8]
            elif isinstance(item, dict):
                child = {}
                for child_index, (child_key, child_value) in enumerate(item.items()):
                    if child_index >= 12:
                        break
                    child[child_key] = child_value
                trimmed[key] = child
            else:
                trimmed[key] = item
        return trimmed
    return value[:20]


def _load_artifact_inline_content(item: Dict[str, Any], *, limit_chars: int = 4000) -> Any:
    kind = str(item.get("kind") or "").strip()
    if kind == "abs_resolved_dataset":
        return None

    path_value = str(item.get("path") or "").strip()
    if not path_value:
        return None
    path = Path(path_value)
    if not path.exists() or not path.is_file():
        return None

    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return None

    if len(raw) > limit_chars:
        raw = raw[:limit_chars]

    stripped = raw.strip()
    if not stripped:
        return None

    if path.suffix.lower() == ".json":
        try:
            return _compact_structured_value(json.loads(stripped), limit_chars=limit_chars)
        except Exception:
            return stripped
    return stripped


def compact_loop_history(loops: List[Dict[str, Any]], limit: int = 8) -> List[Dict[str, Any]]:
    compact: List[Dict[str, Any]] = []
    for item in loops[-limit:]:
        if not isinstance(item, dict):
            continue
        step = item.get("step") if isinstance(item.get("step"), dict) else {}
        entry = {
            "step": {
                "id": str(step.get("id") or "").strip(),
                "summary": str(step.get("summary") or "").strip()[:160],
            },
            "progress_note": str(item.get("progress_note") or "").strip()[:120],
            "result_summary": str(item.get("result_summary") or "").strip()[:2400],
        }
        if "result_data" in item:
            compact_result = _compact_structured_value(item.get("result_data"), limit_chars=12000)
            if compact_result is not None:
                entry["result_data"] = compact_result
        compact.append(entry)
    return compact


def compact_chat_history(messages: List[Dict[str, str]], limit: int = 6) -> List[Dict[str, str]]:
    compact: List[Dict[str, str]] = []
    for item in messages[-limit:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        compact.append({"role": role, "content": content[:600]})
    return compact


def build_chat_history_payload(
    messages: List[Dict[str, str]],
    *,
    recent_full_limit: int = 8,
    older_compact_limit: int = 4,
) -> List[Dict[str, str]]:
    if not isinstance(messages, list) or not messages:
        return []

    recent_slice = messages[-recent_full_limit:] if recent_full_limit > 0 else []
    older_slice = messages[:-recent_full_limit] if recent_full_limit > 0 else messages

    payload: List[Dict[str, str]] = []
    if older_slice and older_compact_limit > 0:
        payload.extend(compact_chat_history(older_slice, limit=older_compact_limit))

    for item in recent_slice:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        payload.append({"role": role, "content": content})

    return payload


def compact_artifacts(artifacts: List[Dict[str, Any]], limit: int = 8) -> List[Dict[str, Any]]:
    compact: List[Dict[str, Any]] = []
    for item in artifacts[-limit:]:
        if not isinstance(item, dict):
            continue
        entry = {
            "artifact_id": str(item.get("artifact_id") or "").strip(),
            "kind": str(item.get("kind") or "").strip(),
            "label": str(item.get("label") or "").strip()[:120],
            "summary": str(item.get("summary") or "").strip()[:320],
        }
        inline_content = _load_artifact_inline_content(item, limit_chars=4000)
        if inline_content is not None:
            entry["content"] = inline_content
        compact.append(entry)
    return compact
