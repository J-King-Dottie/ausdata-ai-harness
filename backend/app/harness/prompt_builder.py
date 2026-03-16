from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


PROMPT_PATH = Path(__file__).resolve().parents[3] / "HARNESS_SYSTEM_PROMPT.txt"
SANDBOX_CODEGEN_PROMPT_PATH = Path(__file__).resolve().parents[3] / "SANDBOX_CODEGEN_SYSTEM_PROMPT.txt"
SOUL_PATH = Path(__file__).resolve().parents[3] / "SOUL.md"
CURATION_GUIDE_PATH = Path(__file__).resolve().parents[3] / "ABS_CURATION_AGENT.md"

_PROMPT_CACHE = ""
_PROMPT_CACHE_KEY = ""
_SANDBOX_PROMPT_CACHE = ""
_SANDBOX_PROMPT_CACHE_KEY = ""


def _compact_soul_text(soul_text: str) -> str:
    lines = [line.strip() for line in soul_text.splitlines()]
    sections: Dict[str, List[str]] = {}
    current = ""
    for line in lines:
        if not line:
            continue
        if line.startswith("## "):
            current = line[3:].strip().lower()
            sections[current] = []
            continue
        if current:
            sections.setdefault(current, []).append(line)

    compact_lines: List[str] = []
    name_lines = sections.get("name") or []
    myth_lines = sections.get("myth") or []
    role_lines = sections.get("role") or []
    personality_lines = sections.get("personality") or []
    preference_lines = sections.get("preferences") or []
    candour_lines = sections.get("candour") or []
    scope_lines = sections.get("humility with scope") or []

    if name_lines:
        compact_lines.append("Name and role: " + " ".join(name_lines[:2]))
    if myth_lines:
        compact_lines.append("Mythic meaning: " + " ".join(myth_lines[:3]))
    if role_lines:
        compact_lines.append("Purpose: " + " ".join(role_lines[:2]))
    if personality_lines:
        compact_lines.append("Voice: " + " ".join(personality_lines[:6]))
    if preference_lines:
        compact_lines.append("Preferences: " + " ".join(preference_lines[:6]))
    if candour_lines:
        compact_lines.append("Candour: " + " ".join(candour_lines[:3]))
    if scope_lines:
        compact_lines.append("Scope: " + " ".join(scope_lines[:4]))

    compact = "\n".join(compact_lines).strip()
    return compact if compact else soul_text[:1200]


def _needs_curation_guide(plan_state: Dict[str, Any] | None) -> bool:
    state = dict(plan_state or {})
    if bool(state.get("curation_mode")):
        return True
    approved_plan = state.get("approved_plan")
    if isinstance(approved_plan, dict):
        if bool(approved_plan.get("allow_raw_discovery")):
            return True
        if str(approved_plan.get("curate_dataset_id") or "").strip():
            return True
    pending_context = state.get("pending_plan_context")
    if isinstance(pending_context, dict):
        if bool(pending_context.get("allow_raw_discovery")):
            return True
        if str(pending_context.get("curate_dataset_id") or "").strip():
            return True
    return False


def load_system_prompt(include_curation_guide: bool = False) -> str:
    global _PROMPT_CACHE, _PROMPT_CACHE_KEY
    prompt_text = PROMPT_PATH.read_text(encoding="utf-8").strip()
    soul_text = SOUL_PATH.read_text(encoding="utf-8").strip() if SOUL_PATH.exists() else ""
    curation_text = (
        CURATION_GUIDE_PATH.read_text(encoding="utf-8").strip()
        if include_curation_guide and CURATION_GUIDE_PATH.exists()
        else ""
    )
    cache_key = f"{hash(prompt_text)}:{hash(soul_text)}:{hash(curation_text)}"
    if _PROMPT_CACHE and _PROMPT_CACHE_KEY == cache_key:
        return _PROMPT_CACHE

    combined = prompt_text
    if soul_text:
        compact_soul = _compact_soul_text(soul_text)
        combined = (
            f"{combined}\n\n"
            "Standing identity and tone guide from SOUL.md:\n"
            f"{compact_soul}"
        )
    if curation_text:
        combined = (
            f"{combined}\n\n"
            "Conditional autonomous ABS curation guide:\n"
            f"{curation_text}"
        )

    _PROMPT_CACHE = combined
    _PROMPT_CACHE_KEY = cache_key
    return _PROMPT_CACHE


def load_sandbox_codegen_system_prompt() -> str:
    global _SANDBOX_PROMPT_CACHE, _SANDBOX_PROMPT_CACHE_KEY
    prompt_text = SANDBOX_CODEGEN_PROMPT_PATH.read_text(encoding="utf-8").strip()
    cache_key = str(hash(prompt_text))
    if _SANDBOX_PROMPT_CACHE and _SANDBOX_PROMPT_CACHE_KEY == cache_key:
        return _SANDBOX_PROMPT_CACHE
    _SANDBOX_PROMPT_CACHE = prompt_text
    _SANDBOX_PROMPT_CACHE_KEY = cache_key
    return _SANDBOX_PROMPT_CACHE


def build_loop_payload(
    *,
    user_message: str,
    chat_history: List[Dict[str, str]],
    loop_history: List[Dict[str, Any]],
    artifacts: List[Dict[str, Any]],
    plan_state: Dict[str, Any],
    pre_run_provider_route: Dict[str, Any] | None,
    pre_run_dataset_shortlist: List[Dict[str, Any]] | None,
    pre_run_macro_indicator_shortlist: List[Dict[str, Any]] | None,
    loop_index: int,
    max_loops: int,
    protected_loop_history_count: int = 0,
    protected_artifact_count: int = 0,
) -> Dict[str, Any]:
    payload = {
        "task": {
            "user_message": user_message,
            "loop_index": loop_index,
            "max_loops": max_loops,
            "force_compose_final": loop_index >= max_loops,
        },
        "chat_history": chat_history,
        "loop_history": loop_history,
        "available_artifacts": artifacts,
        "plan_state": plan_state,
        "pre_run_provider_route": pre_run_provider_route or {},
        "pre_run_dataset_shortlist": pre_run_dataset_shortlist or [],
        "pre_run_macro_indicator_shortlist": pre_run_macro_indicator_shortlist or [],
    }
    serialized = json.dumps(payload, ensure_ascii=True)
    if len(serialized) <= 18000:
        return payload

    trimmed_payload = dict(payload)
    keep_loop_count = max(protected_loop_history_count + 4, min(len(loop_history), 8))
    keep_artifact_count = max(protected_artifact_count + 4, min(len(artifacts), 8))
    trimmed_payload["loop_history"] = loop_history[-keep_loop_count:]
    trimmed_payload["available_artifacts"] = artifacts[-keep_artifact_count:]
    trimmed_payload["chat_history"] = chat_history[-4:]
    serialized = json.dumps(trimmed_payload, ensure_ascii=True)
    if len(serialized) <= 14000:
        return trimmed_payload

    keep_loop_count = max(protected_loop_history_count, min(len(loop_history), 4))
    keep_artifact_count = max(protected_artifact_count, min(len(artifacts), 4))
    trimmed_payload["loop_history"] = loop_history[-keep_loop_count:] if keep_loop_count else []
    trimmed_payload["available_artifacts"] = artifacts[-keep_artifact_count:] if keep_artifact_count else []
    trimmed_payload["chat_history"] = chat_history[-3:]
    return trimmed_payload


def _count_recent_parse_failures(loop_history: List[Dict[str, Any]]) -> int:
    count = 0
    for item in reversed(loop_history):
        if not isinstance(item, dict):
            break
        result_data = item.get("result_data") if isinstance(item.get("result_data"), dict) else {}
        if str(result_data.get("kind") or "").strip() != "harness_parse_error":
            break
        count += 1
    return count


def build_model_messages(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    include_curation_guide = _needs_curation_guide(
        payload.get("plan_state") if isinstance(payload.get("plan_state"), dict) else None
    )
    messages = [
        {"role": "system", "content": load_system_prompt(include_curation_guide=include_curation_guide)},
        {
            "role": "user",
            "content": (
                "Loop payload for this cycle:\n"
                f"{json.dumps(payload, ensure_ascii=True)}\n\n"
                "Return strict JSON only."
            ),
        },
    ]
    loop_history = payload.get("loop_history") if isinstance(payload.get("loop_history"), list) else []
    parse_failures = _count_recent_parse_failures(loop_history)
    if parse_failures == 1:
        messages.append(
            {
                "role": "user",
                "content": (
                    "Your previous loop failed output validation.\n"
                    "Do not rethink the task. Return the next intended loop decision as one literal JSON object only.\n"
                    "Required top-level keys: step, progress_note, model_output.\n"
                    "Do not include prose, markdown fences, quoted JSON, escaped JSON, or wrapper objects."
                ),
            }
        )
    elif parse_failures >= 2:
        messages.append(
            {
                "role": "user",
                "content": (
                    "Output formatting correction is mandatory.\n"
                    "Return exactly one literal top-level JSON object.\n"
                    "The object itself must contain only the normal harness fields for this decision: step, progress_note, model_output.\n"
                    "For tool steps, use this exact shape:\n"
                    "{\"step\":{\"id\":\"sandbox_tool\",\"summary\":\"...\"},\"progress_note\":\"...\",\"model_output\":{\"tool_name\":\"sandbox_tool\",\"tool_input\":{\"artifact_ids\":[\"artifact-001\"],\"sandbox_request\":\"Inspect the artifact, isolate the exact comparable slice, save a narrowed artifact if needed, then prepare the calculation output.\"}}}\n"
                    "No prose. No markdown fences. No quoted JSON. No escaped JSON. Do not rethink the task; only return a valid harness payload."
                ),
            }
        )
    return messages


def _extract_artifact_refs(value: Any) -> set[str]:
    refs: set[str] = set()
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("artifact-"):
            refs.add(text)
        return refs
    if isinstance(value, list):
        for item in value:
            refs.update(_extract_artifact_refs(item))
        return refs
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "artifact_id" and isinstance(item, str) and item.strip():
                refs.add(item.strip())
            else:
                refs.update(_extract_artifact_refs(item))
        return refs
    return refs


def _filter_codegen_loop_history(loop_history: List[Dict[str, Any]], artifact_ids: set[str]) -> List[Dict[str, Any]]:
    if not loop_history:
        return []

    filtered: List[Dict[str, Any]] = []
    for index, item in enumerate(loop_history):
        if not isinstance(item, dict):
            continue
        keep = index >= len(loop_history) - 3
        if not keep:
            step = item.get("step") if isinstance(item.get("step"), dict) else {}
            result_data = item.get("result_data") if isinstance(item.get("result_data"), dict) else {}
            tool_input = item.get("tool_input") if isinstance(item.get("tool_input"), dict) else {}
            step_id = str(step.get("id") or "").strip()
            item_artifacts = (
                _extract_artifact_refs(tool_input)
                | _extract_artifact_refs(result_data)
                | _extract_artifact_refs(item.get("result_summary"))
            )
            if artifact_ids and item_artifacts.intersection(artifact_ids):
                keep = True
            elif str(result_data.get("kind") or "").strip() in {
                "tool_failure",
                "harness_parse_error",
                "sandbox_output",
                "sandbox_result",
            }:
                keep = True
            elif step_id == "sandbox_tool":
                keep = True
        if keep:
            filtered.append(item)

    return filtered[-8:]


def build_sandbox_codegen_messages(
    *,
    payload: Dict[str, Any],
    sandbox_request: str,
    artifact_ids: List[str],
) -> List[Dict[str, str]]:
    codegen_payload = dict(payload)
    artifact_id_set = {str(item).strip() for item in artifact_ids if str(item).strip()}
    task = dict(codegen_payload.get("task") or {})
    task["mode"] = "sandbox_codegen"
    task["sandbox_request"] = str(sandbox_request or "").strip()
    task["artifact_ids"] = sorted(artifact_id_set)
    codegen_payload["task"] = task
    codegen_payload.pop("chat_history", None)

    artifacts = codegen_payload.get("available_artifacts")
    if isinstance(artifacts, list):
        codegen_payload["available_artifacts"] = [
            item
            for item in artifacts
            if isinstance(item, dict) and str(item.get("artifact_id") or "").strip() in artifact_id_set
        ]

    loop_history = codegen_payload.get("loop_history")
    if isinstance(loop_history, list):
        codegen_payload["loop_history"] = _filter_codegen_loop_history(loop_history, artifact_id_set)

    return [
        {"role": "system", "content": load_sandbox_codegen_system_prompt()},
        {
            "role": "user",
            "content": (
                "Loop payload for this sandbox code step:\n"
                f"{json.dumps(codegen_payload, ensure_ascii=True)}\n\n"
                "Return Python code only."
            ),
        },
    ]
