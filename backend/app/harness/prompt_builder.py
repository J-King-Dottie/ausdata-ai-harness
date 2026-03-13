from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


PROMPT_PATH = Path(__file__).resolve().parents[3] / "HARNESS_SYSTEM_PROMPT.txt"
SOUL_PATH = Path(__file__).resolve().parents[3] / "SOUL.md"
CURATION_GUIDE_PATH = Path(__file__).resolve().parents[3] / "ABS_CURATION_AGENT.md"

_PROMPT_CACHE = ""
_PROMPT_CACHE_KEY = ""


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
            f"{prompt_text}\n\n"
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


def build_loop_payload(
    *,
    user_message: str,
    chat_history: List[Dict[str, str]],
    loop_history: List[Dict[str, Any]],
    artifacts: List[Dict[str, Any]],
    plan_state: Dict[str, Any],
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


def build_model_messages(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    include_curation_guide = _needs_curation_guide(
        payload.get("plan_state") if isinstance(payload.get("plan_state"), dict) else None
    )
    return [
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
