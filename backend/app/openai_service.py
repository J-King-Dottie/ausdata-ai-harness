from __future__ import annotations

from html import unescape
import json
import logging
import re
import shutil
import subprocess
from urllib.parse import parse_qs, urlparse
from pathlib import Path
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional

import httpx

from .config import get_settings
from .curated_abs import get_curated_dataset, list_curated_datasets, upsert_ai_curated_dataset
from .harness.parser import HarnessParserError, parse_harness_loop_output
from .harness.prompt_builder import (
    build_sandbox_codegen_messages,
    build_loop_payload,
    build_model_messages,
    load_system_prompt,
)
from .harness.state import build_chat_history_payload, compact_artifacts, compact_chat_history, compact_loop_history
from .mcp_bridge import MCPBridgeError, get_dataflow_metadata, list_dataflows, resolve_dataset
from .storage import ConversationStore


settings = get_settings()
logger = logging.getLogger("abs.backend.harness")
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
    )
    logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)
logger.propagate = False

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
WEB_SEARCH_URL = "https://html.duckduckgo.com/html/"
WEB_USER_AGENT = "Mozilla/5.0 (compatible; Seshat/1.0; +https://dottieaistudio.com.au/)"
HARNESS_RESPONSE_SCHEMA: Dict[str, Any] = {
    "type": "json_object",
}
PLAN_APPROVAL_RE = re.compile(r"^\s*(yes|y|ok|okay|proceed|go ahead|do it|use that|add it|sounds good)\b", re.IGNORECASE)
PLAN_REJECT_RE = re.compile(r"^\s*(no|nah|stop|cancel|change|revise|refine)\b", re.IGNORECASE)
CORRECTION_RESET_RE = re.compile(
    r"\b("
    r"wrong|mistake|made a mistake|double[\s-]?count|double[\s-]?counted|"
    r"corrected|redo|re-do|recheck|re-check|rebuild|re-run|rerun|"
    r"that's not right|that is not right|you messed up|you are making mistakes|"
    r"correct totals|with the corrected totals|with correct totals"
    r")\b",
    re.IGNORECASE,
)
WEB_RESULT_LINK_RE = re.compile(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
WEB_RESULT_SNIPPET_RE = re.compile(r'<(?:a|div)[^>]+class="result__snippet"[^>]*>(.*?)</(?:a|div)>', re.IGNORECASE | re.DOTALL)
STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "by", "for", "from", "how", "i", "in",
    "is", "it", "of", "on", "or", "the", "to", "what", "which", "with", "over",
    "last", "years", "year", "there", "many", "much", "jobs", "job", "data",
}
CURATED_DATASET_ALIASES = {
    "LABOUR_ACCT": "LABOUR_ACCT_Q",
    "LABOUR_ACCOUNT": "LABOUR_ACCT_Q",
    "LABOUR_ACCOUNTS": "LABOUR_ACCT_Q",
    "NATIONAL_ACCTS_SFD": "ANA_SFD",
    "NATIONAL_ACCOUNTS_SFD": "ANA_SFD",
    "STATE_FINAL_DEMAND": "ANA_SFD",
    "STATE_FINAL_DEMAND_DATA": "ANA_SFD",
    "NATIONAL_ACCTS_AGG": "ANA_AGG",
    "NATIONAL_ACCOUNTS_AGG": "ANA_AGG",
    "KEY_AGGREGATES": "ANA_AGG",
}
CLARIFICATION_KEYWORDS = {
    "why", "driver", "drivers", "cause", "causes", "explain", "decline",
    "declining", "falling", "trend", "over", "happened", "happen",
}
COMPLEX_ANALYSIS_KEYWORDS = {
    "compare", "comparison", "ratio", "per", "highest", "lowest", "rank",
    "ranking", "versus", "vs", "relative", "relative_to", "productivity",
}

CORRECTION_EXPLANATION_RE = re.compile(
    r"\b(what caused|why did you make|why did that happen|explain what went wrong)\b",
    re.IGNORECASE,
)
MAX_CONSECUTIVE_RECOVERY_FAILURES = 3


class ConversationCancelled(RuntimeError):
    """Raised when a conversation is cancelled mid-generation."""


_CANCELLATION_LOCK = Lock()
_CANCELLATION_EVENTS: Dict[str, Event] = {}


def _acquire_cancellation_event(conversation_id: str) -> Event:
    with _CANCELLATION_LOCK:
        event = _CANCELLATION_EVENTS.get(conversation_id)
        if event is None:
            event = Event()
            _CANCELLATION_EVENTS[conversation_id] = event
        event.clear()
        return event


def cancel_conversation_processing(conversation_id: str) -> None:
    with _CANCELLATION_LOCK:
        event = _CANCELLATION_EVENTS.get(conversation_id)
        if event is None:
            event = Event()
            _CANCELLATION_EVENTS[conversation_id] = event
        event.set()


def _release_cancellation_event(conversation_id: str) -> None:
    with _CANCELLATION_LOCK:
        _CANCELLATION_EVENTS.pop(conversation_id, None)


def _ensure_not_cancelled(conversation_id: str, event: Event, stage: str) -> None:
    if event.is_set():
        logger.info("Conversation cancelled cid=%s stage=%s", conversation_id, stage)
        raise ConversationCancelled(f"Conversation {conversation_id} cancelled during {stage}")


def _conversation_runtime_dir(conversation_id: str) -> Path:
    safe_id = "".join(ch for ch in conversation_id if ch.isalnum() or ch in {"-", "_"})
    if not safe_id:
        safe_id = "conversation"
    return settings.runtime_dir / "conversations" / safe_id


def _ensure_runtime_dirs(conversation_id: str) -> Path:
    run_dir = _conversation_runtime_dir(conversation_id)
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (run_dir / "sandbox").mkdir(parents=True, exist_ok=True)
    return run_dir


def _clear_runtime_dir(conversation_id: str) -> None:
    run_dir = _conversation_runtime_dir(conversation_id)
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)


def clear_dataset_cache() -> None:
    if settings.runtime_dir.exists():
        shutil.rmtree(settings.runtime_dir, ignore_errors=True)


def reset_conversation_runtime(conversation_id: str) -> None:
    _clear_runtime_dir(conversation_id)


def reset_all_conversation_memory() -> None:
    clear_dataset_cache()


def _extract_openai_output_text(response_data: Dict[str, Any]) -> str:
    output_text = str(response_data.get("output_text") or "").strip()
    if output_text:
        return output_text

    output = response_data.get("output")
    if not isinstance(output, list):
        return ""

    fragments: List[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = str(block.get("type") or "").strip().lower()
            text_value = block.get("text")
            if block_type in {"output_text", "text"} and text_value:
                fragments.append(str(text_value))

    return "".join(fragments).strip()


def _summarize_openai_response(response_data: Dict[str, Any]) -> str:
    if not isinstance(response_data, dict):
        return _truncate(str(response_data), 600)

    def _present(value: Any) -> bool:
        return value is not None and value != ""

    summary: Dict[str, Any] = {}
    for key in ("id", "status", "model", "output_text"):
        value = response_data.get(key)
        if _present(value):
            summary[key] = _truncate(str(value), 240)

    for key in ("incomplete_details", "error", "usage"):
        value = response_data.get(key)
        if _present(value):
            try:
                summary[key] = json.loads(json.dumps(value, ensure_ascii=True))
            except Exception:
                summary[key] = _truncate(str(value), 240)

    output = response_data.get("output")
    if isinstance(output, list):
        output_summary = []
        for item in output[:4]:
            if not isinstance(item, dict):
                output_summary.append(_truncate(str(item), 120))
                continue
            entry: Dict[str, Any] = {}
            item_type = item.get("type")
            if _present(item_type):
                entry["type"] = item_type
            item_status = item.get("status")
            if _present(item_status):
                entry["status"] = item_status
            for key in ("id", "role"):
                value = item.get(key)
                if _present(value):
                    entry[key] = value
            content = item.get("content")
            if isinstance(content, list):
                content_summary = []
                for block in content[:4]:
                    if not isinstance(block, dict):
                        content_summary.append(_truncate(str(block), 120))
                        continue
                    block_entry: Dict[str, Any] = {}
                    block_type = block.get("type")
                    if _present(block_type):
                        block_entry["type"] = block_type
                    block_status = block.get("status")
                    if _present(block_status):
                        block_entry["status"] = block_status
                    text_value = block.get("text")
                    if _present(text_value):
                        block_entry["text_preview"] = _truncate(str(text_value), 160)
                    refusal = block.get("refusal")
                    if _present(refusal):
                        block_entry["refusal_preview"] = _truncate(str(refusal), 160)
                    for key in ("id",):
                        value = block.get(key)
                        if _present(value):
                            block_entry[key] = value
                    if block_entry:
                        content_summary.append(block_entry)
                entry["content"] = content_summary
            if entry:
                output_summary.append(entry)
        summary["output"] = output_summary

    return _truncate(json.dumps(summary, ensure_ascii=True), 1200)


def _call_model(messages: List[Dict[str, str]], *, reasoning_effort: Optional[str] = None) -> str:
    openai_messages: List[Dict[str, Any]] = []
    for message in messages:
        role = str(message.get("role") or "user").strip().lower()
        content = str(message.get("content") or "").strip()
        if not content:
            continue
        if role not in {"system", "user", "assistant", "developer"}:
            role = "user"
        content_type = "output_text" if role == "assistant" else "input_text"
        openai_messages.append(
            {
                "role": role,
                "content": [
                    {
                        "type": content_type,
                        "text": content,
                    }
                ],
            }
        )

    if not openai_messages:
        raise RuntimeError("No model input was generated for the harness loop.")

    payload: Dict[str, Any] = {
        "model": settings.openai_model,
        "input": openai_messages,
        "reasoning": {
            "effort": reasoning_effort or settings.openai_reasoning_effort,
        },
        "text": {
            "format": HARNESS_RESPONSE_SCHEMA,
        },
    }

    response = httpx.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=settings.openai_timeout_seconds,
    )
    if response.status_code >= 400:
        if response.status_code == 429:
            raise RuntimeError(
                "OpenAI rate limit reached for this request. "
                "Please retry in a moment or ask a narrower follow-up."
            )
        raise RuntimeError(
            f"OpenAI responses error {response.status_code}: {response.text.strip()}"
        )

    response_data = response.json()
    text = _extract_openai_output_text(response_data)
    if not text:
        raise RuntimeError(
            "OpenAI returned an empty response. "
            f"Response summary: {_summarize_openai_response(response_data)}"
        )
    return text


def _call_model_text(
    messages: List[Dict[str, str]],
    *,
    reasoning_effort: Optional[str] = None,
) -> str:
    openai_messages: List[Dict[str, Any]] = []
    for message in messages:
        role = str(message.get("role") or "user").strip().lower()
        content = str(message.get("content") or "").strip()
        if not content:
            continue
        if role not in {"system", "user", "assistant", "developer"}:
            role = "user"
        content_type = "output_text" if role == "assistant" else "input_text"
        openai_messages.append(
            {
                "role": role,
                "content": [
                    {
                        "type": content_type,
                        "text": content,
                    }
                ],
            }
        )

    if not openai_messages:
        raise RuntimeError("No model input was generated for the sandbox code step.")

    payload: Dict[str, Any] = {
        "model": settings.openai_model,
        "input": openai_messages,
        "reasoning": {
            "effort": reasoning_effort or "low",
        },
    }

    response = httpx.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=settings.openai_timeout_seconds,
    )
    if response.status_code >= 400:
        if response.status_code == 429:
            raise RuntimeError(
                "OpenAI rate limit reached for this sandbox code step. "
                "Please retry in a moment or ask a narrower follow-up."
            )
        raise RuntimeError(
            f"OpenAI responses error {response.status_code}: {response.text.strip()}"
        )

    response_data = response.json()
    text = _extract_openai_output_text(response_data)
    if not text:
        raise RuntimeError(
            "OpenAI returned an empty response for the sandbox code step. "
            f"Response summary: {_summarize_openai_response(response_data)}"
        )
    return text


def _compact_user_only_history(messages: List[Dict[str, str]], limit: int = 6) -> List[Dict[str, str]]:
    compact: List[Dict[str, str]] = []
    for item in messages[-limit * 2:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or "").strip()
        if role != "user" or not content:
            continue
        compact.append({"role": "user", "content": content[:600]})
    return compact[-limit:]


def _retain_non_scratchpad_artifacts(artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "").strip()
        if kind in {"sandbox_result", "sandbox_output"}:
            continue
        kept.append(item)
    return kept


def _retain_non_scratchpad_loop_history(loop_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for item in loop_history:
        if not isinstance(item, dict):
            continue
        step = item.get("step") if isinstance(item.get("step"), dict) else {}
        step_id = str(step.get("id") or "").strip()
        result_data = item.get("result_data") if isinstance(item.get("result_data"), dict) else {}
        kind = str(result_data.get("kind") or "").strip()
        if step_id == "use_sandbox_tool":
            continue
        if step_id == "compose_final" and kind != "curation_handoff":
            continue
        kept.append(item)
    return kept


def _should_reset_after_user_correction(state, user_message: str) -> bool:
    text = str(user_message or "").strip()
    if not text:
        return False
    if CORRECTION_EXPLANATION_RE.search(text):
        return False
    if not CORRECTION_RESET_RE.search(text):
        return False
    if not state.loop_history and not state.artifacts:
        return False
    return any(
        isinstance(item, dict) and str(item.get("role") or "").strip().lower() == "assistant"
        for item in (state.messages[-6:] if isinstance(state.messages, list) else [])
    )


def _build_correction_reset_message(state, user_message: str) -> str:
    recent_user_messages = [
        str(item.get("content") or "").strip()
        for item in (state.messages if isinstance(state.messages, list) else [])
        if isinstance(item, dict) and str(item.get("role") or "").strip().lower() == "user"
    ]
    current = str(user_message or "").strip()
    prior_user_messages = [message for message in recent_user_messages[:-1] if message]
    prior_question = prior_user_messages[-1] if prior_user_messages else ""

    parts = ["A user correction invalidated earlier derived results."]
    if prior_question:
        parts.append(f"Most recent user context before the correction: {prior_question}")
    parts.append(f"Current correction or revised request: {current}")
    parts.append(
        "Treat prior derived artifacts, rankings, charts, and intermediate calculations as stale. "
        "Do not reuse them. Re-retrieve or re-derive from source data and validated totals only."
    )
    return "\n\n".join(parts).strip()


def _reset_context_after_user_correction(state, user_message: str) -> str:
    correction_message = _build_correction_reset_message(state, user_message)
    handoff_entry = {
        "step": {
            "id": "compose_final",
            "summary": "User correction detected; previous analytical scratchpad cleared",
        },
        "progress_note": "Rechecking from source after the correction.",
        "result_summary": (
            "The user called out a likely analytical error. Current-run artifacts and loop history "
            "were cleared before recomputation."
        ),
        "result_data": {
            "kind": "correction_handoff",
            "user_correction": str(user_message or "").strip(),
        },
    }
    preserved_loop_history = _retain_non_scratchpad_loop_history(
        state.loop_history[:-1] if state.loop_history else []
    )
    preserved_artifacts = _retain_non_scratchpad_artifacts(state.artifacts)
    state.loop_history = preserved_loop_history + [handoff_entry]
    state.artifacts = preserved_artifacts
    return correction_message


def _compose_best_effort_final(conversation_id: str, user_message: str, state) -> str:
    payload = {
        "task": {
            "user_message": user_message,
            "reason": "Loop limit reached. Compose the best possible final answer from the evidence already gathered.",
        },
        "chat_history": build_chat_history_payload(state.messages, recent_full_limit=6, older_compact_limit=3),
        "loop_history": compact_loop_history(state.loop_history, limit=4),
        "available_artifacts": compact_artifacts(state.artifacts, limit=4),
        "plan_state": _build_plan_state(state),
    }
    messages = [
        {"role": "system", "content": load_system_prompt()},
        {
            "role": "user",
            "content": (
                "You must stop using tools and write the best possible final answer now.\n"
                "Use only the evidence already gathered in this conversation.\n"
                "Do not mention loop limits or internal harness mechanics.\n"
                "If the evidence is incomplete, answer to the best of your ability, "
                "state what the evidence does show, and end with one short caveat only if needed.\n"
                "Do not ask the user a clarification question in this fallback path.\n\n"
                f"Current state:\n{json.dumps(payload, ensure_ascii=True)}"
            ),
        },
    ]
    try:
        return _call_model(messages)
    except Exception as exc:
        logger.exception(
            "Best-effort final compose failed cid=%s error=%s",
            conversation_id,
            exc,
        )
        return (
            "I couldn't fully resolve every part of that, but here is the best answer from the evidence gathered so far.\n\n"
            "I found relevant ABS data and narrowed the likely series, but the result was not fully resolved into a clean final analysis. "
            "If you want, narrow the request to one measure such as jobs, filled jobs, output, or a specific period."
        )


def _next_artifact_id(artifacts: List[Dict[str, Any]]) -> str:
    return f"artifact-{len(artifacts) + 1:03d}"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_artifact_record(
    *,
    state,
    path: Path,
    kind: str,
    label: str,
    summary: str,
) -> Dict[str, Any]:
    artifact_id = _next_artifact_id(state.artifacts)
    record = {
        "artifact_id": artifact_id,
        "kind": kind,
        "label": label,
        "summary": summary,
        "path": str(path),
    }
    state.artifacts.append(record)
    return record


def _truncate(text: Any, limit: int = 180) -> str:
    value = str(text or "").replace("\n", " ").strip()
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _strip_html(value: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", value or "")
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _to_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _clean_string_list(value: Any) -> List[str]:
    values = value if isinstance(value, list) else [value]
    result: List[str] = []
    for item in values:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _tokenize_query(text: str) -> List[str]:
    tokens = re.findall(r"[a-z0-9]+", str(text or "").lower())
    return [token for token in tokens if len(token) > 2 and token not in STOPWORDS]


def _score_text_match(query: str, *haystacks: str) -> int:
    tokens = _tokenize_query(query)
    if not tokens:
        return 0
    corpus = " ".join(str(item or "").lower() for item in haystacks)
    score = 0
    for token in tokens:
        if token in corpus:
            score += 3 if re.search(rf"\b{re.escape(token)}\b", corpus) else 1
    return score


def _detect_plan_reply(user_message: str) -> str:
    text = str(user_message or "").strip()
    if PLAN_APPROVAL_RE.match(text):
        return "approve"
    if PLAN_REJECT_RE.match(text):
        return "revise"
    return "revise"


def _lookup_concept_label(metadata: Dict[str, Any], concept_id: str) -> str:
    for concept in _to_list(metadata.get("concepts")):
        if not isinstance(concept, dict):
            continue
        if str(concept.get("id") or "").strip() != concept_id:
            continue
        return str(concept.get("name") or concept.get("description") or concept_id).strip()
    return concept_id


def _metadata_to_curated_structure(dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    dataflow = metadata.get("dataflow") if isinstance(metadata.get("dataflow"), dict) else {}
    data_structure = metadata.get("dataStructure") if isinstance(metadata.get("dataStructure"), dict) else {}
    dimensions = [item for item in _to_list(metadata.get("dimensions")) if isinstance(item, dict)]
    codelist_lookup = {
        str(item.get("id") or "").strip(): item
        for item in _to_list(metadata.get("codelists"))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }

    dimension_order: List[str] = []
    for dimension in sorted(dimensions, key=lambda item: int(item.get("position") or 0)):
        field_id = str(dimension.get("id") or "").strip()
        if not field_id:
            continue
        dimension_order.append(field_id)

    structure_note = (
        f"ABS structured dataset. Data key order: {', '.join(dimension_order)}. "
        "Use startPeriod and endPeriod for time when appropriate."
    )

    return {
        "dataset_id": dataset_id,
        "title": str(dataflow.get("name") or data_structure.get("name") or dataset_id).strip(),
        "description": str(
            dataflow.get("description")
            or data_structure.get("description")
            or dataflow.get("name")
            or data_structure.get("name")
            or dataset_id
        ).strip(),
        "data_structure": structure_note,
        "query_templates": [],
    }


def _curate_dataset_from_abs(dataset_id: str) -> Dict[str, Any]:
    metadata = get_dataflow_metadata(dataset_id, force_refresh=True)
    if not isinstance(metadata, dict):
        raise RuntimeError(f"Live ABS metadata for {dataset_id} was not an object")
    structure_entry = _metadata_to_curated_structure(dataset_id, metadata)
    return upsert_ai_curated_dataset(structure_entry)


def _normalize_plan_context(plan_context: Any, *, fallback_question: str) -> Dict[str, Any]:
    context = dict(plan_context) if isinstance(plan_context, dict) else {}
    question = str(context.get("question") or "").strip() or str(fallback_question or "").strip()
    selected_dataset_ids = _clean_string_list(context.get("selected_dataset_ids"))
    curate_dataset_id = str(context.get("curate_dataset_id") or "").strip()
    allow_raw_discovery = bool(context.get("allow_raw_discovery"))
    await_user_input = bool(context.get("await_user_input"))
    post_curation_confirmation = bool(context.get("post_curation_confirmation"))
    normalized = {
        "question": question,
        "selected_dataset_ids": selected_dataset_ids,
        "allow_raw_discovery": allow_raw_discovery,
    }
    if curate_dataset_id:
        normalized["curate_dataset_id"] = curate_dataset_id
    if await_user_input:
        normalized["await_user_input"] = True
    if post_curation_confirmation:
        normalized["post_curation_confirmation"] = True
    return normalized


def _build_plan_state(state) -> Dict[str, Any]:
    pending = state.pending_plan if isinstance(state.pending_plan, dict) else {}
    status = str(pending.get("status") or "none").strip() or "none"
    plan_context = pending.get("plan_context") if isinstance(pending.get("plan_context"), dict) else None
    approved_plan = plan_context if status == "approved" else None
    curation_mode = False
    if isinstance(plan_context, dict):
        curation_mode = bool(plan_context.get("allow_raw_discovery")) or bool(
            str(plan_context.get("curate_dataset_id") or "").strip()
        )
        curation_mode = curation_mode or bool(plan_context.get("post_curation_confirmation"))
    return {
        "status": status,
        "approved_plan": approved_plan,
        "pending_plan_summary": str(pending.get("plan_markdown") or "").strip()[:1200] if status == "awaiting_approval" else "",
        "pending_plan_context": plan_context if status == "awaiting_approval" else None,
        "curation_mode": curation_mode,
    }


def _normalize_dataset_hint(value: str) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "_", str(value or "").upper()).strip("_")
    return normalized


def _resolve_curated_dataset_id(dataset_id: str) -> str:
    raw = str(dataset_id or "").strip()
    if not raw:
        return raw

    if get_curated_dataset(raw) is not None:
        return raw

    normalized = _normalize_dataset_hint(raw)
    alias_hit = CURATED_DATASET_ALIASES.get(normalized)
    if alias_hit and get_curated_dataset(alias_hit) is not None:
        logger.info("Resolved curated dataset alias requested=%s resolved=%s", raw, alias_hit)
        return alias_hit

    candidates = list_curated_datasets()
    normalized_compact = normalized.replace("_", "")
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        candidate_id = str(entry.get("dataset_id") or "").strip()
        candidate_title = str(entry.get("title") or "").strip()
        candidate_id_normalized = _normalize_dataset_hint(candidate_id)
        candidate_title_normalized = _normalize_dataset_hint(candidate_title)
        if normalized == candidate_id_normalized or normalized == candidate_title_normalized:
            if candidate_id:
                logger.info(
                    "Resolved curated dataset exact-normalized requested=%s resolved=%s",
                    raw,
                    candidate_id,
                )
                return candidate_id
        if normalized_compact and (
            normalized_compact == candidate_id_normalized.replace("_", "")
            or normalized_compact == candidate_title_normalized.replace("_", "")
        ):
            if candidate_id:
                logger.info(
                    "Resolved curated dataset compact-normalized requested=%s resolved=%s",
                    raw,
                    candidate_id,
                )
                return candidate_id

    best_entry = None
    best_score = 0
    search_query = normalized.replace("_", " ")
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        score = _score_text_match(
            search_query,
            str(entry.get("dataset_id") or ""),
            str(entry.get("title") or ""),
            str(entry.get("description") or ""),
        )
        if score > best_score:
            best_score = score
            best_entry = entry
    if best_entry is not None and best_score >= 6:
        resolved = str(best_entry.get("dataset_id") or "").strip()
        if resolved:
            logger.info(
                "Resolved curated dataset fuzzy-match requested=%s resolved=%s score=%s",
                raw,
                resolved,
                best_score,
            )
            return resolved

    return raw


def _load_curated_entry(dataset_id: str) -> Dict[str, Any]:
    resolved_dataset_id = _resolve_curated_dataset_id(dataset_id)
    entry = get_curated_dataset(resolved_dataset_id)
    if entry is None:
        available = ", ".join(str(item.get("dataset_id") or "") for item in list_curated_datasets())
        raise RuntimeError(
            f"Unknown curated datasetId '{dataset_id}'. Use catalog first or copy datasetId exactly from tool output. "
            f"Available dataset ids: {available}"
        )
    return entry


def _summarize_tool_input(tool_input: Dict[str, Any]) -> str:
    if not isinstance(tool_input, dict):
        return "invalid tool input"
    parts: List[str] = []
    for key in (
        "action",
        "datasetId",
        "templateId",
        "measureId",
        "dataItemId",
        "searchQuery",
        "query",
        "url",
        "startPeriod",
        "endPeriod",
    ):
        value = tool_input.get(key)
        if value is None or value == "" or value == [] or value == {}:
            continue
        else:
            parts.append(f"{key}={value}")
    artifact_ids = tool_input.get("artifact_ids")
    if isinstance(artifact_ids, list) and artifact_ids:
        parts.append(f"artifact_ids={','.join(str(item) for item in artifact_ids[:6])}")
    code = str(tool_input.get("code") or "").strip()
    if code:
        parts.append(f"code_preview={_truncate(code, 120)}")
    sandbox_request = str(tool_input.get("sandbox_request") or "").strip()
    if sandbox_request:
        parts.append(f"sandbox_request={_truncate(sandbox_request, 160)}")
    return "; ".join(parts) if parts else "no key inputs"


def _build_catalog_payload() -> Dict[str, Any]:
    datasets = []
    for entry in list_curated_datasets():
        datasets.append(
            {
                "dataset_id": entry.get("dataset_id"),
                "title": entry.get("title"),
                "description": entry.get("description"),
                "data_shape": entry.get("data_shape"),
                "curation_source": entry.get("curation_source"),
            }
        )
    return {"datasets": datasets}


def _summarize_catalog_payload(payload: Dict[str, Any]) -> str:
    return _json_text(payload)


def _build_discover_payload(search_query: str, limit: int = 8) -> Dict[str, Any]:
    payload = list_dataflows(force_refresh=False)
    flows = payload.get("dataflows") if isinstance(payload, dict) else []
    candidates: List[Dict[str, Any]] = []
    for item in _to_list(flows):
        if not isinstance(item, dict):
            continue
        entry = {
            "dataset_id": str(item.get("id") or "").strip(),
            "title": str(item.get("name") or item.get("id") or "").strip(),
            "description": str(item.get("description") or "").strip(),
        }
        if not entry["dataset_id"]:
            continue
        if search_query:
            entry["score"] = _score_text_match(
                search_query,
                entry["dataset_id"],
                entry["title"],
                entry["description"],
            )
        else:
            entry["score"] = 0
        candidates.append(entry)
    candidates.sort(key=lambda item: (-int(item.get("score") or 0), item["dataset_id"]))
    trimmed = [
        {
            "dataset_id": item["dataset_id"],
            "title": item["title"],
            "description": item["description"],
        }
        for item in candidates[:limit]
        if search_query == "" or int(item.get("score") or 0) > 0 or len(candidates) <= limit
    ]
    return {
        "search_query": search_query,
        "datasets": trimmed,
    }


def _summarize_discover_payload(payload: Dict[str, Any]) -> str:
    return _json_text(payload)


def _build_raw_metadata_payload(dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    dataflow = metadata.get("dataflow") if isinstance(metadata.get("dataflow"), dict) else {}
    data_structure = metadata.get("dataStructure") if isinstance(metadata.get("dataStructure"), dict) else {}
    dimensions = [
        item
        for item in _to_list(metadata.get("dimensions"))
        if isinstance(item, dict)
    ]
    concepts = [
        item
        for item in _to_list(metadata.get("concepts"))
        if isinstance(item, dict)
    ]
    codelists = {
        str(item.get("id") or "").strip(): item
        for item in _to_list(metadata.get("codelists"))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }

    dimension_rows: List[Dict[str, Any]] = []
    for dimension in sorted(dimensions, key=lambda item: int(item.get("position") or 0)):
        codelist_id = str(dimension.get("codeList") or "").strip()
        codes = codelists.get(codelist_id, {})
        code_values = [
            {
                "code": str(code.get("id") or "").strip(),
                "label": str(code.get("name") or code.get("description") or code.get("id") or "").strip(),
            }
            for code in _to_list(codes.get("codes"))
            if isinstance(code, dict) and str(code.get("id") or "").strip()
        ]
        dimension_rows.append(
            {
                "id": str(dimension.get("id") or "").strip(),
                "name": str(dimension.get("name") or dimension.get("id") or "").strip(),
                "position": int(dimension.get("position") or 0),
                "code_list_id": codelist_id,
                "code_count": len(code_values),
                "sample_codes": code_values[:20],
            }
        )

    return {
        "dataset_id": dataset_id,
        "title": str(dataflow.get("name") or data_structure.get("name") or dataset_id).strip(),
        "description": str(
            dataflow.get("description")
            or data_structure.get("description")
            or dataflow.get("name")
            or data_structure.get("name")
            or dataset_id
        ).strip(),
        "dimension_order": [str(item.get("id") or "").strip() for item in dimension_rows if str(item.get("id") or "").strip()],
        "dimensions": dimension_rows,
        "concepts": [
            {
                "id": str(item.get("id") or "").strip(),
                "name": str(item.get("name") or item.get("description") or item.get("id") or "").strip(),
            }
            for item in concepts[:20]
            if str(item.get("id") or "").strip()
        ],
    }


def _summarize_raw_metadata_payload(payload: Dict[str, Any]) -> str:
    return _json_text(payload)


def _tool_result(summary: str, result_data: Any = None) -> Dict[str, Any]:
    payload = {
        "summary": str(summary or "").strip(),
    }
    if result_data is not None:
        payload["result_data"] = result_data
    return payload


def _normalize_result_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        redirected = parse_qs(parsed.query).get("uddg") or []
        if redirected:
            return redirected[0]
    return url


def _search_web(query: str, max_results: int = 5) -> Dict[str, Any]:
    response = httpx.get(
        WEB_SEARCH_URL,
        params={"q": query},
        headers={
            "User-Agent": WEB_USER_AGENT,
        },
        follow_redirects=True,
        timeout=20,
    )
    response.raise_for_status()
    html = response.text

    links = WEB_RESULT_LINK_RE.findall(html)
    snippets = [_strip_html(item) for item in WEB_RESULT_SNIPPET_RE.findall(html)]
    results: List[Dict[str, Any]] = []

    for index, (raw_url, raw_title) in enumerate(links[:max_results]):
        url = _normalize_result_url(raw_url)
        title = _strip_html(raw_title)
        if not url or not title:
            continue
        domain = urlparse(url).netloc
        results.append(
            {
                "rank": len(results) + 1,
                "title": title,
                "url": url,
                "domain": domain,
                "snippet": snippets[index] if index < len(snippets) else "",
            }
        )

    return {
        "query": query,
        "engine": "duckduckgo_html",
        "results": results,
    }


def _fetch_web_page(url: str) -> Dict[str, Any]:
    response = httpx.get(
        url,
        headers={"User-Agent": WEB_USER_AGENT},
        follow_redirects=True,
        timeout=20,
    )
    response.raise_for_status()
    html = response.text
    title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
    title = _strip_html(title_match.group(1)) if title_match else ""
    cleaned = _strip_html(html)
    final_url = str(response.url)
    return {
        "url": final_url,
        "domain": urlparse(final_url).netloc,
        "title": title or urlparse(final_url).netloc,
        "text_preview": cleaned[:12000],
    }


def _execute_web_search_tool(
    *,
    tool_input: Dict[str, Any],
    state,
    conversation_id: str,
) -> Dict[str, Any]:
    action = str(tool_input.get("action") or "search").strip().lower()
    run_dir = _ensure_runtime_dirs(conversation_id)

    if action == "search":
        query = str(tool_input.get("query") or "").strip()
        if not query:
            raise RuntimeError("web_search_tool action search requires query")
        max_results = int(tool_input.get("maxResults") or 5)
        logger.info(
            'Web search start cid=%s action=search query="%s" max_results=%s',
            conversation_id,
            _truncate(query, 280),
            max(1, min(max_results, 8)),
        )
        payload = _search_web(query, max_results=max(1, min(max_results, 8)))
        results = payload.get("results") or []
        preview = []
        for item in results[:3]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            url = str(item.get("url") or "").strip()
            if title or url:
                preview.append(f"{title} <{url}>".strip())
        logger.info(
            'Web search complete cid=%s action=search query="%s" result_count=%s preview="%s"',
            conversation_id,
            _truncate(query, 280),
            len(results),
            _truncate(" | ".join(preview), 500),
        )
        artifact_path = run_dir / "artifacts" / f"web_search_{len(state.artifacts) + 1:03d}.json"
        _write_json(artifact_path, payload)
        record = _make_artifact_record(
            state=state,
            path=artifact_path,
            kind="web_search_results",
            label=f"Web search: {query}",
            summary=_truncate(f"Web search for '{query}' returned {len(results)} results.", 300),
        )
        return _tool_result(
            (
                f"Web search for '{query}' returned {len(results)} results.\n"
                f"Created artifact: {record['artifact_id']}. Use sandbox to inspect or compare the results."
            ),
            {
                "kind": "web_search",
                "query": query,
                "results": results,
                "artifact_id": record["artifact_id"],
            },
        )

    if action == "fetch":
        url = str(tool_input.get("url") or "").strip()
        if not url:
            raise RuntimeError("web_search_tool action fetch requires url")
        logger.info(
            'Web search start cid=%s action=fetch url="%s"',
            conversation_id,
            _truncate(url, 500),
        )
        payload = _fetch_web_page(url)
        logger.info(
            'Web search complete cid=%s action=fetch url="%s" final_url="%s" title="%s" domain="%s"',
            conversation_id,
            _truncate(url, 500),
            _truncate(str(payload.get("url") or ""), 500),
            _truncate(str(payload.get("title") or ""), 200),
            _truncate(str(payload.get("domain") or ""), 120),
        )
        artifact_path = run_dir / "artifacts" / f"web_page_{len(state.artifacts) + 1:03d}.json"
        _write_json(artifact_path, payload)
        record = _make_artifact_record(
            state=state,
            path=artifact_path,
            kind="web_page",
            label=f"Web page: {payload.get('title') or payload.get('domain')}",
            summary=_truncate(f"Fetched web page {payload.get('url')}.", 300),
        )
        return _tool_result(
            (
                f"Fetched web page {payload.get('domain') or payload.get('url')}.\n"
                f"Created artifact: {record['artifact_id']}. Use sandbox to inspect the extracted text."
            ),
            {
                "kind": "web_page",
                "url": payload.get("url"),
                "domain": payload.get("domain"),
                "title": payload.get("title"),
                "text_preview": payload.get("text_preview"),
                "artifact_id": record["artifact_id"],
            },
        )

    raise RuntimeError(f"Unsupported web_search_tool action: {action}")


def _build_structure_payload(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "dataset_id": entry.get("dataset_id"),
        "title": entry.get("title"),
        "description": entry.get("description"),
        "data_shape": entry.get("data_shape"),
        "curation_source": entry.get("curation_source"),
        "data_structure": entry.get("data_structure"),
        "query_templates": entry.get("query_templates"),
    }


def _summarize_structure_payload(payload: Dict[str, Any]) -> str:
    return _json_text(payload)


def _normalize_filters(raw_filters: Any) -> Dict[str, List[str]]:
    if raw_filters is None:
        return {}
    if not isinstance(raw_filters, dict):
        raise RuntimeError("abs_data_tool retrieve filters must be an object keyed by dimension id")

    normalized: Dict[str, List[str]] = {}
    for dim_id, values in raw_filters.items():
        key = str(dim_id or "").strip()
        if not key:
            continue
        normalized_values = _clean_string_list(values)
        if normalized_values:
            normalized[key] = normalized_values
    return normalized


def _build_data_key(
    *,
    dimension_order: List[str],
    filters: Dict[str, List[str]],
) -> str:
    if not dimension_order:
        return "all"
    parts: List[str] = []
    for dim_id in dimension_order:
        selected = filters.get(dim_id) or []
        parts.append("+".join(selected))
    if all(part == "" for part in parts):
        return "all"
    return ".".join(parts)


def _parse_template_api_call(api_call: str) -> Dict[str, Any]:
    raw = str(api_call or "").strip()
    if not raw:
        raise RuntimeError("Selected query template is missing api_call")

    parsed = urlparse(raw)
    path = parsed.path or raw.split("?", 1)[0]
    match = re.match(r"^/rest/data/([^/]+)/([^/?]+)$", path)
    if not match:
        raise RuntimeError(f"Unsupported template api_call format: {raw}")

    query = parse_qs(parsed.query, keep_blank_values=True)
    return {
        "dataset_id": match.group(1),
        "data_key": match.group(2),
        "detail": (query.get("detail") or [None])[0],
        "dimension_at_observation": (query.get("dimensionAtObservation") or [None])[0],
        "start_period": (query.get("startPeriod") or [None])[0],
        "end_period": (query.get("endPeriod") or [None])[0],
    }


def _find_measure_entry(template: Dict[str, Any], measure_id: str) -> Optional[Dict[str, Any]]:
    target = str(measure_id or "").strip()
    if not target:
        return None
    for item in _to_list(template.get("measures")):
        if not isinstance(item, dict):
            continue
        if str(item.get("measure_id") or "").strip() == target:
            return dict(item)
    return None


def _find_data_item_entry(template: Dict[str, Any], data_item_id: str) -> Optional[Dict[str, Any]]:
    target = str(data_item_id or "").strip()
    if not target:
        return None
    for item in _to_list(template.get("data_items")):
        if not isinstance(item, dict):
            continue
        if str(item.get("data_item_id") or "").strip() == target:
            return dict(item)
    return None


def _load_query_template(entry: Dict[str, Any], template_id: str) -> Dict[str, Any]:
    target = str(template_id or "").strip()
    for template in _to_list(entry.get("query_templates")):
        if not isinstance(template, dict):
            continue
        if str(template.get("template_id") or "").strip() == target:
            return dict(template)
    available = ", ".join(
        str(item.get("template_id") or "").strip()
        for item in _to_list(entry.get("query_templates"))
        if isinstance(item, dict)
    )
    raise RuntimeError(
        f"Unknown query template '{template_id}' for dataset {entry.get('dataset_id')}. "
        f"Available templates: {available}"
    )


def _resolve_query_template(
    entry: Dict[str, Any],
    template_id: str,
    measure_id: str,
    data_item_id: str,
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    target_template_id = str(template_id or "").strip()
    target_measure_id = str(measure_id or "").strip()
    target_data_item_id = str(data_item_id or "").strip()

    if target_template_id:
        template = _load_query_template(entry, target_template_id)
        measure_entry = _find_measure_entry(template, target_measure_id)
        data_item_entry = _find_data_item_entry(template, target_data_item_id)
        return template, measure_entry, data_item_entry

    if target_measure_id:
        for template in _to_list(entry.get("query_templates")):
            if not isinstance(template, dict):
                continue
            measure_entry = _find_measure_entry(template, target_measure_id)
            if measure_entry is not None:
                return dict(template), measure_entry, None

    if target_data_item_id:
        for template in _to_list(entry.get("query_templates")):
            if not isinstance(template, dict):
                continue
            data_item_entry = _find_data_item_entry(template, target_data_item_id)
            if data_item_entry is not None:
                return dict(template), None, data_item_entry

    raise RuntimeError("abs_data_tool retrieve requires templateId, measureId or dataItemId")


def _materialize_template_api_call(
    template: Dict[str, Any],
    measure_entry: Optional[Dict[str, Any]],
    data_item_entry: Optional[Dict[str, Any]],
) -> str:
    api_call = str(template.get("api_call") or "").strip()
    if "{MEASURE}" in api_call:
        measure_id = str((measure_entry or {}).get("measure_id") or "").strip()
        if not measure_id:
            raise RuntimeError(
                "This curated template requires a measureId so the harness can substitute the MEASURE placeholder."
            )
        api_call = api_call.replace("{MEASURE}", measure_id)

    if "{DATA_ITEM}" in api_call:
        data_item_id = str((data_item_entry or {}).get("data_item_id") or "").strip()
        if not data_item_id:
            raise RuntimeError(
                "This curated template requires a dataItemId so the harness can substitute the DATA_ITEM placeholder."
            )
        api_call = api_call.replace("{DATA_ITEM}", data_item_id)

    return api_call


def _bridge_error_status_code(exc: Exception) -> Optional[int]:
    if not isinstance(exc, MCPBridgeError):
        return None
    match = re.search(r"status code (\d+)", f"{exc.stderr}\n{exc}")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _fallback_tstest_filters(filters: Dict[str, List[str]]) -> List[Dict[str, List[str]]]:
    selected = filters.get("TSEST") or []
    if len(selected) != 1:
        return []
    current = selected[0]
    fallbacks: List[str] = []
    if current == "30":
        fallbacks = ["20", "10"]
    elif current == "20":
        fallbacks = ["10"]
    else:
        return []

    attempts: List[Dict[str, List[str]]] = []
    for code in fallbacks:
        updated = {key: list(value) for key, value in filters.items()}
        updated["TSEST"] = [code]
        attempts.append(updated)
    return attempts


def _build_retrieval_summary(
    *,
    dataset_id: str,
    entry: Dict[str, Any],
    resolved_payload: Dict[str, Any],
    filters: Dict[str, List[str]],
    data_key: str,
    artifact_id: str,
    fallback_note: str = "",
) -> str:
    series = resolved_payload.get("series") if isinstance(resolved_payload, dict) else []
    observation_count = resolved_payload.get("observationCount") if isinstance(resolved_payload, dict) else None
    lines = [
        f"Retrieved and resolved {entry['title']} datasetId {dataset_id}.",
        f"Observation count: {observation_count if observation_count is not None else 'unknown'}.",
        f"Series count: {len(series) if isinstance(series, list) else 0}.",
        f"Applied data key: {data_key}.",
    ]
    retrieval_query = resolved_payload.get("query") if isinstance(resolved_payload, dict) else {}
    if isinstance(retrieval_query, dict):
        start_period = str(retrieval_query.get("startPeriod") or "").strip()
        end_period = str(retrieval_query.get("endPeriod") or "").strip()
        if start_period or end_period:
            lines.append(
                f"Applied time range: {start_period or 'open'} to {end_period or 'open'}."
            )
    if filters:
        filter_parts = [
            f"{dim_id}={'+'.join(values)}"
            for dim_id, values in filters.items()
        ]
        lines.append(f"Applied filters: {', '.join(filter_parts)}.")
    if fallback_note:
        lines.append(fallback_note)
    dimension_labels = resolved_payload.get("dimensions") if isinstance(resolved_payload, dict) else {}
    if isinstance(dimension_labels, dict):
        lines.append(
            "Resolved dimensions: "
            + ", ".join(list(dimension_labels.keys())[:8])
            + "."
        )
    lines.append(
        f"Created artifact: {artifact_id}. Use the sandbox tool to inspect, filter, aggregate or calculate."
    )
    return "\n".join(lines)


def _execute_abs_data_tool(
    *,
    tool_input: Dict[str, Any],
    state,
    conversation_id: str,
) -> Dict[str, Any]:
    action = str(tool_input.get("action") or "").strip()
    if action not in {"catalog", "structure", "retrieve", "discover", "metadata", "raw_retrieve"}:
        raise RuntimeError(f"Unsupported abs_data_tool action: {action}")

    dataset_id = str(tool_input.get("datasetId") or "").strip()

    if action == "catalog":
        payload = _build_catalog_payload()
        return _tool_result(
            _summarize_catalog_payload(payload),
            {
                "kind": "catalog",
                "datasets": payload.get("datasets") or [],
            },
        )

    approved_plan = (
        state.pending_plan.get("plan_context")
        if isinstance(state.pending_plan, dict)
        and str(state.pending_plan.get("status") or "") == "approved"
        and isinstance(state.pending_plan.get("plan_context"), dict)
        else {}
    )

    if action == "discover":
        if not bool(approved_plan.get("allow_raw_discovery")):
            raise RuntimeError(
                "Raw ABS discovery requires prior user approval. Propose a plan asking for permission first."
            )
        search_query = str(tool_input.get("searchQuery") or "").strip()
        payload = _build_discover_payload(search_query)
        return _tool_result(
            _summarize_discover_payload(payload),
            {
                "kind": "discover",
                "search_query": search_query,
                "datasets": payload.get("datasets") or [],
            },
        )

    if action == "metadata":
        if not bool(approved_plan.get("allow_raw_discovery")):
            raise RuntimeError(
                "Raw ABS metadata inspection requires prior user approval. Propose a plan asking for permission first."
            )
        if not dataset_id:
            raise RuntimeError("abs_data_tool action metadata requires datasetId")
        metadata = get_dataflow_metadata(dataset_id, force_refresh=True)
        if not isinstance(metadata, dict):
            raise RuntimeError(f"Live ABS metadata for {dataset_id} was not an object")
        payload = _build_raw_metadata_payload(dataset_id, metadata)
        return _tool_result(
            _summarize_raw_metadata_payload(payload),
            {
                "kind": "raw_metadata",
                "dataset_id": payload.get("dataset_id"),
                "title": payload.get("title"),
                "description": payload.get("description"),
                "dimension_order": payload.get("dimension_order") or [],
                "dimensions": payload.get("dimensions") or [],
                "concepts": payload.get("concepts") or [],
            },
        )

    if not dataset_id:
        raise RuntimeError(f"abs_data_tool action {action} requires datasetId")

    if action == "raw_retrieve":
        if not bool(approved_plan.get("allow_raw_discovery")):
            raise RuntimeError(
                "Raw ABS retrieval requires prior user approval. Propose a plan asking for permission first."
            )
        data_key = str(tool_input.get("dataKey") or "").strip()
        if not data_key:
            raise RuntimeError("abs_data_tool action raw_retrieve requires dataKey")
        run_dir = _ensure_runtime_dirs(conversation_id)
        start_period = str(tool_input.get("startPeriod") or "").strip() or None
        end_period = str(tool_input.get("endPeriod") or "").strip() or None
        detail = str(tool_input.get("detail") or "").strip() or "full"
        dimension_at_observation = (
            str(tool_input.get("dimensionAtObservation") or "").strip()
            or "TIME_PERIOD"
        )
        try:
            resolved_payload = resolve_dataset(
                dataset_id=dataset_id,
                data_key=data_key,
                start_period=start_period,
                end_period=end_period,
                detail=detail,
                dimension_at_observation=dimension_at_observation,
            )
        except Exception as exc:
            status_code = _bridge_error_status_code(exc)
            if status_code == 404:
                raise RuntimeError("ABS returned no data for that raw ABS call.") from exc
            raise

        artifact_payload = {
            "artifact_type": "resolved_abs_dataset",
            "catalog_entry": {
                "datasetId": dataset_id,
                "title": dataset_id,
            },
            "retrieval": {
                "datasetId": dataset_id,
                "dataKey": data_key,
                "startPeriod": start_period,
                "endPeriod": end_period,
                "detail": detail,
                "dimensionAtObservation": dimension_at_observation,
                "rawDiscovery": True,
            },
            "resolved_dataset": resolved_payload,
        }
        artifact_path = run_dir / "artifacts" / f"raw_retrieve_{len(state.artifacts) + 1:03d}.json"
        _write_json(artifact_path, artifact_payload)
        record = _make_artifact_record(
            state=state,
            path=artifact_path,
            kind="abs_resolved_dataset",
            label=f"ABS raw resolved {dataset_id}",
            summary=_truncate(
                f"Resolved raw ABS dataset {dataset_id} with {resolved_payload.get('observationCount', 'unknown')} observations.",
                300,
            ),
        )
        return _tool_result(
            (
                f"Retrieved raw ABS dataset {dataset_id}.\n"
                f"Observation count: {resolved_payload.get('observationCount', 'unknown')}.\n"
                f"Series count: {len(resolved_payload.get('series') or []) if isinstance(resolved_payload, dict) else 0}.\n"
                f"Applied data key: {data_key}.\n"
                f"Created artifact: {record['artifact_id']}. Use the sandbox tool to inspect or verify the wildcard retrieval."
            ),
            {
                "kind": "raw_retrieve",
                "dataset_id": dataset_id,
                "data_key": data_key,
                "observation_count": resolved_payload.get("observationCount") if isinstance(resolved_payload, dict) else None,
                "series_count": len(resolved_payload.get("series") or []) if isinstance(resolved_payload, dict) else None,
                "artifact_id": record["artifact_id"],
            },
        )

    run_dir = _ensure_runtime_dirs(conversation_id)
    entry = _load_curated_entry(dataset_id)

    if action == "structure":
        payload = _build_structure_payload(entry)
        return _tool_result(
            _summarize_structure_payload(payload),
            {
                "kind": "structure",
                "dataset_id": payload.get("dataset_id"),
                "title": payload.get("title"),
                "description": payload.get("description"),
                "data_shape": payload.get("data_shape"),
                "curation_source": payload.get("curation_source"),
                "data_structure": payload.get("data_structure"),
                "query_templates": payload.get("query_templates") or [],
            },
        )

    template_id = str(tool_input.get("templateId") or "").strip()
    measure_id = str(tool_input.get("measureId") or "").strip()
    data_item_id = str(tool_input.get("dataItemId") or "").strip()

    overrides = _normalize_filters(tool_input.get("filters"))
    if "TIME_PERIOD" in overrides:
        raise RuntimeError("Use startPeriod/endPeriod instead of a TIME_PERIOD filter")
    template, measure_entry, data_item_entry = _resolve_query_template(
        entry,
        template_id,
        measure_id,
        data_item_id,
    )
    if overrides:
        raise RuntimeError(
            "This curated template must be used as-is. Retrieve it exactly, then narrow the data in sandbox."
        )
    materialized_api_call = _materialize_template_api_call(template, measure_entry, data_item_entry)
    parsed_call = _parse_template_api_call(materialized_api_call)
    data_key = str(parsed_call.get("data_key") or "").strip() or "all"
    start_period = str(tool_input.get("startPeriod") or "").strip() or str(parsed_call.get("start_period") or "").strip() or None
    end_period = str(tool_input.get("endPeriod") or "").strip() or str(parsed_call.get("end_period") or "").strip() or None
    detail = str(tool_input.get("detail") or "").strip() or str(parsed_call.get("detail") or "full")
    dimension_at_observation = (
        str(tool_input.get("dimensionAtObservation") or "").strip()
        or str(parsed_call.get("dimension_at_observation") or "TIME_PERIOD")
    )
    dataset_id = str(parsed_call.get("dataset_id") or entry.get("dataset_id") or dataset_id).strip()

    fallback_note = ""
    active_filters: Dict[str, List[str]] = {}
    active_data_key = data_key
    try:
        resolved_payload = resolve_dataset(
            dataset_id=dataset_id,
            data_key=active_data_key,
            start_period=start_period,
            end_period=end_period,
            detail=detail,
            dimension_at_observation=dimension_at_observation,
        )
    except Exception as exc:
        status_code = _bridge_error_status_code(exc)
        if status_code == 404:
            raise RuntimeError(
                "ABS returned no data for that curated template call."
            ) from exc
        raise

    artifact_payload = {
        "artifact_type": "resolved_abs_dataset",
        "catalog_entry": {
            "datasetId": entry["dataset_id"],
            "title": entry["title"],
        },
        "retrieval": {
            "datasetId": dataset_id,
            "templateId": str(template.get("template_id") or template_id).strip(),
            "measureId": str((measure_entry or {}).get("measure_id") or measure_id).strip(),
            "dataItemId": str((data_item_entry or {}).get("data_item_id") or data_item_id).strip(),
            "filters": active_filters,
            "dataKey": active_data_key,
            "startPeriod": start_period,
            "endPeriod": end_period,
            "detail": detail,
            "dimensionAtObservation": dimension_at_observation,
            "apiCall": materialized_api_call,
        },
        "resolved_dataset": resolved_payload,
    }

    artifact_path = run_dir / "artifacts" / f"retrieve_{len(state.artifacts) + 1:03d}.json"
    _write_json(artifact_path, artifact_payload)
    summary_preview = (
        f"Resolved ABS dataset {dataset_id} for {entry['title']} with "
        f"{resolved_payload.get('observationCount', 'unknown')} observations."
    )
    record = _make_artifact_record(
        state=state,
        path=artifact_path,
        kind="abs_resolved_dataset",
        label=f"ABS resolved {dataset_id}",
        summary=summary_preview,
    )
    return _tool_result(
        _build_retrieval_summary(
            dataset_id=dataset_id,
            entry=entry,
            resolved_payload=resolved_payload if isinstance(resolved_payload, dict) else {},
            filters=active_filters,
            data_key=active_data_key,
            artifact_id=record["artifact_id"],
            fallback_note=fallback_note,
        ),
        {
            "kind": "retrieve",
            "dataset_id": dataset_id,
            "catalog_dataset_id": entry.get("dataset_id"),
            "title": entry.get("title"),
            "retrieval": artifact_payload.get("retrieval"),
            "observation_count": resolved_payload.get("observationCount") if isinstance(resolved_payload, dict) else None,
            "series_count": len(resolved_payload.get("series") or []) if isinstance(resolved_payload, dict) else None,
            "artifact_id": record["artifact_id"],
        },
    )


def _summarize_sandbox_result(result: Dict[str, Any], created_ids: List[str]) -> str:
    lines: List[str] = []
    stdout = str(result.get("stdout") or "").strip()
    if stdout:
        lines.append(f"stdout: {_truncate(stdout, 300)}")

    value = result.get("result")
    if isinstance(value, dict):
        rendered = _json_text(value)
        if len(rendered) <= 35000:
            lines.append(f"result_json:\n{rendered}")
        else:
            lines.append(f"result: object with keys {list(value.keys())[:20]}")
            lines.append(f"result_json_preview:\n{rendered[:12000]}")
    elif isinstance(value, list):
        rendered = _json_text(value)
        if len(rendered) <= 35000:
            lines.append(f"result_json:\n{rendered}")
        else:
            lines.append(f"result: list length {len(value)}")
            lines.append(f"result_json_preview:\n{rendered[:12000]}")
    elif value is not None:
        lines.append(f"result: {_truncate(value, 300)}")

    if created_ids:
        lines.append(f"created artifacts: {', '.join(created_ids)}")

    if not lines:
        lines.append("Sandbox completed without a structured result.")
    return "\n".join(lines)


def _create_sandbox_artifacts(
    *,
    state,
    conversation_id: str,
    runner_result: Dict[str, Any],
) -> List[str]:
    run_dir = _ensure_runtime_dirs(conversation_id)
    created_ids: List[str] = []

    raw_result = runner_result.get("result")
    if isinstance(raw_result, (dict, list)):
        path = run_dir / "artifacts" / f"sandbox_result_{len(state.artifacts) + 1:03d}.json"
        _write_json(path, raw_result)
        record = _make_artifact_record(
            state=state,
            path=path,
            kind="sandbox_result",
            label="Sandbox result",
            summary=_truncate(json.dumps(raw_result, ensure_ascii=False), 600),
        )
        created_ids.append(record["artifact_id"])

    for created in runner_result.get("created_artifacts") or []:
        if not isinstance(created, dict):
            continue
        path_value = created.get("path")
        if not isinstance(path_value, str):
            continue
        path = Path(path_value)
        if not path.exists():
            continue
        summary = f"Sandbox created file {path.name}."
        record = _make_artifact_record(
            state=state,
            path=path,
            kind=str(created.get("kind") or "sandbox_output"),
            label=f"Sandbox {created.get('name') or path.name}",
            summary=summary,
        )
        created_ids.append(record["artifact_id"])

    return created_ids


def _validate_sandbox_artifact_references(code: str, artifact_ids: List[str]) -> None:
    referenced = sorted(set(re.findall(r"\bartifact-\d{3}\b", str(code or ""))))
    if not referenced:
        return
    allowed = {str(item).strip() for item in artifact_ids if str(item).strip()}
    invalid = [artifact_id for artifact_id in referenced if artifact_id not in allowed]
    if invalid:
        raise RuntimeError(
            "sandbox_tool code referenced artifact ids that were not passed in artifact_ids: "
            + ", ".join(invalid)
        )


def _lint_sandbox_code(code: str) -> Optional[str]:
    source = str(code or "")

    invented_artifact_helper = re.search(
        r"\b(create_artifact|save_artifact|make_artifact|create_json_artifact|create_text_artifact)\s*\(",
        source,
        re.IGNORECASE,
    )
    if invented_artifact_helper:
        return (
            "The sandbox step called an artifact helper that does not exist. "
            "Do not invent helper names. If you need to save output, use save_json(...) or save_text(...). "
            "Otherwise write normal Python."
        )

    positional_filter_rows = re.search(
        r"\bfilter_rows\s*\(\s*[^,\n\)]+\s*,\s*[^A-Za-z_\n\)][^,\n\)]*",
        source,
        re.IGNORECASE,
    )
    if positional_filter_rows:
        return (
            "The sandbox step called filter_rows(...) with unsupported positional arguments. "
            "Use filter_rows(rows, FIELD_code='X', OTHER_code='Y') with keyword equality filters only. "
            "If you need more complex filtering, write normal Python."
        )

    brittle_descending_sort = re.search(
        r"key\s*=\s*lambda\s+[^:]+:\s*-\s*[^\n,)]{3,}",
        source,
        re.IGNORECASE,
    )
    if brittle_descending_sort:
        return (
            "The sandbox step uses a handwritten descending lambda with unary minus. "
            "That pattern is brittle on ABS rows because fields may be nested or non-numeric. "
            "Use sort_by_numeric(...), top_n_by_numeric(...), or extract numbers with get_numeric()/coerce_number() first."
        )

    custom_uniq = re.search(r"^\s*def\s+uniq\s*\(", source, re.IGNORECASE | re.MULTILINE)
    if custom_uniq:
        return (
            "The sandbox step defines a custom uniq helper. "
            "Prefer distinct_values(...), group_rows(...), or index_rows(...) instead of handwritten dedupe logic."
        )

    raw_value_arithmetic = re.search(
        r"(\.get\(\s*['\"]value['\"]\s*\)|\[['\"]value['\"]\]|get_value\s*\([^)]+\)|get_numeric\s*\([^)]+\))\s*[-+]\s*"
        r"(\.get\(\s*['\"]value['\"]\s*\)|\[['\"]value['\"]\]|get_value\s*\([^)]+\)|get_numeric\s*\([^)]+\))",
        source,
        re.IGNORECASE,
    )
    if raw_value_arithmetic:
        return (
            "The sandbox step performs raw arithmetic on potentially nullable ABS values. "
            "Use safe_float()/get_value()/get_numeric() first and guard missing values before + or -."
        )

    return None


def _generate_sandbox_code(
    *,
    tool_input: Dict[str, Any],
    state,
    loop_payload: Dict[str, Any],
) -> str:
    code = str(tool_input.get("code") or "").strip()
    if code:
        return code

    sandbox_request = str(tool_input.get("sandbox_request") or "").strip()
    if not sandbox_request:
        raise RuntimeError("sandbox_tool requires either code or sandbox_request")

    artifact_ids = [
        str(item).strip()
        for item in (tool_input.get("artifact_ids") or [])
        if isinstance(item, str) and item.strip()
    ]

    logger.info(
        'Sandbox codegen request brief="%s" artifacts=%s',
        _truncate(sandbox_request, 280),
        artifact_ids,
    )

    try:
        generated = _call_model_text(
            build_sandbox_codegen_messages(
                payload=loop_payload,
                sandbox_request=sandbox_request,
                artifact_ids=artifact_ids,
            ),
            reasoning_effort="low",
        ).strip()
    except Exception as exc:
        logger.exception(
            'Sandbox codegen failed brief="%s" artifacts=%s error=%s',
            _truncate(sandbox_request, 280),
            artifact_ids,
            exc,
        )
        raise RuntimeError(
            "Sandbox code generation failed. "
            f"Brief: {_truncate(sandbox_request, 220)}. "
            f"Artifacts: {artifact_ids}. "
            f"Underlying error: {exc}"
        ) from exc
    if generated.startswith("```"):
        generated = re.sub(r"^```(?:python)?\s*", "", generated)
        generated = re.sub(r"\s*```$", "", generated)
        generated = generated.strip()
    if not generated:
        raise RuntimeError("Sandbox code generator returned empty code.")
    logger.info(
        'Sandbox codegen output preview="%s"',
        _truncate(generated, 280),
    )
    return generated


def _execute_sandbox_tool(
    *,
    tool_input: Dict[str, Any],
    state,
    conversation_id: str,
    loop_payload: Dict[str, Any],
    status_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    status_callback = status_callback or (lambda _message: None)
    artifact_ids = tool_input.get("artifact_ids")
    code = _generate_sandbox_code(tool_input=tool_input, state=state, loop_payload=loop_payload)
    tool_input["code"] = code
    status_callback("Code generated.")

    if not isinstance(artifact_ids, list) or not all(isinstance(item, str) and item.strip() for item in artifact_ids):
        raise RuntimeError("sandbox_tool requires a non-empty artifact_ids array of strings")
    _validate_sandbox_artifact_references(code, artifact_ids)
    lint_error = _lint_sandbox_code(code)
    if lint_error:
        raise RuntimeError(lint_error)
    try:
        compile(code, "<sandbox>", "exec")
    except SyntaxError as exc:
        raise RuntimeError(
            "Sandbox code generation produced invalid Python. "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc

    allowed_artifacts = {
        artifact["artifact_id"]: artifact
        for artifact in state.artifacts
        if isinstance(artifact, dict) and artifact.get("artifact_id") in artifact_ids
    }
    if len(allowed_artifacts) != len(artifact_ids):
        raise RuntimeError("sandbox_tool requested unknown artifact ids")

    run_dir = _ensure_runtime_dirs(conversation_id)
    payload_path = run_dir / "sandbox" / f"payload_{len(state.loop_history) + 1:03d}.json"
    output_dir = run_dir / "sandbox" / f"output_{len(state.loop_history) + 1:03d}"
    payload = {
        "code": code,
        "output_dir": str(output_dir),
        "artifacts": allowed_artifacts,
    }
    _write_json(payload_path, payload)

    runner_path = Path(__file__).resolve().parent / "harness" / "sandbox_runner.py"
    completed = subprocess.run(
        [settings.python_binary, str(runner_path), str(payload_path)],
        capture_output=True,
        text=True,
        cwd=str(run_dir),
        timeout=90,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Sandbox execution failed:\n"
            f"STDOUT: {completed.stdout[:1000]}\nSTDERR: {completed.stderr[:1000]}"
        )
    status_callback("Code run.")

    try:
        runner_result = json.loads(completed.stdout.strip())
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Sandbox returned invalid JSON: {completed.stdout[:1000]}"
        ) from exc

    if runner_result.get("error"):
        raise RuntimeError(f"Sandbox error:\n{runner_result['error']}")

    created_ids = _create_sandbox_artifacts(
        state=state,
        conversation_id=conversation_id,
        runner_result=runner_result,
    )
    return _tool_result(
        _summarize_sandbox_result(runner_result, created_ids),
        {
            "kind": "sandbox",
            "result": runner_result.get("result"),
            "stdout": str(runner_result.get("stdout") or "").strip()[:4000],
            "created_artifact_ids": created_ids,
            "generated_code_preview": code[:600],
        },
    )


def _build_loop_handoff_summary(
    *,
    step: Dict[str, Any],
    progress_note: str,
    result_summary: str,
    result_data: Any = None,
) -> str:
    step_id = str(step.get("id") or "").strip()
    attempt = str(step.get("summary") or progress_note or "").strip()
    if not attempt:
        attempt = f"Ran step {step_id or 'unknown'}."

    result_dict = result_data if isinstance(result_data, dict) else {}
    kind = str(result_dict.get("kind") or "").strip()

    outcome = "Completed."
    known = str(result_summary or "").strip()
    remaining = ""

    if kind == "tool_error":
        error_text = str(result_dict.get("error") or "").strip()
        retry_guidance = str(result_dict.get("retry_guidance") or "").strip()
        outcome = "Failed."
        known = error_text or "The step did not complete successfully."
        remaining = retry_guidance or "Choose a safer next step using the failure details."
    elif kind in {"retrieve", "raw_retrieve"}:
        dataset_id = str(result_dict.get("dataset_id") or "").strip()
        artifact_id = str(result_dict.get("artifact_id") or "").strip()
        observation_count = result_dict.get("observation_count")
        series_count = result_dict.get("series_count")
        outcome_bits = [f"Retrieved {dataset_id}." if dataset_id else "Retrieved the dataset."]
        if observation_count is not None:
            outcome_bits.append(f"Observations: {observation_count}.")
        if series_count is not None:
            outcome_bits.append(f"Series: {series_count}.")
        outcome = " ".join(outcome_bits)
        known_bits = []
        if artifact_id:
            known_bits.append(f"Artifact available: {artifact_id}.")
        retrieval = result_dict.get("retrieval") if isinstance(result_dict.get("retrieval"), dict) else {}
        template_id = str(retrieval.get("templateId") or "").strip()
        data_item_id = str(retrieval.get("dataItemId") or "").strip()
        measure_id = str(retrieval.get("measureId") or "").strip()
        if template_id:
            known_bits.append(f"Template: {template_id}.")
        if data_item_id:
            known_bits.append(f"Data item: {data_item_id}.")
        if measure_id:
            known_bits.append(f"Measure: {measure_id}.")
        known = " ".join(known_bits) or known
        remaining = "Inspect or narrow the retrieved artifact before further analysis."
    elif kind == "structure":
        dataset_id = str(result_dict.get("dataset_id") or "").strip()
        data_shape = str(result_dict.get("data_shape") or "").strip()
        template_count = len(result_dict.get("query_templates") or []) if isinstance(result_dict.get("query_templates"), list) else 0
        outcome = f"Inspected structure for {dataset_id or 'the dataset'}."
        known_bits = []
        if data_shape:
            known_bits.append(f"Data shape: {data_shape}.")
        if template_count:
            known_bits.append(f"Templates available: {template_count}.")
        known = " ".join(known_bits) or known
        remaining = "Choose the exact retrieval path from the inspected structure."
    elif kind == "catalog":
        datasets = result_dict.get("datasets") if isinstance(result_dict.get("datasets"), list) else []
        outcome = "Reviewed the curated catalog."
        known = f"Candidate datasets found: {len(datasets)}." if datasets else (known or "No catalog candidates were recorded.")
        remaining = "Pick the most relevant dataset and inspect its structure."
    elif kind == "discover":
        datasets = result_dict.get("datasets") if isinstance(result_dict.get("datasets"), list) else []
        outcome = "Reviewed broader ABS discovery results."
        known = f"Candidate datasets found: {len(datasets)}." if datasets else (known or "No discovery candidates were recorded.")
        remaining = "Inspect metadata for the best candidate before raw retrieval."
    elif kind == "raw_metadata":
        dataset_id = str(result_dict.get("dataset_id") or "").strip()
        dims = result_dict.get("dimension_order") if isinstance(result_dict.get("dimension_order"), list) else []
        outcome = f"Inspected metadata for {dataset_id or 'the dataset'}."
        known = f"Dimension order: {', '.join(str(item) for item in dims[:8])}." if dims else known
        remaining = "Choose the exact raw data key from the metadata."
    elif kind == "sandbox":
        created_ids = result_dict.get("created_artifact_ids") if isinstance(result_dict.get("created_artifact_ids"), list) else []
        sandbox_result = result_dict.get("result")
        outcome = "Sandbox step completed."
        if isinstance(sandbox_result, dict):
            keys = [str(key) for key in list(sandbox_result.keys())[:8]]
            known_bits = [f"Result keys: {', '.join(keys)}."] if keys else []
            if created_ids:
                known_bits.append(f"Created artifacts: {', '.join(str(item) for item in created_ids[:4])}.")
            known = " ".join(known_bits) or known
        elif isinstance(sandbox_result, list):
            known = f"Result list length: {len(sandbox_result)}."
            if created_ids:
                known += f" Created artifacts: {', '.join(str(item) for item in created_ids[:4])}."
        elif created_ids:
            known = f"Created artifacts: {', '.join(str(item) for item in created_ids[:4])}."
        remaining = "Use this result to decide whether the next step is more narrowing or the final calculation."
    elif kind in {"web_search", "web_page"}:
        outcome = "Gathered supporting web context."
        remaining = "Use it only as supporting context, with ABS evidence primary."
    elif kind in {"model_call_error", "harness_parse_error"}:
        outcome = "The loop failed before a valid decision completed."
        remaining = "Retry with a valid loop decision and stricter output formatting."

    sections = [
        f"Tried: {_truncate(attempt, 240)}",
        f"Outcome: {_truncate(outcome, 320)}",
    ]
    if known:
        sections.append(f"Known now: {_truncate(known, 480)}")
    if remaining:
        sections.append(f"Still unresolved: {_truncate(remaining, 320)}")
    return "\n".join(sections)


def _record_loop_feedback(
    state,
    *,
    step: Dict[str, Any],
    progress_note: str,
    result_summary: str,
    result_data: Any = None,
) -> None:
    handoff_summary = _build_loop_handoff_summary(
        step=step,
        progress_note=progress_note,
        result_summary=result_summary,
        result_data=result_data,
    )
    entry = {
        "step": step,
        "progress_note": progress_note,
        "handoff_summary": handoff_summary,
        "result_summary": str(result_summary or "").strip()[:12000],
    }
    if result_data is not None:
        entry["result_data"] = result_data
    state.loop_history.append(entry)


def _normalize_sandbox_code(code: Any) -> str:
    if not isinstance(code, str):
        return ""
    normalized_lines = []
    for raw_line in code.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized_lines.append(line)
    return re.sub(r"\s+", " ", "\n".join(normalized_lines)).strip()


def _recent_failed_sandbox_entry(state) -> Optional[Dict[str, Any]]:
    for item in reversed(state.loop_history):
        if not isinstance(item, dict):
            continue
        step = item.get("step") if isinstance(item.get("step"), dict) else {}
        if str(step.get("id") or "").strip() != "use_sandbox_tool":
            continue
        result_data = item.get("result_data") if isinstance(item.get("result_data"), dict) else {}
        if str(result_data.get("kind") or "").strip() != "tool_error":
            continue
        if str(result_data.get("tool_step_id") or "").strip() != "use_sandbox_tool":
            continue
        return item
    return None


def _classify_tool_failure(step_id: str, error_text: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
    clean_error = str(error_text or "").strip()
    tool_name = {
        "use_abs_data_tool": "abs_data_tool",
        "use_web_search_tool": "web_search_tool",
        "use_sandbox_tool": "sandbox_tool",
    }.get(step_id, "")

    result = {
        "kind": "tool_error",
        "tool_step_id": step_id,
        "tool_name": tool_name,
        "error": clean_error[:4000],
    }

    if step_id != "use_sandbox_tool":
        result["retry_guidance"] = "Choose a different valid step or adjust the tool input to address the reported error."
        return result

    sandbox_code = str(tool_input.get("code") or "")
    sandbox_request = str(tool_input.get("sandbox_request") or "")
    result["sandbox_code_preview"] = sandbox_code[:600]
    result["sandbox_code_normalized"] = _normalize_sandbox_code(sandbox_code)[:2000]
    result["sandbox_request"] = sandbox_request[:1000]
    result["artifact_ids"] = [
        str(item).strip()
        for item in (tool_input.get("artifact_ids") or [])
        if isinstance(item, str) and item.strip()
    ]

    lowered = clean_error.lower()
    if "nonetype' object is not subscriptable" in lowered:
        result["error_class"] = "missing_match_or_null_lookup"
        result["retry_guidance"] = (
            "A sandbox lookup returned None and the code indexed into it. "
            "Do not reuse the same lookup chain. Inspect rows first, then use find_row/safe_get or require_row/require_fields."
        )
    elif "no row matched:" in lowered:
        result["error_class"] = "missing_row"
        result["retry_guidance"] = (
            "The requested row was not present. Inspect available rows and codes before retrying, "
            "or retrieve data at a compatible level instead of assuming the join target exists."
        )
    elif "missing required fields:" in lowered:
        result["error_class"] = "missing_fields"
        result["retry_guidance"] = (
            "The target row exists but required fields were absent or null. "
            "Inspect schema/rows first and adjust the calculation to fields that are actually populated."
        )
    elif "numeric_change requires two non-null numeric values" in lowered:
        result["error_class"] = "missing_numeric_inputs"
        result["retry_guidance"] = (
            "The calculation attempted to use null numeric values. "
            "Verify the numerator and denominator exist before computing the metric."
        )
    elif "timed out" in lowered or "timeout" in lowered:
        result["error_class"] = "sandbox_timeout"
        result["retry_guidance"] = (
            "The sandbox step was too heavy. Retry with a smaller inspection step or simplify the transformation before joining artifacts."
        )
    elif "handwritten descending lambda with unary minus" in lowered:
        result["error_class"] = "sandbox_brittle_sort"
        result["retry_guidance"] = (
            "Do not hand-write descending sort lambdas over ABS rows. "
            "Use sort_by_numeric/top_n_by_numeric or inspect and extract numeric fields first."
        )
    elif "defines a custom uniq helper" in lowered:
        result["error_class"] = "sandbox_custom_dedupe"
        result["retry_guidance"] = (
            "Do not write custom uniq/dedupe helpers. "
            "Use distinct_values, group_rows, or index_rows to inspect or deduplicate rows."
        )
    elif "raw arithmetic on potentially nullable abs values" in lowered:
        result["error_class"] = "sandbox_null_arithmetic"
        result["retry_guidance"] = (
            "Do not add or subtract raw ABS values directly. "
            "Use get_value/get_numeric/safe_float first and guard missing values before arithmetic."
        )
    else:
        result["error_class"] = "sandbox_generic"
        result["retry_guidance"] = (
            "Do not repeat the same sandbox code. Inspect the artifact shape again and choose a narrower or safer analysis step."
        )
    return result


def _sandbox_retry_conflict(state, tool_input: Dict[str, Any]) -> Optional[str]:
    recent_failure = _recent_failed_sandbox_entry(state)
    if recent_failure is None:
        return None

    prior_data = recent_failure.get("result_data") if isinstance(recent_failure.get("result_data"), dict) else {}
    prior_normalized = str(prior_data.get("sandbox_code_normalized") or "").strip()
    current_normalized = _normalize_sandbox_code(tool_input.get("code"))
    prior_request = str(prior_data.get("sandbox_request") or "").strip()
    current_request = str(tool_input.get("sandbox_request") or "").strip()

    same_attempt = False
    if prior_normalized and current_normalized and prior_normalized == current_normalized:
        same_attempt = True
    elif prior_request and current_request and prior_request == current_request:
        same_attempt = True

    if same_attempt:
        guidance = str(prior_data.get("retry_guidance") or "").strip()
        return (
            "The proposed sandbox step is materially the same as the most recent failed sandbox attempt. "
            "Choose a different approach instead of retrying identical code. "
            f"Previous guidance: {guidance}"
        ).strip()

    return None


def _count_recent_recovery_failures(state) -> int:
    count = 0
    for item in reversed(state.loop_history):
        if not isinstance(item, dict):
            break
        result_data = item.get("result_data") if isinstance(item.get("result_data"), dict) else {}
        kind = str(result_data.get("kind") or "").strip()
        if kind not in {"model_call_error", "harness_parse_error"}:
            break
        count += 1
    return count


def _count_recent_harness_parse_failures(state) -> int:
    count = 0
    for item in reversed(state.loop_history):
        if not isinstance(item, dict):
            break
        result_data = item.get("result_data") if isinstance(item.get("result_data"), dict) else {}
        if str(result_data.get("kind") or "").strip() != "harness_parse_error":
            break
        count += 1
    return count


def _retry_reasoning_effort(state) -> Optional[str]:
    last_entry = state.loop_history[-1] if state.loop_history else None
    if not isinstance(last_entry, dict):
        return None
    result_data = last_entry.get("result_data") if isinstance(last_entry.get("result_data"), dict) else {}
    kind = str(result_data.get("kind") or "").strip()
    if kind in {"tool_error", "model_call_error", "harness_parse_error"}:
        return "medium"
    return None


def _reset_context_after_curation(state, plan_context: Dict[str, Any]) -> None:
    dataset_id = str(plan_context.get("curate_dataset_id") or "").strip()
    dataset_title = str(plan_context.get("curated_dataset_title") or dataset_id).strip()
    question = str(plan_context.get("question") or "").strip()
    selected_ids = _clean_string_list(plan_context.get("selected_dataset_ids"))

    handoff_summary = (
        f"Curation completed for {dataset_title or dataset_id}. "
        "Raw discovery context was collapsed before answer execution."
    ).strip()
    if question:
        handoff_summary += f" Original question: {question}"

    handoff_entry = {
        "step": {
            "id": "compose_final",
            "summary": "Curation completed; switch to curated-answer mode",
        },
        "progress_note": "Using the newly curated dataset.",
        "result_summary": handoff_summary[:2400],
        "result_data": {
            "kind": "curation_handoff",
            "dataset_id": dataset_id,
            "title": dataset_title,
            "selected_dataset_ids": selected_ids,
            "question": question,
        },
    }

    state.loop_history = [handoff_entry]
    state.artifacts = []


def _payload_loop_history(state, run_loop_start_index: int) -> tuple[List[Dict[str, Any]], int]:
    prior = state.loop_history[:run_loop_start_index]
    current = state.loop_history[run_loop_start_index:]
    prior_payload = compact_loop_history(prior, limit=4) if prior else []
    current_payload = compact_loop_history(current, limit=max(len(current), 1)) if current else []
    return prior_payload + current_payload, len(current_payload)


def _payload_artifacts(state, run_artifact_start_index: int) -> tuple[List[Dict[str, Any]], int]:
    prior = state.artifacts[:run_artifact_start_index]
    current = state.artifacts[run_artifact_start_index:]
    prior_payload = compact_artifacts(prior, limit=4) if prior else []
    current_payload = compact_artifacts(current, limit=max(len(current), 1)) if current else []
    return prior_payload + current_payload, len(current_payload)


def generate_response(
    conversation_id: str,
    user_content: str,
    store: ConversationStore,
    status_callback: Optional[Callable[[str], None]] = None,
) -> str:
    cancel_event = _acquire_cancellation_event(conversation_id)
    status_callback = status_callback or (lambda _message: None)

    try:
        state = store.load(conversation_id)
        _ensure_runtime_dirs(conversation_id)
        saved_progress_messages: list[str] = ["Starting analysis"]

        def emit_status(message: str) -> None:
            normalized = str(message or "").strip()
            if not normalized:
                return
            status_callback(normalized)
            if not saved_progress_messages or saved_progress_messages[-1] != normalized:
                saved_progress_messages.append(normalized)

        def persist_completed_turn(assistant_content: str) -> None:
            state.messages.append({"role": "user", "content": user_content})
            for progress_message in saved_progress_messages:
                state.messages.append({"role": "progress", "content": progress_message})
            state.messages.append({"role": "assistant", "content": assistant_content})
            state.active_run_message_count = len(state.messages)
            state.active_run_loop_count = len(state.loop_history)
            state.active_run_artifact_count = len(state.artifacts)
            store.save(state)

        active_user_message = user_content
        payload_chat_history = build_chat_history_payload(state.messages, recent_full_limit=8, older_compact_limit=4)
        pending_plan = state.pending_plan if isinstance(state.pending_plan, dict) else None
        if pending_plan and str(pending_plan.get("status") or "") == "awaiting_approval":
            plan_context = _normalize_plan_context(
                pending_plan.get("plan_context"),
                fallback_question=user_content,
            )
            if bool(plan_context.get("await_user_input")):
                state.pending_plan = None
                active_user_message = (
                    f"{str(plan_context.get('question') or user_content).strip()}\n\n"
                    f"User clarification: {user_content.strip()}"
                ).strip()
                emit_status("Got the clarification. Continuing with the analysis.")
            else:
                plan_reply = _detect_plan_reply(user_content)
                if plan_reply == "approve":
                    curate_dataset_id = str(plan_context.get("curate_dataset_id") or "").strip()
                    if bool(plan_context.get("post_curation_confirmation")):
                        _reset_context_after_curation(state, plan_context)
                        approved_answer_context = {
                            "question": str(plan_context.get("question") or user_content).strip() or user_content,
                            "selected_dataset_ids": _clean_string_list(plan_context.get("selected_dataset_ids")),
                        }
                        curated_dataset_title = str(plan_context.get("curated_dataset_title") or "").strip()
                        if curated_dataset_title:
                            approved_answer_context["curated_dataset_title"] = curated_dataset_title
                        state.pending_plan = {
                            "status": "approved",
                            "plan_markdown": str(pending_plan.get("plan_markdown") or "").strip(),
                            "plan_context": approved_answer_context,
                        }
                        active_user_message = str(approved_answer_context.get("question") or user_content).strip() or user_content
                        emit_status("Curation approved. Continuing in curated-answer mode.")
                    elif curate_dataset_id:
                        emit_status(f"Adding `{curate_dataset_id}` to the AI-curated ABS overlay.")
                        curated_entry = _curate_dataset_from_abs(curate_dataset_id)
                        selected_ids = _clean_string_list(plan_context.get("selected_dataset_ids"))
                        if curate_dataset_id not in selected_ids:
                            selected_ids.append(curate_dataset_id)
                        plan_context["selected_dataset_ids"] = selected_ids
                        plan_context["curated_dataset_title"] = str(curated_entry.get("title") or curate_dataset_id).strip()
                        followup_markdown = (
                            f"I've reviewed and added `{curate_dataset_id}` to the AI-curated ABS overlay.\n\n"
                            f"It is now available for this conversation as **{plan_context['curated_dataset_title']}**.\n\n"
                            "Shall I proceed with answering your original question using it?"
                        )
                        plan_context["post_curation_confirmation"] = True
                        state.pending_plan = {
                            "status": "awaiting_approval",
                            "plan_markdown": followup_markdown,
                            "plan_context": plan_context,
                        }
                        persist_completed_turn(followup_markdown)
                        emit_status("The dataset has been curated into the AI overlay and is ready to use.")
                        return followup_markdown
                    else:
                        # A normal approval plan is the gate for broader raw ABS discovery.
                        # Clarification prompts are handled above via await_user_input, and
                        # post-curation confirmations are handled in the dedicated branch.
                        plan_context["allow_raw_discovery"] = True
                        state.pending_plan = {
                            "status": "approved",
                            "plan_markdown": str(pending_plan.get("plan_markdown") or "").strip(),
                            "plan_context": plan_context,
                        }
                        active_user_message = str(plan_context.get("question") or user_content).strip() or user_content
                        emit_status("Plan approved. Continuing with the harness execution.")
                else:
                    state.pending_plan = None

        if _should_reset_after_user_correction(state, user_content):
            active_user_message = _reset_context_after_user_correction(state, user_content)
            payload_chat_history = build_chat_history_payload(state.messages, recent_full_limit=8, older_compact_limit=4)
            state.pending_plan = None
            emit_status("Rechecking from source after the correction.")

        store.save(state)

        run_loop_start_index = len(state.loop_history)
        run_artifact_start_index = len(state.artifacts)

        for loop_index in range(1, settings.max_loops + 1):
            _ensure_not_cancelled(conversation_id, cancel_event, f"loop_{loop_index}_start")

            payload_loop_history, protected_loop_history_count = _payload_loop_history(
                state,
                run_loop_start_index,
            )
            payload_artifacts, protected_artifact_count = _payload_artifacts(
                state,
                run_artifact_start_index,
            )

            payload = build_loop_payload(
                user_message=active_user_message,
                chat_history=payload_chat_history,
                loop_history=payload_loop_history,
                artifacts=payload_artifacts,
                plan_state=_build_plan_state(state),
                loop_index=loop_index,
                max_loops=settings.max_loops,
                protected_loop_history_count=protected_loop_history_count,
                protected_artifact_count=protected_artifact_count,
            )
            try:
                raw_model_response = _call_model(
                    build_model_messages(payload),
                    reasoning_effort=_retry_reasoning_effort(state),
                )
            except Exception as exc:
                logger.exception(
                    "Model call failed cid=%s loop=%s error=%s",
                    conversation_id,
                    loop_index,
                    exc,
                )
                _record_loop_feedback(
                    state,
                    step={
                        "id": "model_call_error",
                        "summary": "Model API call failed before a loop decision was returned",
                    },
                    progress_note="Recovering from a model call failure.",
                    result_summary=(
                        "The model API call failed before the harness received a valid loop decision.\n"
                        f"Error: {str(exc)}"
                    ),
                    result_data={
                        "kind": "model_call_error",
                        "error": str(exc),
                    },
                )
                store.save(state)
                if _count_recent_recovery_failures(state) >= MAX_CONSECUTIVE_RECOVERY_FAILURES:
                    raise RuntimeError(
                        "The harness hit repeated model-call failures and stopped after 3 recovery attempts. "
                        "Please retry or ask a narrower follow-up."
                    )
                emit_status("That loop hit a model error. Trying again.")
                continue
            try:
                parsed = parse_harness_loop_output(raw_model_response)
            except HarnessParserError as exc:
                diagnostics = exc.diagnostics if isinstance(getattr(exc, "diagnostics", None), dict) else {}
                logger.warning(
                    "Harness parse failed cid=%s loop=%s error=%s failure_class=%s candidate_count=%s validation_path=%s top_level_keys=%s truncated_suspected=%s raw_len=%s raw=%s",
                    conversation_id,
                    loop_index,
                    str(exc),
                    diagnostics.get("failure_class"),
                    diagnostics.get("candidate_count"),
                    diagnostics.get("validation_path"),
                    diagnostics.get("top_level_keys_detected") or diagnostics.get("normalized_top_level_keys"),
                    diagnostics.get("truncated_suspected"),
                    diagnostics.get("raw_length"),
                    _truncate(raw_model_response, 600),
                )
                next_parse_retry = _count_recent_harness_parse_failures(state) + 1
                if next_parse_retry == 1:
                    progress_text = "That step failed output validation. Retrying with stricter formatting."
                elif next_parse_retry == 2:
                    progress_text = "That step failed output validation again. Retrying with ultra-strict formatting."
                else:
                    progress_text = "That step failed output validation repeatedly."

                _record_loop_feedback(
                    state,
                    step={
                        "id": "invalid_model_output",
                        "summary": "Model returned malformed harness JSON",
                    },
                    progress_note=progress_text,
                    result_summary=(
                        "The model returned malformed harness JSON.\n"
                        f"Parse error: {str(exc)}\n"
                        "Correction required: stay with the intended loop decision, but return one literal top-level JSON object with "
                        "`step`, `progress_note`, and `model_output` only. "
                        "Do not include prose, markdown fences, quoted JSON, escaped JSON, or nested wrapper objects.\n"
                        f"Raw output preview: {_truncate(raw_model_response, 600)}"
                    ),
                    result_data={
                        "kind": "harness_parse_error",
                        "parse_error": str(exc),
                        "parse_diagnostics": diagnostics,
                        "retry_stage": next_parse_retry,
                        "raw_output_preview": _truncate(raw_model_response, 600),
                        "correction_instructions": (
                            "Return one literal top-level JSON object only. "
                            "Required keys: step, progress_note, model_output. "
                            "For tool steps, model_output must contain tool_name and tool_input as an object. "
                            "Do not rethink the task; fix the output shape."
                        ),
                    },
                )
                store.save(state)
                if _count_recent_recovery_failures(state) >= MAX_CONSECUTIVE_RECOVERY_FAILURES:
                    raise RuntimeError(
                        "The harness hit repeated malformed model outputs and stopped after 3 recovery attempts. "
                        "Please retry or ask a narrower follow-up."
                    )
                emit_status(progress_text)
                continue

            step = parsed["step"]
            progress_note = parsed["progress_note"]
            model_output = parsed["model_output"]

            logger.info(
                'Loop decision cid=%s loop=%s step=%s summary="%s" progress="%s"',
                conversation_id,
                loop_index,
                step.get("id"),
                _truncate(step.get("summary") or "", 220),
                _truncate(progress_note, 220),
            )
            if step["id"] in {"use_abs_data_tool", "use_web_search_tool", "use_sandbox_tool"}:
                logger.info(
                    'Loop tool input cid=%s loop=%s step=%s input="%s"',
                    conversation_id,
                    loop_index,
                    step.get("id"),
                    _summarize_tool_input(model_output.get("tool_input") if isinstance(model_output, dict) else {}),
                )

            if step["id"] not in {"propose_plan", "compose_final"}:
                emit_status(progress_note)
            _ensure_not_cancelled(conversation_id, cancel_event, f"loop_{loop_index}_after_parse")

            if step["id"] == "propose_plan":
                plan_markdown = str(model_output.get("plan_markdown") or "").strip()
                plan_context = _normalize_plan_context(
                    model_output.get("plan_context"),
                    fallback_question=active_user_message,
                )
                state.pending_plan = {
                    "status": "awaiting_approval",
                    "plan_markdown": plan_markdown,
                    "plan_context": plan_context,
                }
                persist_completed_turn(plan_markdown)
                logger.info(
                    'Loop plan cid=%s loop=%s plan="%s"',
                    conversation_id,
                    loop_index,
                    _truncate(plan_markdown, 280),
                )
                return plan_markdown

            if step["id"] == "compose_final":
                final_answer = str(model_output.get("final_answer_markdown") or "").strip()
                state.pending_plan = None
                persist_completed_turn(final_answer)
                logger.info(
                    'Loop final cid=%s loop=%s preview="%s"',
                    conversation_id,
                    loop_index,
                    _truncate(final_answer, 280),
                )
                return final_answer

            try:
                if step["id"] == "use_sandbox_tool":
                    retry_conflict = _sandbox_retry_conflict(state, model_output["tool_input"])
                    if retry_conflict:
                        raise RuntimeError(retry_conflict)
                if step["id"] == "use_abs_data_tool":
                    tool_result = _execute_abs_data_tool(
                        tool_input=model_output["tool_input"],
                        state=state,
                        conversation_id=conversation_id,
                    )
                elif step["id"] == "use_web_search_tool":
                    tool_result = _execute_web_search_tool(
                        tool_input=model_output["tool_input"],
                        state=state,
                        conversation_id=conversation_id,
                    )
                elif step["id"] == "use_sandbox_tool":
                    tool_result = _execute_sandbox_tool(
                        tool_input=model_output["tool_input"],
                        state=state,
                        conversation_id=conversation_id,
                        loop_payload=payload,
                        status_callback=emit_status,
                    )
                else:
                    raise RuntimeError(f"Unsupported step id: {step['id']}")
            except Exception as exc:
                logger.exception(
                    "Tool step failed cid=%s loop=%s step=%s error=%s",
                    conversation_id,
                    loop_index,
                    step.get("id"),
                    exc,
                )
                result_summary = (
                    "Tool execution failed. Adjust the next step using this feedback.\n"
                    f"Error: {str(exc)}"
                )
                failure_data = _classify_tool_failure(
                    str(step.get("id") or "").strip(),
                    str(exc),
                    model_output.get("tool_input") if isinstance(model_output, dict) else {},
                )
                retry_guidance = str(failure_data.get("retry_guidance") or "").strip()
                if retry_guidance:
                    result_summary += f"\nRecovery guidance: {retry_guidance}"
                _record_loop_feedback(
                    state,
                    step=step,
                    progress_note=progress_note,
                    result_summary=result_summary,
                    result_data=failure_data,
                )
                store.save(state)
                emit_status("That step failed. Trying a different approach.")
                continue

            result_summary = str(tool_result.get("summary") or "").strip()
            result_data = tool_result.get("result_data")

            _record_loop_feedback(
                state,
                step=step,
                progress_note=progress_note,
                result_summary=result_summary,
                result_data=result_data,
            )
            store.save(state)
            logger.info(
                'Loop result cid=%s loop=%s step=%s result="%s"',
                conversation_id,
                loop_index,
                step.get("id"),
                _truncate(result_summary, 280),
            )

        final_answer = _compose_best_effort_final(conversation_id, active_user_message, state)
        state.pending_plan = None
        persist_completed_turn(final_answer)
        logger.info(
            'Loop max best-effort final cid=%s preview="%s"',
            conversation_id,
            _truncate(final_answer, 280),
        )
        return final_answer
    finally:
        _release_cancellation_event(conversation_id)
