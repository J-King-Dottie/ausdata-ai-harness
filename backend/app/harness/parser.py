from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

from .state import ALLOWED_STEP_IDS


class HarnessParserError(Exception):
    def __init__(self, message: str, diagnostics: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.diagnostics: Dict[str, Any] = diagnostics or {}


def _candidate_score(decoded: Any) -> int:
    if not isinstance(decoded, dict):
        return -1
    score = 0
    for key in ("step", "progress_note", "model_output"):
        if key in decoded:
            score += 3
    for key in (
        "tool_name",
        "tool_input",
        "final_answer_markdown",
        "plan_markdown",
        "summary",
        "message",
        "status",
        "progress",
    ):
        if key in decoded:
            score += 1
    return score


def _normalize_payload_shape(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)

    step = normalized.get("step")
    if isinstance(step, str):
        normalized["step"] = {
            "id": step.strip(),
            "summary": str(normalized.get("summary") or normalized.get("step_summary") or "").strip(),
        }
        step = normalized["step"]

    model_output = normalized.get("model_output")
    if not isinstance(model_output, dict):
        model_output = {}

    tool_name = str(model_output.get("tool_name") or normalized.get("tool_name") or "").strip()
    tool_input = model_output.get("tool_input")
    if not isinstance(tool_input, dict):
        candidate_input = normalized.get("tool_input")
        if isinstance(candidate_input, dict):
            tool_input = candidate_input
        else:
            action = str(normalized.get("action") or "").strip()
            if action:
                tool_input = {
                    "action": action,
                }
                dataset_id = str(normalized.get("datasetId") or normalized.get("dataset_id") or "").strip()
                if dataset_id:
                    tool_input["datasetId"] = dataset_id
                filters = normalized.get("filters")
                if isinstance(filters, dict):
                    tool_input["filters"] = filters
                for key in ("startPeriod", "endPeriod", "detail", "dimensionAtObservation"):
                    value = normalized.get(key)
                    if value not in {None, ""}:
                        tool_input[key] = value
            elif "artifact_ids" in normalized or "code" in normalized:
                tool_input = {}
                if isinstance(normalized.get("artifact_ids"), list):
                    tool_input["artifact_ids"] = normalized["artifact_ids"]
                if normalized.get("code") is not None:
                    tool_input["code"] = normalized.get("code")

    final_answer = str(
        model_output.get("final_answer_markdown")
        or normalized.get("final_answer_markdown")
        or normalized.get("final_answer")
        or normalized.get("answer")
        or ""
    ).strip()
    plan_markdown = str(
        model_output.get("plan_markdown")
        or normalized.get("plan_markdown")
        or normalized.get("plan")
        or ""
    ).strip()

    if not isinstance(step, dict):
        inferred_step_id = ""
        if final_answer:
            inferred_step_id = "compose_final"
        elif plan_markdown:
            inferred_step_id = "propose_plan"
        elif tool_name == "provider_route_tool":
            inferred_step_id = "provider_route_tool"
        elif tool_name == "web_search_tool":
            inferred_step_id = "web_search_tool"
        elif tool_name == "macro_data_tool":
            inferred_step_id = "macro_data_tool"
        elif tool_name == "aus_metadata_tool":
            inferred_step_id = "aus_metadata_tool"
        elif tool_name == "aus_raw_retrieve_tool":
            inferred_step_id = "aus_raw_retrieve_tool"
        elif str(normalized.get("action") or "").strip():
            action = str(normalized.get("action") or "").strip()
            if action == "metadata":
                inferred_step_id = "aus_metadata_tool"
            elif action == "raw_retrieve":
                inferred_step_id = "aus_raw_retrieve_tool"
        elif tool_name == "sandbox_tool" or (
            isinstance(tool_input, dict)
            and (
                "artifact_ids" in tool_input
                or "code" in tool_input
            )
        ):
            inferred_step_id = "sandbox_tool"

        if inferred_step_id:
            normalized["step"] = {
                "id": inferred_step_id,
                "summary": str(normalized.get("summary") or normalized.get("step_summary") or "").strip(),
            }

    if final_answer:
        normalized["model_output"] = {
            "final_answer_markdown": final_answer,
        }
    elif plan_markdown:
        plan_context = model_output.get("plan_context")
        if not isinstance(plan_context, dict):
            candidate_plan_context = normalized.get("plan_context")
            plan_context = candidate_plan_context if isinstance(candidate_plan_context, dict) else {}
        normalized["model_output"] = {
            "plan_markdown": plan_markdown,
            "plan_context": plan_context,
        }
    elif tool_name or isinstance(tool_input, dict):
        if not tool_name:
            action = str(normalized.get("action") or "").strip()
            if action == "metadata":
                tool_name = "aus_metadata_tool"
            elif action == "raw_retrieve":
                tool_name = "aus_raw_retrieve_tool"
            elif isinstance(tool_input, dict) and (
                "artifact_ids" in tool_input or "code" in tool_input
            ):
                tool_name = "sandbox_tool"
        normalized["model_output"] = {
            "tool_name": tool_name,
            "tool_input": tool_input if isinstance(tool_input, dict) else {},
        }

    if "progress_note" not in normalized or not str(normalized.get("progress_note") or "").strip():
        normalized["progress_note"] = str(
            normalized.get("message")
            or normalized.get("status")
            or normalized.get("progress")
            or normalized.get("note")
            or ((normalized.get("step") or {}).get("summary") if isinstance(normalized.get("step"), dict) else "")
            or ""
        ).strip()

    return normalized


def _decode_first_json_object(text: str) -> Optional[tuple[str, Any]]:
    snippet = (text or "").lstrip("\ufeff \t\r\n")
    if not snippet:
        return None

    decoder = json.JSONDecoder()
    try:
        decoded, end_index = decoder.raw_decode(snippet)
    except json.JSONDecodeError:
        return None

    if isinstance(decoded, dict):
        return snippet[:end_index], decoded

    if isinstance(decoded, str):
        nested = decoded.strip()
        if not nested:
            return None
        try:
            nested_decoded, nested_end = decoder.raw_decode(nested)
        except json.JSONDecodeError:
            return None
        if isinstance(nested_decoded, dict):
            return nested[:nested_end], nested_decoded

    return None


def _looks_truncated(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False
    if stripped.endswith(("...", "\u2026")):
        return True
    return stripped.count("{") > stripped.count("}") or stripped.count("[") > stripped.count("]")


def _extract_json_candidates(raw_text: str) -> list[Dict[str, Any]]:
    text = (raw_text or "").strip()
    if not text:
        raise HarnessParserError(
            "Model returned empty output.",
            diagnostics={
                "failure_class": "empty_output",
                "raw_length": 0,
            },
        )

    candidates: list[Dict[str, Any]] = []

    fenced_blocks = re.findall(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    for block in fenced_blocks:
        decoded = _decode_first_json_object(block)
        if decoded:
            candidate_text, candidate_obj = decoded
            candidates.append(
                {
                    "score": _candidate_score(candidate_obj),
                    "text": candidate_text,
                    "source": "fenced_block",
                }
            )

    direct = _decode_first_json_object(text)
    if direct:
        candidate_text, candidate_obj = direct
        candidates.append(
            {
                "score": _candidate_score(candidate_obj),
                "text": candidate_text,
                "source": "top_level",
            }
        )

    # Recover from leading prose, markdown labels, or other wrapper text by
    # scanning for the first decodable JSON object start.
    for match in re.finditer(r"\{", text):
        decoded = _decode_first_json_object(text[match.start() :])
        if decoded:
            candidate_text, candidate_obj = decoded
            candidates.append(
                {
                    "score": _candidate_score(candidate_obj),
                    "text": candidate_text,
                    "source": "brace_scan",
                }
            )

    # Also tolerate quoted JSON objects embedded in surrounding text.
    decoder = json.JSONDecoder()
    for match in re.finditer(r'"', text):
        snippet = text[match.start() :]
        try:
            decoded, end_index = decoder.raw_decode(snippet)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, str):
            nested = _decode_first_json_object(decoded)
            if nested:
                candidate_text, candidate_obj = nested
                candidates.append(
                    {
                        "score": _candidate_score(candidate_obj),
                        "text": candidate_text,
                        "source": "quoted_json",
                    }
                )

    if candidates:
        ordered = sorted(candidates, key=lambda item: int(item.get("score") or -1), reverse=True)
        unique_candidates: list[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in ordered:
            candidate = str(item.get("text") or "")
            if candidate in seen:
                continue
            seen.add(candidate)
            unique_candidates.append(item)
        return unique_candidates

    raise HarnessParserError(
        "No JSON object found in model output.",
        diagnostics={
            "failure_class": "no_json_found",
            "raw_length": len(raw_text or ""),
            "raw_preview": text[:600],
            "truncated_suspected": _looks_truncated(raw_text),
        },
    )


def parse_harness_loop_output(raw_text: str) -> Dict[str, Any]:
    last_error: Optional[HarnessParserError] = None
    candidate_infos = _extract_json_candidates(raw_text)
    for index, candidate_info in enumerate(candidate_infos, start=1):
        candidate = str(candidate_info.get("text") or "")
        candidate_source = str(candidate_info.get("source") or "unknown")
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_error = HarnessParserError(
                f"Invalid JSON: {exc}",
                diagnostics={
                    "failure_class": "invalid_json_candidate",
                    "candidate_index": index,
                    "candidate_source": candidate_source,
                    "candidate_count": len(candidate_infos),
                    "raw_length": len(raw_text or ""),
                    "raw_preview": (raw_text or "")[:600],
                    "candidate_preview": candidate[:400],
                    "truncated_suspected": _looks_truncated(raw_text),
                },
            )
            continue

        if not isinstance(payload, dict):
            last_error = HarnessParserError(
                "Top-level JSON must be an object.",
                diagnostics={
                    "failure_class": "top_level_not_object",
                    "candidate_index": index,
                    "candidate_source": candidate_source,
                    "candidate_count": len(candidate_infos),
                    "raw_length": len(raw_text or ""),
                    "raw_preview": (raw_text or "")[:600],
                    "candidate_type": type(payload).__name__,
                    "truncated_suspected": _looks_truncated(raw_text),
                },
            )
            continue

        original_top_level_keys = sorted(str(key) for key in payload.keys())
        payload = _normalize_payload_shape(payload)
        normalized_top_level_keys = sorted(str(key) for key in payload.keys())

        def _schema_error(message: str, *, validation_path: str) -> HarnessParserError:
            return HarnessParserError(
                message,
                diagnostics={
                    "failure_class": "schema_validation_failed",
                    "candidate_index": index,
                    "candidate_source": candidate_source,
                    "candidate_count": len(candidate_infos),
                    "raw_length": len(raw_text or ""),
                    "raw_preview": (raw_text or "")[:600],
                    "candidate_preview": candidate[:400],
                    "top_level_keys_detected": original_top_level_keys,
                    "normalized_top_level_keys": normalized_top_level_keys,
                    "validation_path": validation_path,
                    "truncated_suspected": _looks_truncated(raw_text),
                },
            )

        step = payload.get("step")
        if not isinstance(step, dict):
            last_error = _schema_error("step is required and must be an object", validation_path="step")
            continue

        step_id = str(step.get("id") or "").strip()
        if step_id not in ALLOWED_STEP_IDS:
            last_error = _schema_error(f"Invalid step.id '{step_id}'", validation_path="step.id")
            continue

        progress_note = str(payload.get("progress_note") or "").strip()
        if not progress_note:
            last_error = _schema_error("progress_note is required", validation_path="progress_note")
            continue

        model_output = payload.get("model_output")
        if not isinstance(model_output, dict):
            last_error = _schema_error("model_output must be an object", validation_path="model_output")
            continue

        if step_id in {"provider_route_tool", "aus_metadata_tool", "aus_raw_retrieve_tool", "macro_data_tool", "web_search_tool", "sandbox_tool"}:
            tool_name = str(model_output.get("tool_name") or "").strip()
            tool_input = model_output.get("tool_input")
            if not tool_name:
                last_error = _schema_error(
                    "model_output.tool_name is required",
                    validation_path="model_output.tool_name",
                )
                continue
            if not isinstance(tool_input, dict):
                last_error = _schema_error(
                    "model_output.tool_input must be an object",
                    validation_path="model_output.tool_input",
                )
                continue
            expected = (
                "provider_route_tool"
                if step_id == "provider_route_tool"
                else "aus_metadata_tool"
                if step_id == "aus_metadata_tool"
                else "aus_raw_retrieve_tool"
                if step_id == "aus_raw_retrieve_tool"
                else "macro_data_tool"
                if step_id == "macro_data_tool"
                else "web_search_tool"
                if step_id == "web_search_tool"
                else "sandbox_tool"
            )
            if tool_name != expected:
                last_error = _schema_error(
                    f"step.id '{step_id}' must use model_output.tool_name='{expected}'",
                    validation_path="model_output.tool_name",
                )
                continue

        if step_id == "compose_final":
            final_answer = str(model_output.get("final_answer_markdown") or "").strip()
            if not final_answer:
                last_error = _schema_error(
                    "model_output.final_answer_markdown is required for compose_final",
                    validation_path="model_output.final_answer_markdown",
                )
                continue

        if step_id == "propose_plan":
            plan_markdown = str(model_output.get("plan_markdown") or "").strip()
            if not plan_markdown:
                last_error = _schema_error(
                    "model_output.plan_markdown is required for propose_plan",
                    validation_path="model_output.plan_markdown",
                )
                continue
            plan_context = model_output.get("plan_context")
            if plan_context is not None and not isinstance(plan_context, dict):
                last_error = _schema_error(
                    "model_output.plan_context must be an object if provided",
                    validation_path="model_output.plan_context",
                )
                continue

        return {
            "step": {
                "id": step_id,
                "summary": str(step.get("summary") or "").strip(),
            },
            "progress_note": progress_note,
            "model_output": model_output,
        }

    if last_error is not None:
        raise last_error
    raise HarnessParserError(
        "No valid harness payload object found in model output.",
        diagnostics={
            "failure_class": "no_valid_payload_found",
            "candidate_count": len(candidate_infos),
            "raw_length": len(raw_text or ""),
            "raw_preview": (raw_text or "")[:600],
            "truncated_suspected": _looks_truncated(raw_text),
        },
    )
