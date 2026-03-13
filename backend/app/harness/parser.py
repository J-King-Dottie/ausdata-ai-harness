from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

from .state import ALLOWED_STEP_IDS


class HarnessParserError(Exception):
    pass


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
        elif tool_name == "web_search_tool":
            inferred_step_id = "use_web_search_tool"
        elif tool_name == "abs_data_tool" or str(normalized.get("action") or "").strip():
            inferred_step_id = "use_abs_data_tool"
        elif tool_name == "sandbox_tool" or (
            isinstance(tool_input, dict)
            and (
                "artifact_ids" in tool_input
                or "code" in tool_input
            )
        ):
            inferred_step_id = "use_sandbox_tool"

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
            if str(normalized.get("action") or "").strip():
                tool_name = "abs_data_tool"
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
            or "Working on the next step."
        ).strip()

    return normalized


def _decode_first_json_object(text: str) -> Optional[str]:
    snippet = (text or "").lstrip("\ufeff \t\r\n")
    if not snippet:
        return None

    decoder = json.JSONDecoder()
    try:
        decoded, end_index = decoder.raw_decode(snippet)
    except json.JSONDecodeError:
        return None

    if isinstance(decoded, dict):
        return snippet[:end_index]

    if isinstance(decoded, str):
        nested = decoded.strip()
        if not nested:
            return None
        try:
            nested_decoded, nested_end = decoder.raw_decode(nested)
        except json.JSONDecodeError:
            return None
        if isinstance(nested_decoded, dict):
            return nested[:nested_end]

    return None


def _extract_json_candidate(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if not text:
        raise HarnessParserError("Model returned empty output.")

    fenced_blocks = re.findall(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    for block in fenced_blocks:
        candidate = _decode_first_json_object(block)
        if candidate:
            return candidate

    direct = _decode_first_json_object(text)
    if direct:
        return direct

    for index, char in enumerate(text):
        if char != "{":
            continue
        candidate = _decode_first_json_object(text[index:])
        if candidate:
            return candidate

    raise HarnessParserError("No JSON object found in model output.")


def parse_harness_loop_output(raw_text: str) -> Dict[str, Any]:
    candidate = _extract_json_candidate(raw_text)

    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise HarnessParserError(f"Invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise HarnessParserError("Top-level JSON must be an object.")

    payload = _normalize_payload_shape(payload)

    step = payload.get("step")
    if not isinstance(step, dict):
        raise HarnessParserError("step is required and must be an object")

    step_id = str(step.get("id") or "").strip()
    if step_id not in ALLOWED_STEP_IDS:
        raise HarnessParserError(f"Invalid step.id '{step_id}'")

    progress_note = str(payload.get("progress_note") or "").strip()
    if not progress_note:
        raise HarnessParserError("progress_note is required")

    model_output = payload.get("model_output")
    if not isinstance(model_output, dict):
        raise HarnessParserError("model_output must be an object")

    if step_id in {"use_abs_data_tool", "use_web_search_tool", "use_sandbox_tool"}:
        tool_name = str(model_output.get("tool_name") or "").strip()
        tool_input = model_output.get("tool_input")
        if not tool_name:
            raise HarnessParserError("model_output.tool_name is required")
        if not isinstance(tool_input, dict):
            raise HarnessParserError("model_output.tool_input must be an object")
        expected = (
            "abs_data_tool"
            if step_id == "use_abs_data_tool"
            else "web_search_tool"
            if step_id == "use_web_search_tool"
            else "sandbox_tool"
        )
        if tool_name != expected:
            raise HarnessParserError(
                f"step.id '{step_id}' must use model_output.tool_name='{expected}'"
            )

    if step_id == "compose_final":
        final_answer = str(model_output.get("final_answer_markdown") or "").strip()
        if not final_answer:
            raise HarnessParserError(
                "model_output.final_answer_markdown is required for compose_final"
            )

    if step_id == "propose_plan":
        plan_markdown = str(model_output.get("plan_markdown") or "").strip()
        if not plan_markdown:
            raise HarnessParserError("model_output.plan_markdown is required for propose_plan")
        plan_context = model_output.get("plan_context")
        if plan_context is not None and not isinstance(plan_context, dict):
            raise HarnessParserError("model_output.plan_context must be an object if provided")

    return {
        "step": {
            "id": step_id,
            "summary": str(step.get("summary") or "").strip(),
        },
        "progress_note": progress_note,
        "model_output": model_output,
    }
