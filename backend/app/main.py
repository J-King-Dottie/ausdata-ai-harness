from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import secrets
import sys
import time
from pathlib import Path
from typing import Optional

import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from openai import APIError, APIStatusError
from pydantic import BaseModel, Field

from .agents_service import (
    ConversationCancelled,
    cancel_conversation_processing,
    clear_agent_session,
    generate_response,
    generate_latest_export,
    get_latest_export_artifact_path,
    reset_conversation_runtime,
    sync_agent_session_from_state,
)
from .storage import ConversationStore


class ChatRequest(BaseModel):
    conversation_id: str = Field(..., description="Stable identifier for the chat session.")
    message: str = Field(..., description="The user's natural language query.")


class ResetRequest(BaseModel):
    conversation_id: str = Field(..., description="Conversation to reset.")


class CancelRequest(BaseModel):
    conversation_id: str = Field(..., description="Conversation to cancel without clearing chat history.")


class PendingMessageRequest(BaseModel):
    conversation_id: str = Field(..., description="Conversation to update.")
    message: str = Field(..., description="Queued or steer message text.")
    mode: str = Field(..., description="Pending message mode: queued or steer.")


class ConversationSnapshot(BaseModel):
    conversation_id: str
    messages: list[dict[str, object]]
    run_status: str
    latest_progress: str
    latest_error: str
    pending_user_message: str = ""
    pending_user_mode: str = ""
    latest_export_url: str = ""
    latest_export_status: str = ""


class ChatAcceptedResponse(BaseModel):
    conversation_id: str
    run_status: str
    latest_progress: str


def _configure_logger() -> logging.Logger:
    logger = logging.getLogger("abs.backend")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


store = ConversationStore()
app = FastAPI(title="ABS Analyst Harness API", version="0.2.0")
logger = _configure_logger()
frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
_RUN_TASKS: dict[str, asyncio.Task] = {}
_EXPORT_TASKS: dict[str, asyncio.Task] = {}


def _should_skip_request_logging(request: Request) -> bool:
    path = request.url.path
    if request.method.upper() == "GET" and path.startswith("/api/conversation/"):
        return True
    return False


def _cors_origins() -> list[str]:
    raw = os.getenv("CORS_ALLOWED_ORIGINS", "")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


allowed_origins = _cors_origins()
if allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/health", tags=["health"])
async def healthcheck():
    return {"status": "ok"}


@app.middleware("http")
async def runtime_request_logger(request: Request, call_next):
    started = time.perf_counter()
    skip_logging = _should_skip_request_logging(request)
    if not skip_logging:
        _emit_runtime_log(f"HTTP start {request.method} {request.url.path}")
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        if not skip_logging:
            _emit_runtime_log(f"HTTP error {request.method} {request.url.path} after {elapsed_ms}ms")
        raise
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    if not skip_logging:
        _emit_runtime_log(
            f"HTTP end {request.method} {request.url.path} status={response.status_code} duration_ms={elapsed_ms}"
        )
    return response


def _truncate(text: str, length: int = 280) -> str:
    clean = text.replace("\n", " ").strip()
    return clean if len(clean) <= length else clean[: length - 1] + "…"


def _emit_runtime_log(message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]}] INFO abs.backend - {text}"
    print(line, flush=True)


def _truncate_jsonable(value: object, length: int = 1000) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = str(value)
    text = text.replace("\n", " ").strip()
    return text if len(text) <= length else text[: length - 1] + "…"


def _openai_error_details(exc: BaseException) -> dict[str, object]:
    details: dict[str, object] = {
        "exception_type": exc.__class__.__name__,
        "message": str(exc),
    }
    if not isinstance(exc, APIError):
        return details

    details["openai_error_type"] = getattr(exc, "type", None)
    details["openai_error_code"] = getattr(exc, "code", None)
    details["openai_error_param"] = getattr(exc, "param", None)

    request = getattr(exc, "request", None)
    if request is not None:
        details["request_method"] = getattr(request, "method", None)
        url = getattr(request, "url", None)
        if url is not None:
            details["request_url"] = str(url)

    body = getattr(exc, "body", None)
    if body is not None:
        details["response_body"] = _truncate_jsonable(body)

    if isinstance(exc, APIStatusError):
        details["status_code"] = getattr(exc, "status_code", None)
        details["request_id"] = getattr(exc, "request_id", None)
        response = getattr(exc, "response", None)
        if response is not None:
            retry_after = getattr(response, "headers", {}).get("retry-after")
            if retry_after:
                details["retry_after"] = retry_after

    return {key: value for key, value in details.items() if value not in (None, "", {})}


def _filtered_messages(state) -> list[dict[str, str]]:
    return [
        message
        for message in state.messages
        if isinstance(message, dict)
        and str(message.get("role") or "").strip().lower() in {"user", "assistant", "progress"}
        and str(message.get("content") or "").strip()
    ]


def _snapshot_from_state(state) -> ConversationSnapshot:
    export_url = ""
    if get_latest_export_artifact_path(state):
        export_url = f"/api/conversation/{state.conversation_id}/latest-export"
    return ConversationSnapshot(
        conversation_id=state.conversation_id,
        messages=_filtered_messages(state),
        run_status=str(state.run_status or "idle"),
        latest_progress=str(state.latest_progress or ""),
        latest_error=str(state.latest_error or ""),
        pending_user_message=str(getattr(state, "pending_user_message", "") or ""),
        pending_user_mode=str(getattr(state, "pending_user_mode", "") or ""),
        latest_export_url=export_url,
        latest_export_status=str(getattr(state, "latest_export_status", "") or ""),
    )


async def _normalize_stale_processing_state(state) -> bool:
    if str(state.run_status or "").strip() != "processing":
        return False
    active_run_id = str(state.active_run_id or "").strip()
    if active_run_id and state.conversation_id in _RUN_TASKS:
        return False

    _rollback_unfinished_run(state)
    state.run_status = "cancelled" if _filtered_messages(state) else "idle"
    state.latest_progress = ""
    state.latest_error = "Previous run was interrupted before completion."
    state.active_run_id = None
    state.active_run_message_count = None
    state.active_run_loop_count = None
    state.active_run_artifact_count = None
    store.save(state)
    await sync_agent_session_from_state(state.conversation_id, state)
    _emit_runtime_log(f"Recovered stale processing run cid={state.conversation_id}")
    logger.info("Recovered stale processing run cid=%s", state.conversation_id)
    return True


def _rollback_unfinished_run(state) -> None:
    if isinstance(state.active_run_message_count, int):
        state.messages = state.messages[: state.active_run_message_count]
    if isinstance(state.active_run_loop_count, int):
        state.loop_history = state.loop_history[: state.active_run_loop_count]
    if isinstance(state.active_run_artifact_count, int):
        state.artifacts = state.artifacts[: state.active_run_artifact_count]
    state.active_run_message_count = None
    state.active_run_loop_count = None
    state.active_run_artifact_count = None


async def _run_generation_job(
    *,
    conversation_id: str,
    user_input: str,
    run_id: str,
) -> None:
    def emit_status(message: str) -> None:
        state = store.load(conversation_id)
        if state.active_run_id != run_id:
            return
        normalized = str(message or "").strip()
        state.latest_progress = normalized
        state.latest_error = ""
        store.save(state)
        if normalized:
            _emit_runtime_log(f'Progress cid={conversation_id} message="{_truncate(normalized, 220)}"')

    try:
        final_response = await asyncio.to_thread(
            generate_response,
            conversation_id,
            user_input,
            store,
            emit_status,
        )
    except ConversationCancelled:
        _emit_runtime_log(f"Conversation cancelled mid-generation cid={conversation_id}")
        logger.info("Conversation cancelled mid-generation cid=%s", conversation_id)
        state = store.load(conversation_id)
        if state.active_run_id == run_id:
            _rollback_unfinished_run(state)
            state.run_status = "cancelled"
            state.latest_progress = ""
            state.latest_error = "Conversation cancelled by user."
            state.active_run_id = None
            store.save(state)
            await sync_agent_session_from_state(conversation_id, state)
    except Exception as exc:
        state = store.load(conversation_id)
        if state.active_run_id == run_id:
            error_details = _openai_error_details(exc)
            logger.exception(
                "Failed to generate response cid=%s error=%s details=%s",
                conversation_id,
                exc,
                _truncate_jsonable(error_details, 2000),
            )
            _rollback_unfinished_run(state)
            state.run_status = "failed"
            state.latest_progress = ""
            state.latest_error = str(exc)
            state.active_run_id = None
            state.active_run_message_count = None
            state.active_run_loop_count = None
            state.active_run_artifact_count = None
            store.save(state)
            await sync_agent_session_from_state(conversation_id, state)
        else:
            logger.info(
                "Ignoring late generation failure for stale run cid=%s run_id=%s error=%s",
                conversation_id,
                run_id,
                exc,
            )
    else:
        _emit_runtime_log(f'Response ready cid={conversation_id} preview="{_truncate(final_response)}"')
        logger.info(
            'Response ready cid=%s preview="%s"',
            conversation_id,
            _truncate(final_response),
        )
        state = store.load(conversation_id)
        if state.active_run_id == run_id:
            state.run_status = "completed"
            state.latest_progress = ""
            state.latest_error = ""
            state.active_run_id = None
            state.active_run_message_count = None
            state.active_run_loop_count = None
            state.active_run_artifact_count = None
            store.save(state)
            if isinstance(getattr(state, "latest_export_request", None), dict):
                export_task = _EXPORT_TASKS.get(conversation_id)
                if export_task is None or export_task.done():
                    _EXPORT_TASKS[conversation_id] = asyncio.create_task(
                        _run_export_job(conversation_id)
                    )
    finally:
        _RUN_TASKS.pop(conversation_id, None)


async def _run_export_job(conversation_id: str) -> None:
    try:
        await asyncio.to_thread(generate_latest_export, conversation_id, store)
    except Exception as exc:
        logger.exception("Failed to generate export cid=%s error=%s", conversation_id, exc)
        _emit_runtime_log(f"Export generation failed cid={conversation_id} error={_truncate(str(exc), 220)}")
    else:
        _emit_runtime_log(f"Export ready cid={conversation_id}")
        logger.info("Export ready cid=%s", conversation_id)
    finally:
        _EXPORT_TASKS.pop(conversation_id, None)


@app.post("/api/chat", response_model=ChatAcceptedResponse)
async def chat(request: ChatRequest):
    user_input = request.message.strip()
    if not user_input:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    _emit_runtime_log(f'Incoming chat request cid={request.conversation_id} message="{_truncate(user_input)}"')
    logger.info(
        'Incoming chat request cid=%s message="%s"',
        request.conversation_id,
        _truncate(user_input),
    )

    state = store.load(request.conversation_id)
    await _normalize_stale_processing_state(state)
    if state.run_status == "processing":
        return ChatAcceptedResponse(
            conversation_id=request.conversation_id,
            run_status="processing",
            latest_progress=str(state.latest_progress or ""),
        )

    run_id = secrets.token_hex(8)
    state.run_status = "processing"
    state.latest_progress = ""
    state.latest_error = ""
    state.active_run_id = run_id
    state.active_run_message_count = len(state.messages)
    state.active_run_loop_count = len(state.loop_history)
    state.active_run_artifact_count = len(state.artifacts)
    store.save(state)

    task = asyncio.create_task(
        _run_generation_job(
            conversation_id=request.conversation_id,
            user_input=user_input,
            run_id=run_id,
        )
    )
    _RUN_TASKS[request.conversation_id] = task

    return ChatAcceptedResponse(
        conversation_id=request.conversation_id,
        run_status="processing",
        latest_progress=state.latest_progress or "",
    )


@app.get("/api/conversation/{conversation_id}", response_model=ConversationSnapshot)
async def get_conversation(conversation_id: str):
    state = store.load(conversation_id)
    await _normalize_stale_processing_state(state)
    return _snapshot_from_state(state)


@app.get("/api/conversation/{conversation_id}/latest-export")
async def get_latest_export(conversation_id: str):
    state = store.load(conversation_id)
    await _normalize_stale_processing_state(state)
    path = get_latest_export_artifact_path(state)
    if path is None:
        raise HTTPException(status_code=404, detail="No export available.")
    download_filename = path.name
    artifact_id = str(getattr(state, "latest_export_artifact_id", "") or "").strip()
    if artifact_id:
        for item in reversed(state.artifacts):
            if not isinstance(item, dict):
                continue
            if str(item.get("artifact_id") or "").strip() != artifact_id:
                continue
            candidate = str(item.get("download_filename") or "").strip()
            if candidate:
                download_filename = candidate
            break
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=download_filename,
    )


@app.post("/api/pending-message")
async def set_pending_message(request: PendingMessageRequest):
    message = request.message.strip()
    mode = request.mode.strip().lower()
    if not message:
        raise HTTPException(status_code=400, detail="Pending message cannot be empty.")
    if mode not in {"queued", "steer"}:
        raise HTTPException(status_code=400, detail="Pending message mode must be queued or steer.")

    state = store.load(request.conversation_id)
    await _normalize_stale_processing_state(state)
    state.pending_user_message = message
    state.pending_user_mode = mode
    store.save(state)
    _emit_runtime_log(
        f'Pending message stored cid={request.conversation_id} mode={mode} message="{_truncate(message)}"'
    )
    logger.info(
        'Pending message stored cid=%s mode=%s message="%s"',
        request.conversation_id,
        mode,
        _truncate(message),
    )
    return _snapshot_from_state(state)


@app.post("/api/pending-message/consume")
async def consume_pending_message(request: ResetRequest):
    state = store.load(request.conversation_id)
    await _normalize_stale_processing_state(state)
    state.pending_user_message = ""
    state.pending_user_mode = ""
    store.save(state)
    return _snapshot_from_state(state)


@app.post("/api/cancel")
async def cancel(request: CancelRequest):
    cancel_conversation_processing(request.conversation_id)
    running_task: Optional[asyncio.Task] = _RUN_TASKS.pop(request.conversation_id, None)
    if running_task is not None and not running_task.done():
        running_task.cancel()
    export_task: Optional[asyncio.Task] = _EXPORT_TASKS.pop(request.conversation_id, None)
    if export_task is not None and not export_task.done():
        export_task.cancel()

    state = store.load(request.conversation_id)
    _rollback_unfinished_run(state)
    state.run_status = "cancelled"
    state.latest_progress = ""
    state.latest_error = ""
    state.active_run_id = None
    store.save(state)
    await sync_agent_session_from_state(request.conversation_id, state)

    _emit_runtime_log(f"Conversation cancelled cid={request.conversation_id}")
    logger.info("Conversation cancelled cid=%s", request.conversation_id)
    return {"status": "cancelled"}


@app.post("/api/reset")
async def reset(request: ResetRequest):
    cancel_conversation_processing(request.conversation_id)
    running_task: Optional[asyncio.Task] = _RUN_TASKS.pop(request.conversation_id, None)
    if running_task is not None and not running_task.done():
        running_task.cancel()
    export_task: Optional[asyncio.Task] = _EXPORT_TASKS.pop(request.conversation_id, None)
    if export_task is not None and not export_task.done():
        export_task.cancel()
    store.clear(request.conversation_id)
    reset_conversation_runtime(request.conversation_id)
    await clear_agent_session(request.conversation_id)
    _emit_runtime_log(f"Conversation cleared cid={request.conversation_id}")
    logger.info("Conversation cleared cid=%s", request.conversation_id)
    return {"status": "cleared"}


if frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(frontend_dist / "assets")), name="frontend-assets")

    @app.get("/", include_in_schema=False)
    async def frontend_index():
        return FileResponse(frontend_dist / "index.html")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def frontend_catchall(full_path: str):
        if full_path.startswith("api/") or full_path == "health":
            raise HTTPException(status_code=404, detail="Not found")
        target = frontend_dist / full_path
        if target.exists() and target.is_file():
            return FileResponse(target)
        return FileResponse(frontend_dist / "index.html")
