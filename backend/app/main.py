from __future__ import annotations

import asyncio
import logging
import secrets
from pathlib import Path
from typing import Optional

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .openai_service import (
    ConversationCancelled,
    cancel_conversation_processing,
    generate_response,
    reset_conversation_runtime,
)
from .storage import ConversationStore


class ChatRequest(BaseModel):
    conversation_id: str = Field(..., description="Stable identifier for the chat session.")
    message: str = Field(..., description="The user's natural language query.")


class ResetRequest(BaseModel):
    conversation_id: str = Field(..., description="Conversation to reset.")


class CancelRequest(BaseModel):
    conversation_id: str = Field(..., description="Conversation to cancel without clearing chat history.")


class ConversationSnapshot(BaseModel):
    conversation_id: str
    messages: list[dict[str, str]]
    run_status: str
    latest_progress: str
    latest_error: str


class ChatAcceptedResponse(BaseModel):
    conversation_id: str
    run_status: str
    latest_progress: str


def _configure_logger() -> logging.Logger:
    logger = logging.getLogger("abs.backend")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


store = ConversationStore()
app = FastAPI(title="ABS Analyst Harness API", version="0.2.0")
logger = _configure_logger()
frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
_RUN_TASKS: dict[str, asyncio.Task] = {}


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


def _truncate(text: str, length: int = 280) -> str:
    clean = text.replace("\n", " ").strip()
    return clean if len(clean) <= length else clean[: length - 1] + "…"


def _filtered_messages(state) -> list[dict[str, str]]:
    return [
        message
        for message in state.messages
        if isinstance(message, dict)
        and str(message.get("role") or "").strip().lower() in {"user", "assistant", "progress"}
        and str(message.get("content") or "").strip()
    ]


def _snapshot_from_state(state) -> ConversationSnapshot:
    return ConversationSnapshot(
        conversation_id=state.conversation_id,
        messages=_filtered_messages(state),
        run_status=str(state.run_status or "idle"),
        latest_progress=str(state.latest_progress or ""),
        latest_error=str(state.latest_error or ""),
    )


def _normalize_stale_processing_state(state) -> bool:
    if str(state.run_status or "").strip() != "processing":
        return False
    active_run_id = str(state.active_run_id or "").strip()
    if active_run_id and state.conversation_id in _RUN_TASKS:
        return False

    state.run_status = "completed" if _filtered_messages(state) else "idle"
    state.latest_progress = ""
    state.latest_error = ""
    state.active_run_id = None
    state.active_run_message_count = None
    state.active_run_loop_count = None
    state.active_run_artifact_count = None
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
        state.latest_progress = str(message or "").strip()
        state.latest_error = ""
        store.save(state)

    try:
        final_response = await asyncio.to_thread(
            generate_response,
            conversation_id,
            user_input,
            store,
            emit_status,
        )
    except ConversationCancelled:
        logger.info("Conversation cancelled mid-generation cid=%s", conversation_id)
        state = store.load(conversation_id)
        if state.active_run_id == run_id:
            _rollback_unfinished_run(state)
            state.run_status = "cancelled"
            state.latest_progress = ""
            state.latest_error = "Conversation cancelled by user."
            state.active_run_id = None
            store.save(state)
    except Exception as exc:
        state = store.load(conversation_id)
        if state.active_run_id == run_id:
            logger.exception(
                "Failed to generate response cid=%s error=%s",
                conversation_id,
                exc,
            )
            state.run_status = "failed"
            state.latest_progress = ""
            state.latest_error = str(exc)
            state.active_run_id = None
            state.active_run_message_count = None
            state.active_run_loop_count = None
            state.active_run_artifact_count = None
            store.save(state)
        else:
            logger.info(
                "Ignoring late generation failure for stale run cid=%s run_id=%s error=%s",
                conversation_id,
                run_id,
                exc,
            )
    else:
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
    finally:
        _RUN_TASKS.pop(conversation_id, None)


@app.post("/api/chat", response_model=ChatAcceptedResponse)
async def chat(request: ChatRequest):
    user_input = request.message.strip()
    if not user_input:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    logger.info(
        'Incoming chat request cid=%s message="%s"',
        request.conversation_id,
        _truncate(user_input),
    )

    state = store.load(request.conversation_id)
    if _normalize_stale_processing_state(state):
        store.save(state)
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
    if _normalize_stale_processing_state(state):
        store.save(state)
    return _snapshot_from_state(state)


@app.post("/api/cancel")
async def cancel(request: CancelRequest):
    cancel_conversation_processing(request.conversation_id)
    running_task: Optional[asyncio.Task] = _RUN_TASKS.pop(request.conversation_id, None)
    if running_task is not None and not running_task.done():
        running_task.cancel()

    state = store.load(request.conversation_id)
    _rollback_unfinished_run(state)
    state.run_status = "cancelled"
    state.latest_progress = ""
    state.latest_error = ""
    state.active_run_id = None
    store.save(state)

    logger.info("Conversation cancelled cid=%s", request.conversation_id)
    return {"status": "cancelled"}


@app.post("/api/reset")
async def reset(request: ResetRequest):
    cancel_conversation_processing(request.conversation_id)
    running_task: Optional[asyncio.Task] = _RUN_TASKS.pop(request.conversation_id, None)
    if running_task is not None and not running_task.done():
        running_task.cancel()
    store.clear(request.conversation_id)
    reset_conversation_runtime(request.conversation_id)
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
