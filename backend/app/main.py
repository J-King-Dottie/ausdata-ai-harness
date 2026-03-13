from __future__ import annotations

import asyncio
import json
import logging
import math
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
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


class ConversationSnapshot(BaseModel):
    conversation_id: str
    messages: list[dict[str, str]]


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


@app.get("/health", tags=["health"])
async def healthcheck():
    return {"status": "ok"}


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


def _chunk_text(text: str, chunk_size: int = 512) -> AsyncGenerator[str, None]:
    total_length = len(text)
    if total_length == 0:
        yield ""
        return

    steps = math.ceil(total_length / chunk_size)
    for index in range(steps):
        start = index * chunk_size
        end = min(start + chunk_size, total_length)
        yield text[start:end]


def _truncate(text: str, length: int = 280) -> str:
    clean = text.replace("\n", " ").strip()
    return clean if len(clean) <= length else clean[: length - 1] + "…"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    user_input = request.message.strip()
    if not user_input:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    logger.info(
        'Incoming chat request cid=%s message="%s"',
        request.conversation_id,
        _truncate(user_input),
    )

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

    def emit_status(message: str) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, ("status", message))

    async def run_generation():
        try:
            final_response = await asyncio.to_thread(
                generate_response,
                request.conversation_id,
                user_input,
                store,
                emit_status,
            )
        except ConversationCancelled:
            logger.info("Conversation cancelled mid-generation cid=%s", request.conversation_id)
            await queue.put(("error", "Conversation cancelled by user."))
        except Exception as exc:
            logger.exception(
                "Failed to generate response cid=%s error=%s",
                request.conversation_id,
                exc,
            )
            await queue.put(("error", str(exc)))
        else:
            logger.info(
                'Response ready cid=%s preview="%s"',
                request.conversation_id,
                _truncate(final_response),
            )
            await queue.put(("final", final_response))

    generation_task = asyncio.create_task(run_generation())

    async def event_stream():
        try:
            while True:
                kind, payload = await queue.get()
                if kind == "status":
                    yield json.dumps({"type": "status", "message": payload}, ensure_ascii=False) + "\n"
                elif kind == "final":
                    for chunk in _chunk_text(payload):
                        yield json.dumps({"type": "final", "chunk": chunk}, ensure_ascii=False) + "\n"
                    yield json.dumps({"type": "done"}, ensure_ascii=False) + "\n"
                    break
                elif kind == "error":
                    yield json.dumps({"type": "error", "message": payload}, ensure_ascii=False) + "\n"
                    break
        finally:
            if not generation_task.done():
                generation_task.cancel()

    return StreamingResponse(event_stream(), media_type="text/plain")


@app.get("/api/conversation/{conversation_id}", response_model=ConversationSnapshot)
async def get_conversation(conversation_id: str):
    state = store.load(conversation_id)
    return ConversationSnapshot(
        conversation_id=conversation_id,
        messages=[
            message
            for message in state.messages
            if isinstance(message, dict)
            and str(message.get("role") or "").strip().lower() in {"user", "assistant"}
            and str(message.get("content") or "").strip()
        ],
    )


@app.post("/api/reset")
async def reset(request: ResetRequest):
    cancel_conversation_processing(request.conversation_id)
    store.clear(request.conversation_id)
    reset_conversation_runtime(request.conversation_id)
    logger.info("Conversation cleared cid=%s", request.conversation_id)
    return {"status": "cleared"}
