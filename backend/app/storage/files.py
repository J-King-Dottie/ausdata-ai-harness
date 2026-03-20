from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List

from ..config import get_settings


@dataclass
class ConversationState:
    conversation_id: str
    messages: List[Dict[str, Any]] = field(default_factory=list)
    loop_history: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: List[Dict[str, Any]] = field(default_factory=list)
    completed_runs: List[Dict[str, int]] = field(default_factory=list)
    current_abs_dataset_shortlist: List[Dict[str, Any]] = field(default_factory=list)
    current_macro_indicator_shortlist: List[Dict[str, Any]] = field(default_factory=list)
    pending_plan: Dict[str, Any] | None = None
    run_status: str = "idle"
    latest_progress: str = ""
    latest_error: str = ""
    pending_user_message: str = ""
    pending_user_mode: str = ""
    latest_export_artifact_id: str = ""
    latest_export_status: str = ""
    latest_export_request: Dict[str, Any] | None = None
    active_run_id: str | None = None
    active_run_message_count: int | None = None
    active_run_loop_count: int | None = None
    active_run_artifact_count: int | None = None
    last_provider_route: str = ""
    last_provider_search_query: str = ""


class ConversationStore:
    """Conversation storage backed by memory and runtime JSON files."""

    def __init__(self) -> None:
        self._states: Dict[str, ConversationState] = {}
        self._lock = RLock()
        settings = get_settings()
        self._base_dir = settings.runtime_dir / "conversation_store"
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _state_path(self, conversation_id: str) -> Path:
        safe_id = "".join(ch for ch in conversation_id if ch.isalnum() or ch in {"-", "_"})
        if not safe_id:
            safe_id = "conversation"
        return self._base_dir / f"{safe_id}.json"

    def _load_from_disk(self, conversation_id: str) -> ConversationState | None:
        path = self._state_path(conversation_id)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(raw, dict):
            return None
        return ConversationState(
            conversation_id=conversation_id,
            messages=list(raw.get("messages") or []),
            loop_history=list(raw.get("loop_history") or []),
            artifacts=list(raw.get("artifacts") or []),
            completed_runs=[
                item for item in list(raw.get("completed_runs") or [])
                if isinstance(item, dict)
            ],
            current_abs_dataset_shortlist=list(raw.get("current_abs_dataset_shortlist") or []),
            current_macro_indicator_shortlist=list(raw.get("current_macro_indicator_shortlist") or []),
            pending_plan=raw.get("pending_plan") if isinstance(raw.get("pending_plan"), dict) else None,
            run_status=str(raw.get("run_status") or "idle"),
            latest_progress=str(raw.get("latest_progress") or ""),
            latest_error=str(raw.get("latest_error") or ""),
            pending_user_message=str(raw.get("pending_user_message") or ""),
            pending_user_mode=str(raw.get("pending_user_mode") or ""),
            latest_export_artifact_id=str(raw.get("latest_export_artifact_id") or ""),
            latest_export_status=str(raw.get("latest_export_status") or ""),
            latest_export_request=raw.get("latest_export_request") if isinstance(raw.get("latest_export_request"), dict) else None,
            active_run_id=str(raw.get("active_run_id") or "").strip() or None,
            active_run_message_count=raw.get("active_run_message_count") if isinstance(raw.get("active_run_message_count"), int) else None,
            active_run_loop_count=raw.get("active_run_loop_count") if isinstance(raw.get("active_run_loop_count"), int) else None,
            active_run_artifact_count=raw.get("active_run_artifact_count") if isinstance(raw.get("active_run_artifact_count"), int) else None,
            last_provider_route=str(raw.get("last_provider_route") or ""),
            last_provider_search_query=str(raw.get("last_provider_search_query") or ""),
        )

    def _save_to_disk(self, state: ConversationState) -> None:
        path = self._state_path(state.conversation_id)
        payload = {
            "conversation_id": state.conversation_id,
            "messages": state.messages,
            "loop_history": state.loop_history,
            "artifacts": state.artifacts,
            "completed_runs": state.completed_runs,
            "current_abs_dataset_shortlist": state.current_abs_dataset_shortlist,
            "current_macro_indicator_shortlist": state.current_macro_indicator_shortlist,
            "pending_plan": state.pending_plan,
            "run_status": state.run_status,
            "latest_progress": state.latest_progress,
            "latest_error": state.latest_error,
            "pending_user_message": state.pending_user_message,
            "pending_user_mode": state.pending_user_mode,
            "latest_export_artifact_id": state.latest_export_artifact_id,
            "latest_export_status": state.latest_export_status,
            "latest_export_request": state.latest_export_request,
            "active_run_id": state.active_run_id,
            "active_run_message_count": state.active_run_message_count,
            "active_run_loop_count": state.active_run_loop_count,
            "active_run_artifact_count": state.active_run_artifact_count,
            "last_provider_route": state.last_provider_route,
            "last_provider_search_query": state.last_provider_search_query,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self, conversation_id: str) -> ConversationState:
        with self._lock:
            state = self._states.get(conversation_id)
            if state is None:
                state = self._load_from_disk(conversation_id) or ConversationState(conversation_id=conversation_id)
                self._states[conversation_id] = state
            return state

    def save(self, state: ConversationState) -> None:
        with self._lock:
            self._states[state.conversation_id] = state
            self._save_to_disk(state)

    def clear(self, conversation_id: str) -> None:
        with self._lock:
            self._states.pop(conversation_id, None)
            self._state_path(conversation_id).unlink(missing_ok=True)

    def clear_all(self) -> None:
        with self._lock:
            self._states.clear()
            for path in self._base_dir.glob("*.json"):
                path.unlink(missing_ok=True)
